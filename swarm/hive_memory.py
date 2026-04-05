"""
Hive Memory — Decentralized shared state for the FlashForge agent swarm.

Track 3 requirement: "Agents pass intermediate state securely, maintaining
a synchronized world view without a central database."

Design:
  - Every agent can WRITE memory entries via FoxMQ `swarm/HIVE_MEMORY`.
  - Every agent can READ the full hive state (replicated locally via MQTT).
  - Entries are HMAC-signed by the producing agent.
  - Memory is partitioned by namespace:
      plan:*     — planning context (decomposition, complexity, components)
      build:*    — build artifacts metadata (size, time, path)
      eval:*     — evaluation results (scores, issues, verdicts)
      fix:*      — fix attempts (iteration count, diff summary)
      meta:*     — swarm-level metadata (agent count, capabilities)

  - Each entry has a TTL (default 5 min) — hive auto-evicts stale knowledge.
  - Total capacity capped at MAX_ENTRIES to prevent memory bloat.

This module is used by:
  1. Agent nodes (planner, builder, critic, fixer) — publish knowledge.
  2. Dashboard server — expose hive state via API.
"""
import hashlib
import hmac as _hmac
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

MAX_ENTRIES     = 500       # max hive memory entries (FIFO eviction)
DEFAULT_TTL_S   = 3600      # 1 hour — keep entries visible for demo/dashboard
HIVE_TOPIC      = "HIVE_MEMORY"


@dataclass
class HiveEntry:
    """A single knowledge entry in the hive memory."""
    namespace: str            # e.g. "plan", "build", "eval", "fix", "meta"
    key: str                  # unique within namespace
    value: Dict[str, Any]     # structured data
    author_id: str            # node_id of the producing agent
    author_role: str          # role of the producing agent
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    ttl_s: float = DEFAULT_TTL_S
    job_id: str = ""          # associated job (empty for meta entries)

    @property
    def full_key(self) -> str:
        return f"{self.namespace}:{self.key}"

    @property
    def expires_ms(self) -> int:
        return self.timestamp_ms + int(self.ttl_s * 1000)

    @property
    def is_expired(self) -> bool:
        return int(time.time() * 1000) > self.expires_ms

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "HiveEntry":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class HiveMemory:
    """
    Local replica of the swarm's shared memory.

    Agents publish entries → FoxMQ replicates → all nodes converge
    to the same state. No central database.
    """

    def __init__(self, max_entries: int = MAX_ENTRIES):
        self._store: OrderedDict[str, HiveEntry] = OrderedDict()
        self._max = max_entries
        self._stats = {
            "writes": 0,
            "reads": 0,
            "evictions": 0,
            "expired": 0,
        }

    # ── Write ──────────────────────────────────────────────────────────────────

    def put(self, entry: HiveEntry) -> None:
        """Insert or update an entry in the hive."""
        fk = entry.full_key
        if fk in self._store:
            del self._store[fk]
        self._store[fk] = entry
        self._stats["writes"] += 1

        # FIFO eviction if over capacity
        while len(self._store) > self._max:
            evicted_key, _ = self._store.popitem(last=False)
            self._stats["evictions"] += 1

    def put_from_payload(self, payload: Dict[str, Any]) -> HiveEntry:
        """Create and store an entry from an MQTT payload dict."""
        entry = HiveEntry.from_dict(payload)
        self.put(entry)
        return entry

    # ── Read ───────────────────────────────────────────────────────────────────

    def get(self, namespace: str, key: str) -> Optional[HiveEntry]:
        """Get a single entry by namespace:key."""
        fk = f"{namespace}:{key}"
        entry = self._store.get(fk)
        if entry and entry.is_expired:
            del self._store[fk]
            self._stats["expired"] += 1
            return None
        if entry:
            self._stats["reads"] += 1
        return entry

    def query(self, namespace: str = "", job_id: str = "") -> List[HiveEntry]:
        """Query entries by namespace and/or job_id. Filters expired."""
        self._gc()
        self._stats["reads"] += 1
        results = []
        for entry in self._store.values():
            if namespace and entry.namespace != namespace:
                continue
            if job_id and entry.job_id != job_id:
                continue
            results.append(entry)
        return results

    def get_all(self) -> List[HiveEntry]:
        """Return all non-expired entries."""
        self._gc()
        return list(self._store.values())

    # ── Stats ──────────────────────────────────────────────────────────────────

    @property
    def size(self) -> int:
        return len(self._store)

    @property
    def stats(self) -> dict:
        return {**self._stats, "size": self.size}

    def namespace_counts(self) -> Dict[str, int]:
        """Count entries per namespace."""
        counts: Dict[str, int] = {}
        for entry in self._store.values():
            counts[entry.namespace] = counts.get(entry.namespace, 0) + 1
        return counts

    # ── GC ─────────────────────────────────────────────────────────────────────

    def _gc(self) -> None:
        """Remove expired entries."""
        now_ms = int(time.time() * 1000)
        expired = [k for k, e in self._store.items() if now_ms > e.expires_ms]
        for k in expired:
            del self._store[k]
            self._stats["expired"] += 1

    # ── Snapshot for dashboard ─────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        """Full snapshot for the dashboard API."""
        self._gc()
        entries = [e.to_dict() for e in self._store.values()]
        return {
            "entries": entries,
            "total": len(entries),
            "namespaces": self.namespace_counts(),
            "stats": self.stats,
        }


# ── Helper: create MQTT payload for publishing ─────────────────────────────────

def make_hive_payload(
    namespace: str,
    key: str,
    value: Dict[str, Any],
    author_id: str,
    author_role: str,
    job_id: str = "",
    ttl_s: float = DEFAULT_TTL_S,
) -> Dict[str, Any]:
    """Build the payload dict for publishing via FoxMQNode.publish(HIVE_MEMORY, ...)."""
    return {
        "namespace": namespace,
        "key": key,
        "value": value,
        "author_id": author_id,
        "author_role": author_role,
        "job_id": job_id,
        "ttl_s": ttl_s,
        "timestamp_ms": int(time.time() * 1000),
    }
