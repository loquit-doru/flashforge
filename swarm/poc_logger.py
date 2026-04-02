"""
Proof of Coordination (PoC) Logger for the FlashForge Agent Swarm.

Each PoC entry is HMAC-SHA256 signed so that:
  1. Events are tamper-evident — any modification breaks the chain.
  2. Any verifier with the shared secret can re-verify all signatures.
  3. A rolling chain-hash links each entry to the previous one (like a mini-blockchain).
  4. Multi-stage coordination is provable: who did what, when, in what order.

Log format: newline-delimited JSON (JSONL), one entry per event.
File location: poc_logs/poc_{job_id}.jsonl

Use verify_poc_log() to independently verify a completed log.
"""
import hashlib
import hmac as _hmac_mod
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


class PoCLogger:
    """
    Append-only, HMAC-chained Proof of Coordination log.

    Every entry contains:
      seq          — monotonically increasing sequence number
      job_id       — which job this belongs to
      event        — event name (e.g. PLAN_READY, BUILD_COMPLETE, EVAL_PASS)
      actor        — node_id or role that produced this event
      timestamp_ms — unix millisecond timestamp
      prev_chain   — HMAC of the previous entry (empty string for first)
      data         — arbitrary event-specific data
      hmac         — HMAC-SHA256 of all of the above (sorted canonical JSON)
    """

    def __init__(
        self,
        job_id: str,
        secret: str = "swarm-secret-change-in-prod",
        log_dir: str = "./poc_logs",
    ):
        self.job_id = job_id
        self._secret = secret.encode()
        self._log_path = Path(log_dir) / f"poc_{job_id}.jsonl"
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._seq = 0
        self._chain_hash = ""   # HMAC of last entry (empty for genesis)
        self._resume_from_file()  # continue chain if file already has entries

    # ── Public API ─────────────────────────────────────────────────────────────

    def _resume_from_file(self) -> None:
        """If the log file already exists, fast-forward seq + chain_hash to its last entry."""
        if not self._log_path.exists():
            return
        last_entry = None
        with open(self._log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        last_entry = json.loads(line)
                    except json.JSONDecodeError:
                        pass
        if last_entry is not None:
            self._seq = last_entry.get("seq", 0) + 1
            self._chain_hash = last_entry.get("hmac", "")

    # ── Public API ─────────────────────────────────────────────────────────────

    def record(
        self,
        event: str,
        actor: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Append a signed event to the PoC log. Returns the signed entry."""
        entry: Dict[str, Any] = {
            "seq": self._seq,
            "job_id": self.job_id,
            "event": event,
            "actor": actor,
            "timestamp_ms": int(time.time() * 1000),
            "prev_chain": self._chain_hash,
            "data": data or {},
        }
        entry["hmac"] = self._sign(entry)
        self._chain_hash = entry["hmac"]
        self._seq += 1

        line = json.dumps(entry, separators=(",", ":")) + "\n"
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

        return entry

    def finalize(
        self,
        signers: List[str],
        signer_secrets: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Append a COORDINATION_COMPLETE summary entry with multi-signer attestation.

        signers:        list of node_ids or roles that contributed to this job.
        signer_secrets: optional dict {signer_id: per-agent HMAC secret} for
                        multi-signer verification.  Each signer independently
                        signs the chain_root, proving they attest to the full log.
                        If not provided, falls back to shared-secret attestation.
        """
        # Collect per-signer attestation signatures over the chain root
        attestations: Dict[str, str] = {}
        chain_root = self._chain_hash
        for signer_id in signers:
            if signer_secrets and signer_id in signer_secrets:
                key = signer_secrets[signer_id].encode()
            else:
                key = self._secret  # shared secret fallback
            attestations[signer_id] = _hmac_mod.new(
                key,
                f"{self.job_id}:{chain_root}:{signer_id}".encode(),
                hashlib.sha256,
            ).hexdigest()

        return self.record(
            "COORDINATION_COMPLETE",
            actor="swarm",
            data={
                "signers": signers,
                "total_events": self._seq,
                "chain_root": chain_root,
                "attestations": attestations,
            },
        )

    @property
    def log_path(self) -> Path:
        return self._log_path

    @property
    def chain_hash(self) -> str:
        """Current chain tip (HMAC of last written entry)."""
        return self._chain_hash

    # ── HMAC helper ────────────────────────────────────────────────────────────

    def _sign(self, entry: Dict[str, Any]) -> str:
        body = {k: v for k, v in entry.items() if k != "hmac"}
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        return _hmac_mod.new(self._secret, canonical, hashlib.sha256).hexdigest()


# ── Standalone verifier ────────────────────────────────────────────────────────

def verify_poc_log(log_path: str, secret: str, verbose: bool = True) -> Dict[str, Any]:
    """
    Re-compute and verify every HMAC + chain link in a PoC log file.

    Returns a structured verification report::

        {
            "valid": bool,
            "entries": [ {"seq":0, "event":"...", "hmac_ok":True, "chain_ok":True, "ts_ok":True}, ... ],
            "chain_intact": bool,
            "timestamps_monotonic": bool,
            "attestations_valid": bool | None,
            "total_entries": int,
            "stages": { "planning": {"actor":"...", "duration_s":2.4}, ... },
            "total_time_s": float,
        }
    """
    secret_bytes = secret.encode()
    prev_chain = ""
    ok = True
    entries_report: List[Dict[str, Any]] = []
    timestamps: List[int] = []
    stages: Dict[str, Dict[str, Any]] = {}
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    chain_intact = True
    ts_monotonic = True
    attestations_valid: Optional[bool] = None

    if verbose:
        print(f"\n🔍 Verifying PoC log: {log_path}")
        print("─" * 60)

    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            seq = entry.get("seq", "?")
            event = entry.get("event", "?")
            actor = entry.get("actor", "?")
            ts = entry.get("timestamp_ms", 0)

            entry_ok = {"seq": seq, "event": event, "actor": actor, "hmac_ok": False, "chain_ok": False, "ts_ok": True}

            # Track timestamps for monotonicity + stage timing
            if first_ts is None:
                first_ts = ts
            if timestamps and ts < timestamps[-1]:
                ts_monotonic = False
                entry_ok["ts_ok"] = False
                if verbose:
                    print(f"  ⚠ seq {seq} [{event}] TIMESTAMP OUT OF ORDER ({ts} < {timestamps[-1]})")
            timestamps.append(ts)
            last_ts = ts

            # Track stage durations
            if event == "PLAN_READY":
                stages["planning"] = {"actor": actor, "duration_s": round((ts - first_ts) / 1000, 2) if first_ts else 0}
            elif event == "BUILD_COMPLETE":
                build_start = stages.get("building", {}).get("_start_ts", first_ts or ts)
                stages["building"] = {"actor": actor, "duration_s": round((ts - build_start) / 1000, 2)}
            elif event == "BUILD_STARTED":
                stages.setdefault("building", {})["_start_ts"] = ts
                stages["building"]["actor"] = actor
            elif event == "EVAL_CONSENSUS":
                eval_start = stages.get("building", {}).get("_end_ts", ts)
                stages["evaluation"] = {"actor": actor, "duration_s": round((ts - (stages.get("building", {}).get("_start_ts", ts))) / 1000, 2)}

            # 1. Chain link
            if entry.get("prev_chain") != prev_chain:
                chain_intact = False
                ok = False
                entry_ok["chain_ok"] = False
                if verbose:
                    print(f"  ✗ seq {seq} [{event}] CHAIN BREAK")
            else:
                entry_ok["chain_ok"] = True
                # 2. HMAC
                stored_hmac = entry.get("hmac", "")
                body = {k: v for k, v in entry.items() if k != "hmac"}
                canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
                computed = _hmac_mod.new(secret_bytes, canonical, hashlib.sha256).hexdigest()
                if _hmac_mod.compare_digest(stored_hmac, computed):
                    entry_ok["hmac_ok"] = True
                    prev_chain = stored_hmac
                    if verbose:
                        print(f"  ✓ seq {seq:>3} [{event}]  actor={actor}")
                else:
                    ok = False
                    if verbose:
                        print(f"  ✗ seq {seq} [{event}] HMAC MISMATCH — TAMPERED")

            # 3. Verify multi-signer attestations on COORDINATION_COMPLETE
            if event == "COORDINATION_COMPLETE":
                data = entry.get("data", {})
                attestations = data.get("attestations", {})
                if attestations:
                    attestations_valid = True
                    chain_root = data.get("chain_root", "")
                    job_id = entry.get("job_id", "")
                    for signer_id, sig in attestations.items():
                        expected = _hmac_mod.new(
                            secret_bytes,
                            f"{job_id}:{chain_root}:{signer_id}".encode(),
                            hashlib.sha256,
                        ).hexdigest()
                        if not _hmac_mod.compare_digest(sig, expected):
                            attestations_valid = False
                            ok = False
                            if verbose:
                                print(f"  ✗ Attestation INVALID for signer: {signer_id}")
                        elif verbose:
                            print(f"  ✓ Attestation valid: {signer_id}")

            entries_report.append(entry_ok)

    # Clean up internal tracking keys from stages
    for stage_data in stages.values():
        stage_data.pop("_start_ts", None)
        stage_data.pop("_end_ts", None)

    total_time = round((last_ts - first_ts) / 1000, 2) if first_ts and last_ts else 0

    if verbose:
        print("─" * 60)
        if ok:
            print(f"  ✅ Log VALID — coordination proof intact.")
        else:
            print(f"  ❌ Log INVALID — tampering detected.")
        print(f"  📊 {len(entries_report)} events | chain={'✓' if chain_intact else '✗'} "
              f"| timestamps={'✓ monotonic' if ts_monotonic else '✗ out-of-order'} "
              f"| attestations={'✓' if attestations_valid else ('✗' if attestations_valid is False else 'N/A')}")
        if stages:
            stage_parts = [f"{k}: {v.get('duration_s', 0)}s" for k, v in stages.items()]
            print(f"  ⏱  Stages: {', '.join(stage_parts)}")
        print(f"  ⏱  Total: {total_time}s")

    return {
        "valid": ok,
        "entries": entries_report,
        "chain_intact": chain_intact,
        "timestamps_monotonic": ts_monotonic,
        "attestations_valid": attestations_valid,
        "total_entries": len(entries_report),
        "stages": stages,
        "total_time_s": total_time,
    }
