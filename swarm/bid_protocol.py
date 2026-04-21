"""
Leaderless Bid Protocol for the FlashForge Agent Swarm.

Design (Track 3 — Agent Economy):
  - No central orchestrator. Any node can announce a TASK_AVAILABLE.
  - Capable agents self-select and send a BID within COMMIT_WINDOW_MS.
  - After the window, every bidder evaluates all received bids using the
    same deterministic rule: min(load_score, timestamp_ms, bidder_id).
  - The winner broadcasts COMMIT; others stand down.
  - Race conditions → first valid COMMIT wins (idempotency key on job_id).
  - All bid/commit events feed directly into PoCLogger for audit trail.

Message schema (all via FoxMQNode.publish):
  TASK_AVAILABLE  { job_id, capability, prompt, context }
  BID             { job_id, bidder_id, bidder_role, load_score, capability, timestamp_ms }
  COMMIT          { job_id, winner_id, winner_role, capability, committed_at_ms }
"""
import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from swarm.agent_economy import AgentEconomy

COMMIT_WINDOW_MS  = 500    # ms to collect bids before evaluating winner
ORPHAN_TIMEOUT_S  = 30.0   # seconds after commit window with no COMMIT → re-announce
ORPHAN_MAX_RETRY  = 3      # max re-announcement attempts before giving up
REPUTATION_WEIGHT = 0.3    # how much reputation influences bid score (0=none, 1=full)
RESULT_TIMEOUT_S  = 120.0  # after COMMIT, how long to wait for next-stage before re-announcing

VALID_CAPABILITIES = {"planning", "building", "evaluation", "fixing", "any"}


@dataclass
class Bid:
    job_id: str
    bidder_id: str
    bidder_role: str
    load_score: float          # 0.0 = idle, 1.0 = saturated
    capability: str
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class Commit:
    job_id: str
    winner_id: str
    winner_role: str
    capability: str
    committed_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))


CommitCallback = Callable[[str, bool, Optional[Dict[str, Any]]], None]
# Called with (job_id, won: bool, task_payload: dict|None)


class BidProtocol:
    """
    Plugs into a FoxMQNode to add leaderless task-bidding.

    Usage:
        node = FoxMQNode(...)
        bp = BidProtocol(node, capability="planning")
        bp.on_commit(my_handler)   # my_handler(job_id, won, task_payload)
        await node.start()
    """

    def __init__(
        self,
        node,                          # FoxMQNode
        capability: str,
        load_fn: Optional[Callable[[], float]] = None,
        economy: Optional["AgentEconomy"] = None,
    ):
        self.node = node
        self.capability = capability
        self._load_fn = load_fn or (lambda: 0.0)
        self._economy = economy        # optional: enables reputation-weighted bidding

        # job_id → list of Bid objects (from all bidders including self)
        self._pending_bids: Dict[str, List[Bid]] = {}
        # job_id → full TASK_AVAILABLE payload (for winner to use)
        self._task_payloads: Dict[str, Dict[str, Any]] = {}
        # committed job IDs (idempotency key)
        self._committed_jobs: Set[str] = set()

        self._commit_callbacks: List[CommitCallback] = []
        # job_id → retry count (for orphan re-announcement)
        self._orphan_retries: Dict[str, int] = {}
        # job_ids already re-announced as orphan (prevents duplicate re-announcements)
        self._orphan_announced: Set[str] = set()
        # job_id → (next_stage_job_id, original_payload) — for result-level orphan detection
        self._result_expected: Dict[str, tuple] = {}

        # Wire up handlers on the node
        node.on("TASK_AVAILABLE", self._handle_task)
        node.on("BID", self._handle_bid)
        node.on("COMMIT", self._handle_commit)

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_commit(self, callback: CommitCallback) -> None:
        """Register callback: called when any job's winner is determined."""
        self._commit_callbacks.append(callback)

    async def announce_task(
        self,
        prompt: str,
        capability: str = "planning",
        context: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        next_stage_job_id: Optional[str] = None,
    ) -> str:
        """Announce a new task to the swarm. Returns job_id.

        next_stage_job_id: if set, after seeing COMMIT for this task the announcer
        starts a result watchdog — if the next-stage COMMIT is not seen within
        RESULT_TIMEOUT_S the executor is presumed dead and the task is re-announced.
        """
        job_id = job_id or str(uuid.uuid4())
        payload = {
            "job_id": job_id,
            "capability": capability,
            "prompt": prompt,
            "context": context or {},
            "announced_at_ms": int(time.time() * 1000),
        }
        self._task_payloads[job_id] = payload
        self._orphan_retries[job_id] = 0
        if next_stage_job_id:
            self._result_expected[job_id] = (next_stage_job_id, payload)
        await self.node.publish("TASK_AVAILABLE", payload)

        # Schedule orphan recovery: if no COMMIT seen after window + timeout, re-announce
        loop = asyncio.get_running_loop()
        loop.call_later(
            COMMIT_WINDOW_MS / 1000 + ORPHAN_TIMEOUT_S,
            lambda: asyncio.create_task(self._check_orphan(job_id, payload)),
        )
        return job_id

    # ── Handlers ───────────────────────────────────────────────────────────────

    async def _handle_task(self, msg: Dict[str, Any]) -> None:
        payload = msg["payload"]
        job_id: str = payload["job_id"]
        required_cap: str = payload.get("capability", "any")

        if required_cap not in VALID_CAPABILITIES:
            return  # reject unknown capability to prevent malformed task injection
        if job_id in self._committed_jobs:
            return
        if required_cap != "any" and required_cap != self.capability:
            return

        # Store task payload for winner to use
        self._task_payloads[job_id] = payload

        # Create our bid
        our_bid = Bid(
            job_id=job_id,
            bidder_id=self.node.node_id,
            bidder_role=self.node.role,
            load_score=self._load_fn(),
            capability=self.capability,
        )
        self._pending_bids.setdefault(job_id, []).append(our_bid)

        # Broadcast our bid
        await self.node.publish("BID", {
            "job_id": job_id,
            "bidder_id": our_bid.bidder_id,
            "bidder_role": our_bid.bidder_role,
            "load_score": our_bid.load_score,
            "capability": our_bid.capability,
            "timestamp_ms": our_bid.timestamp_ms,
        })

        # Schedule evaluation after commit window (explicit capture avoids closure mutation)
        loop = asyncio.get_running_loop()
        jid = job_id
        loop.call_later(
            COMMIT_WINDOW_MS / 1000,
            lambda: asyncio.create_task(self._evaluate_bids(jid))
        )

    async def _handle_bid(self, msg: Dict[str, Any]) -> None:
        payload = msg["payload"]
        job_id: str = payload["job_id"]
        if job_id in self._committed_jobs:
            return
        bid = Bid(
            job_id=job_id,
            bidder_id=payload["bidder_id"],
            bidder_role=payload["bidder_role"],
            load_score=payload["load_score"],
            capability=payload["capability"],
            timestamp_ms=payload["timestamp_ms"],
        )
        bids = self._pending_bids.setdefault(job_id, [])
        # Deduplicate: one bid per bidder (last one wins on re-submission)
        bids[:] = [b for b in bids if b.bidder_id != bid.bidder_id]
        bids.append(bid)

    async def _handle_commit(self, msg: Dict[str, Any]) -> None:
        payload = msg["payload"]
        job_id: str = payload["job_id"]
        winner_id: str = payload["winner_id"]

        if job_id in self._committed_jobs:
            return
        self._committed_jobs.add(job_id)

        # If this node announced this task, start a result watchdog.
        # If the executor commits but then dies, the next-stage COMMIT will never appear
        # and we'll re-announce after RESULT_TIMEOUT_S.
        if job_id in self._result_expected:
            next_jid, orig_payload = self._result_expected[job_id]
            loop = asyncio.get_running_loop()
            loop.call_later(
                RESULT_TIMEOUT_S,
                lambda jid=job_id, njid=next_jid, op=orig_payload: asyncio.create_task(
                    self._check_result(jid, njid, op)
                ),
            )

        won = (winner_id == self.node.node_id)
        task_payload = self._task_payloads.get(job_id)
        for cb in self._commit_callbacks:
            asyncio.create_task(cb(job_id, won, task_payload))

    def _reputation_adjusted_score(self, bid: "Bid") -> float:
        """Adjust load_score by agent reputation — higher reputation = lower effective score.

        Formula: effective = load_score * (1 - REPUTATION_WEIGHT * reputation / MAX_REP)
        Example: rep 300/500, load 0.5 → 0.5 * (1 - 0.3 * 0.6) = 0.5 * 0.82 = 0.41
        Result: experienced agents get a bidding advantage proportional to their track record.
        """
        if not self._economy:
            return bid.load_score
        rep = self._economy.get_reputation(bid.bidder_id)
        from swarm.agent_economy import MAX_REPUTATION
        return bid.load_score * (1.0 - REPUTATION_WEIGHT * rep / MAX_REPUTATION)

    async def _evaluate_bids(self, job_id: str) -> None:
        """After commit window: pick winner deterministically and broadcast COMMIT if we win.

        When an AgentEconomy is wired in, load_score is adjusted by reputation —
        agents with higher reputation get a bidding advantage.  The adjustment is
        deterministic (same economy state → same winner on every node).
        """
        if job_id in self._committed_jobs:
            return
        bids = self._pending_bids.get(job_id, [])
        if not bids:
            return

        # Deterministic winner: min adjusted load → earliest timestamp → smallest ID
        winner = min(bids, key=lambda b: (self._reputation_adjusted_score(b), b.timestamp_ms, b.bidder_id))

        if winner.bidder_id != self.node.node_id:
            # We lost — wait for COMMIT from winner
            return

        # We won — broadcast COMMIT
        self._committed_jobs.add(job_id)
        await self.node.publish("COMMIT", {
            "job_id": job_id,
            "winner_id": self.node.node_id,
            "winner_role": self.node.role,
            "capability": self.capability,
            "committed_at_ms": int(time.time() * 1000),
        })

        task_payload = self._task_payloads.get(job_id)
        for cb in self._commit_callbacks:
            asyncio.create_task(cb(job_id, True, task_payload))

    async def _check_result(self, job_id: str, next_stage_job_id: str, payload: Dict[str, Any]) -> None:
        """Re-announce if executor committed but the next pipeline stage never started.

        This guards against the scenario where a worker dies AFTER winning the bid
        (so committed_jobs suppresses the standard orphan check) but BEFORE producing
        its output and advancing the pipeline.
        """
        if next_stage_job_id in self._committed_jobs:
            return  # next stage committed — executor succeeded

        retries = self._orphan_retries.get(job_id, 0)
        if retries >= ORPHAN_MAX_RETRY:
            print(
                f"[{self.node.role}] ✗ Executor failed for {job_id[:8]} — "
                f"no result after {ORPHAN_MAX_RETRY} retries, giving up"
            )
            self._result_expected.pop(job_id, None)
            return

        self._orphan_retries[job_id] = retries + 1
        print(
            f"[{self.node.role}] ↺ Executor died for {job_id[:8]} — re-announcing "
            f"(attempt {retries + 1}/{ORPHAN_MAX_RETRY})"
        )
        # Clear committed state so a new bidder can win
        self._committed_jobs.discard(job_id)
        self._pending_bids.pop(job_id, None)

        await self.node.publish("TASK_AVAILABLE", payload)

        loop = asyncio.get_running_loop()
        loop.call_later(
            COMMIT_WINDOW_MS / 1000 + RESULT_TIMEOUT_S,
            lambda jid=job_id, njid=next_stage_job_id, op=payload: asyncio.create_task(
                self._check_result(jid, njid, op)
            ),
        )

    async def _check_orphan(self, job_id: str, payload: Dict[str, Any]) -> None:
        """Re-announce a task if no COMMIT was seen within the orphan timeout window."""
        if job_id in self._committed_jobs:
            return  # already handled — not an orphan

        # Guard: only one active re-announcement per job at a time
        if job_id in self._orphan_announced:
            return
        self._orphan_announced.add(job_id)

        retries = self._orphan_retries.get(job_id, 0)
        if retries >= ORPHAN_MAX_RETRY:
            print(
                f"[{self.node.role}] ✗ Job {job_id[:8]} orphaned after "
                f"{ORPHAN_MAX_RETRY} retries — giving up"
            )
            self._orphan_announced.discard(job_id)
            return

        self._orphan_retries[job_id] = retries + 1
        print(
            f"[{self.node.role}] ↺ Orphan detected — re-announcing job "
            f"{job_id[:8]} (attempt {retries + 1}/{ORPHAN_MAX_RETRY})"
        )
        # Clear stale bids so the new window starts fresh
        self._pending_bids.pop(job_id, None)
        self._orphan_announced.discard(job_id)

        await self.node.publish("TASK_AVAILABLE", payload)

        loop = asyncio.get_running_loop()
        jid = job_id  # explicit capture to avoid closure mutation
        loop.call_later(
            COMMIT_WINDOW_MS / 1000 + ORPHAN_TIMEOUT_S,
            lambda: asyncio.create_task(self._check_orphan(jid, payload)),
        )
