"""
Critic Node — FlashForge Swarm (Multi-Critic BFT Consensus Edition).

Every critic node evaluates the build INDEPENDENTLY and publishes an EVAL_VOTE.
ONE critic wins the bid and becomes the CONSENSUS LEADER, responsible for:
  1. Collecting EVAL_VOTEs from all critics via FoxMQ.
  2. Detecting BFT supermajority (quorum = floor(2n/3)+1).
  3. Publishing EVAL_CONSENSUS (the tamper-evident collective verdict).
  4. Advancing the pipeline (fix or finalize PoC).

This directly leverages FoxMQ/Vertex BFT — no single critic can manipulate the
verdict; the swarm's collective judgment is ordered and auditable.

Environment variables:
  NODE_ID           (optional, auto-generated)
  FOXMQ_HOST        default "127.0.0.1"
  FOXMQ_PORT        default 1883
  SWARM_SECRET      default "swarm-secret-change-in-prod"
  POC_LOG_DIR       default "./poc_logs"
  PASS_THRESHOLD    minimum score to pass    (default 75)
  CRITICS_EXPECTED  expected critic count for quorum calculation (default 1)
  QUORUM_TIMEOUT_S  seconds to wait before forcing majority   (default 20)
"""
import asyncio
import os
import sys
import time
import uuid
from typing import Dict, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.foxmq_node import FoxMQNode
from swarm.bid_protocol import BidProtocol
from swarm.poc_logger import PoCLogger
from swarm.critic_consensus import CriticConsensus, ConsensusResult, Vote
from swarm.hive_memory import make_hive_payload, HIVE_TOPIC
from swarm.agent_economy import AgentEconomy

NODE_ID          = os.getenv("NODE_ID",          f"critic-{uuid.uuid4().hex[:8]}")
FOXMQ_HOST       = os.getenv("FOXMQ_HOST",       "127.0.0.1")
FOXMQ_PORT       = int(os.getenv("FOXMQ_PORT",   "1883"))
SWARM_SECRET     = os.getenv("SWARM_SECRET",     "swarm-secret-change-in-prod")
POC_LOG_DIR      = os.getenv("POC_LOG_DIR",      "./poc_logs")
PASS_THRESHOLD   = float(os.getenv("PASS_THRESHOLD",   "62"))
CRITICS_EXPECTED = int(os.getenv("CRITICS_EXPECTED",   "1"))
QUORUM_TIMEOUT_S = float(os.getenv("QUORUM_TIMEOUT_S", "20"))


async def main() -> None:
    from agents.builder import BuildResult
    from agents.critic import CriticAgent

    _active_tasks = 0

    def _get_load() -> float:
        return min(_active_tasks / 4.0, 1.0)

    node         = FoxMQNode(NODE_ID, "critic", FOXMQ_HOST, FOXMQ_PORT, SWARM_SECRET)
    economy      = AgentEconomy()
    bidder       = BidProtocol(node, capability="evaluation", load_fn=_get_load, economy=economy)
    critic_agent = CriticAgent()

    # Economy hook: deduct credits + broadcast LLM_SPENT on every LLM call
    def _on_llm_spend(provider: str) -> None:
        economy.spend_credits(NODE_ID, "critic", provider)
        asyncio.get_event_loop().call_soon(
            lambda: asyncio.create_task(node.publish("LLM_SPENT", {
                "agent_id": NODE_ID, "provider": provider,
            }))
        )
    critic_agent.llm.set_spend_hook(_on_llm_spend)

    # Wire economy into MQTT events — every node maintains a local replica
    for evt in ("COMMIT", "PLAN_READY", "BUILD_COMPLETE", "EVAL_VOTE",
                "EVAL_CONSENSUS", "FIX_COMPLETE", "LLM_SPENT", "PEER_ANNOUNCE"):
        node.on(evt, lambda msg, _et=evt: economy.process_swarm_event(
            _et, msg.get("sender_id", ""), msg.get("sender_role", ""),
            msg.get("payload", {}),
        ))

    # ── Per-job state ───────────────────────────────────────────────────────────
    _trackers:      Dict[str, CriticConsensus] = {}   # job_id → vote tracker
    _task_payloads: Dict[str, dict]            = {}   # job_id → TASK_AVAILABLE payload
    _led_jobs:      Set[str]                   = set() # jobs where WE are consensus leader
    _done_jobs:     Set[str]                   = set() # jobs where EVAL_CONSENSUS published

    # ── VOTER ROLE — every critic evaluates independently ───────────────────────

    async def evaluate_and_vote(job_id: str, task_payload: dict) -> None:
        """Evaluate the build artifact and publish our EVAL_VOTE to the swarm."""
        prompt: str = task_payload.get("prompt", "")
        ctx: dict   = task_payload.get("context", {})
        html: str   = ctx.get("html", "")

        print(f"[critic:{NODE_ID[:8]}] 🗳  Evaluating {job_id[:10]} …")
        try:
            build_result = BuildResult(
                html=html, css=None, js=None,
                success=bool(html), build_time=0.0,
            )
            evaluation = await critic_agent.evaluate(build_result, prompt)
            score      = evaluation.scores.overall
            passed     = score >= PASS_THRESHOLD
            issues     = [
                i.get("description", str(i)) if isinstance(i, dict) else str(i)
                for i in evaluation.issues[:5]
            ]

            await node.publish("EVAL_VOTE", {
                "job_id":       job_id,
                "critic_id":    NODE_ID,
                "score":        round(score, 2),
                "passed":       passed,
                "issues":       issues,
                "timestamp_ms": int(time.time() * 1000),
            })
            print(
                f"[critic:{NODE_ID[:8]}] 📨 EVAL_VOTE published — "
                f"score={score:.1f} {'PASS ✅' if passed else 'FAIL ❌'}"
            )

            # Register OWN vote directly — our own MQTT echo is dropped by FoxMQNode
            tracker = _trackers.get(job_id)
            if tracker:
                own_vote = Vote(
                    critic_id=NODE_ID, job_id=job_id,
                    score=score, passed=passed, issues=issues,
                )
                result = tracker.add_vote(own_vote)
                _print_vote_status(job_id, tracker)
                if result and job_id in _led_jobs and job_id not in _done_jobs:
                    await _publish_consensus(job_id, result)

        except Exception as exc:
            print(f"[critic:{NODE_ID[:8]}] ✗ Evaluation error: {exc}")

    def _print_vote_status(job_id: str, tracker: CriticConsensus) -> None:
        print(
            f"[critic:{NODE_ID[:8]}] 📊 Votes: "
            f"{tracker.vote_count}/{tracker.n_critics} "
            f"(quorum={tracker.quorum}) job={job_id[:10]}"
        )

    # TASK_AVAILABLE fires for ALL critics (voter role — bidder also fires separately)
    @node.on("TASK_AVAILABLE")
    async def _on_task_for_voter(msg: dict) -> None:
        payload = msg["payload"]
        if payload.get("capability") != "evaluation":
            return
        job_id = payload["job_id"]
        if job_id in _done_jobs:
            return

        _task_payloads[job_id] = payload

        # Count expected critics from peer registry + self
        n_critics = max(
            CRITICS_EXPECTED,
            sum(1 for s in node._peer_states.values() if s["role"] == "critic") + 1,
        )
        tracker = CriticConsensus(job_id=job_id, n_critics=n_critics)
        _trackers[job_id] = tracker
        print(
            f"[critic:{NODE_ID[:8]}] 📣 Eval task received — "
            f"expecting {n_critics} critic votes, quorum={tracker.quorum}"
        )
        asyncio.create_task(evaluate_and_vote(job_id, payload))

    # Collect EVAL_VOTEs from peer critics
    @node.on("EVAL_VOTE")
    async def _on_eval_vote(msg: dict) -> None:
        payload = msg["payload"]
        job_id  = payload["job_id"]
        if job_id in _done_jobs:
            return

        tracker = _trackers.get(job_id)
        if not tracker:
            return

        vote = Vote(
            critic_id    = payload["critic_id"],
            job_id       = job_id,
            score        = payload["score"],
            passed       = payload["passed"],
            issues       = payload.get("issues", []),
            timestamp_ms = payload["timestamp_ms"],
        )
        result = tracker.add_vote(vote)
        _print_vote_status(job_id, tracker)

        if result and job_id in _led_jobs and job_id not in _done_jobs:
            await _publish_consensus(job_id, result)

    # ── LEADER ROLE — bid winner collects votes → publishes EVAL_CONSENSUS ──────

    async def on_commit(job_id: str, won: bool, _task_payload_bid: dict | None) -> None:
        nonlocal _active_tasks
        if not won:
            return
        _active_tasks += 1

        print(f"[critic:{NODE_ID[:8]}] 👑 CONSENSUS LEADER for {job_id[:10]}")
        _led_jobs.add(job_id)

        try:
            # Fast path: quorum already reached before we won the bid
            tracker = _trackers.get(job_id)
            if tracker and tracker.result and job_id not in _done_jobs:
                await _publish_consensus(job_id, tracker.result)
                return

            # Slow path: wait for votes with timeout
            deadline = time.time() + QUORUM_TIMEOUT_S
            while time.time() < deadline:
                await asyncio.sleep(0.3)
                if job_id in _done_jobs:
                    return
                tracker = _trackers.get(job_id)
                if tracker and tracker.result:
                    await _publish_consensus(job_id, tracker.result)
                    return

            # Timeout → force majority from available votes
            tracker = _trackers.get(job_id)
            if tracker and job_id not in _done_jobs:
                result = tracker.force_majority()
                if result:
                    print(f"[critic:{NODE_ID[:8]}] ⏰ Quorum timeout — forcing majority verdict")
                    await _publish_consensus(job_id, result)
        finally:
            _active_tasks -= 1

    async def _publish_consensus(job_id: str, result: ConsensusResult) -> None:
        """Publish EVAL_CONSENSUS and advance the pipeline (leader only)."""
        if job_id in _done_jobs:
            return
        _done_jobs.add(job_id)

        verdict_pass, avg_score, votes = result
        root_job_id = job_id.split(":")[0]

        vote_summary = [
            {"critic": v.critic_id[:8], "score": round(v.score, 1), "passed": v.passed}
            for v in votes
        ]
        await node.publish("EVAL_CONSENSUS", {
            "job_id":    job_id,
            "verdict":   "PASS" if verdict_pass else "FAIL",
            "avg_score": round(avg_score, 2),
            "votes":     vote_summary,
            "quorum":    _trackers[job_id].quorum if job_id in _trackers else 1,
        })

        # Publish to Hive Memory — share evaluation consensus
        await node.publish(HIVE_TOPIC, make_hive_payload(
            namespace="eval", key=f"consensus:{root_job_id[:8]}",
            value={
                "verdict": "PASS" if verdict_pass else "FAIL",
                "avg_score": round(avg_score, 2),
                "n_votes": len(vote_summary),
                "quorum_met": True,
                "pass_rate": round(sum(1 for v in votes if v.passed) / max(len(votes), 1) * 100, 1),
            },
            author_id=NODE_ID, author_role="critic", job_id=root_job_id,
        ))

        poc = PoCLogger(root_job_id, SWARM_SECRET, POC_LOG_DIR)
        tracker = _trackers.get(job_id)
        poc.record("EVAL_CONSENSUS", NODE_ID, {
            "verdict":   "PASS" if verdict_pass else "FAIL",
            "avg_score": round(avg_score, 2),
            "votes":     vote_summary,
            "quorum_met": True,
            "n_critics": tracker.n_critics if tracker else 1,
            "quorum_required": tracker.quorum if tracker else 1,
        })

        print(
            f"[critic:{NODE_ID[:8]}] ⚖️  CONSENSUS: "
            f"{'PASS ✅' if verdict_pass else 'FAIL ❌ → fixer'} "
            f"avg={avg_score:.1f}  n_votes={len(votes)}"
        )

        task_payload = _task_payloads.get(job_id, {})
        ctx          = task_payload.get("context", {})
        prompt       = task_payload.get("prompt", "")
        html         = ctx.get("html", "")

        if verdict_pass:
            poc.finalize(signers=["planner", "builder", "critic-consensus"])
            print(f"[critic:{NODE_ID[:8]}] 🎉 Job {root_job_id[:8]} COMPLETE — PoC: {poc.log_path}")
        else:
            # Aggregate unique issues from all votes
            all_issues: list[str] = []
            for v in votes:
                all_issues.extend(v.issues)
            deduped = list(dict.fromkeys(all_issues))[:5]

            await bidder.announce_task(
                prompt=prompt,
                capability="fixing",
                context={
                    "html":   html,
                    "plan":   ctx.get("plan", {}),
                    "issues": deduped,
                    "score":  round(avg_score, 2),
                },
                job_id=f"{root_job_id}:fix",
            )

    bidder.on_commit(on_commit)
    await node.start()

    print(
        f"[critic]  Listening — FoxMQ {FOXMQ_HOST}:{FOXMQ_PORT} | "
        f"mode=voter+leader | CRITICS_EXPECTED={CRITICS_EXPECTED} | quorum=⌊2n/3⌋+1"
    )
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("[critic]  Shutting down…")
        await node.stop()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    while True:
        try:
            asyncio.run(main())
            break
        except SystemExit as e:
            if e.code == 42:
                import time as _time
                NODE_ID = f"critic-{uuid.uuid4().hex[:8]}"
                print(f"[critic] 🔄 Auto-respawn in 3s as {NODE_ID[:12]}…")
                _time.sleep(3)
                continue
            raise
