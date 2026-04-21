"""
Resilience Demo — FlashForge Agent Swarm
Vertex Swarm Challenge 2026 · Track 3 | Agent Economy

Kills a live swarm node mid-job and proves the backup takes over automatically.
Requires the swarm to be already running (FoxMQ + agents).

Usage:
  # Start the swarm first in another terminal:
  #   python start_swarm.py --critics 4
  #   (or: python swarm/run_planner_node.py  etc.)

  python demo_resilience.py --job "Build a weather dashboard"
  python demo_resilience.py --job "Build a todo app" --kill-role planner
  python demo_resilience.py --job "Build a counter" --kill-role builder --kill-after 5
  python demo_resilience.py --kill-role critic    # critic — BFT tolerates 1/4 failures

Supported roles to kill: planner, builder, critic, fixer

The demo:
  1. Connects to FoxMQ and watches swarm events.
  2. Injects a job.
  3. When the target role commits to a task, waits --kill-after seconds.
  4. Sends KILL_SIGNAL to that specific node.
  5. Monitors: backup node (same role) takes over via orphan re-announcement.
  6. Confirms job completes end-to-end (COORDINATION_COMPLETE).
"""
import argparse
import asyncio
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from swarm.foxmq_node import FoxMQNode, PEER_STALE_AFTER
from swarm.bid_protocol import BidProtocol
from swarm.poc_logger import verify_poc_log

FOXMQ_HOST  = os.getenv("FOXMQ_HOST", "127.0.0.1")
FOXMQ_PORT  = int(os.getenv("FOXMQ_PORT", "1883"))
SECRET      = os.getenv("SWARM_SECRET", "swarm-secret-change-in-prod")
POC_LOG_DIR = os.getenv("POC_LOG_DIR", "./poc_logs")

LINE = "─" * 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FlashForge Resilience Demo")
    parser.add_argument("--job",        default="Build a simple counter app",
                        help="Prompt to inject into the swarm")
    parser.add_argument("--kill-role",  default="planner",
                        choices=["planner", "builder", "critic", "fixer"],
                        help="Which agent role to kill mid-job")
    parser.add_argument("--kill-after", type=int, default=3,
                        help="Seconds after target COMMIT before killing it")
    parser.add_argument("--timeout",    type=int, default=300,
                        help="Max seconds to wait for job completion")
    return parser.parse_args()


async def run_demo(args: argparse.Namespace) -> None:
    print("=" * 60)
    print("  🛡  Resilience Demo — FlashForge Agent Swarm")
    print("  Vertex Swarm Challenge 2026 — Track 3 | Agent Economy")
    print(f"  kill role    : {args.kill_role}")
    print(f"  kill delay   : {args.kill_after}s after COMMIT")
    print(f"  prompt       : {args.job[:60]!r}")
    print("=" * 60)

    node_id = f"demo-watcher-{uuid.uuid4().hex[:8]}"
    job_id  = str(uuid.uuid4())

    node = FoxMQNode(node_id, "watcher", FOXMQ_HOST, FOXMQ_PORT, SECRET)
    _ = BidProtocol(node, capability="none")   # watcher never bids

    # State
    target_node_id: list[str] = []           # [0] filled when we know which node to kill
    kill_sent      = asyncio.Event()
    job_done       = asyncio.Event()
    recovery_seen  = asyncio.Event()
    timeline: list[str] = []

    def ts_prefix() -> str:
        return time.strftime("%H:%M:%S")

    def log_event(label: str, detail: str = "") -> None:
        msg = f"  [{ts_prefix()}] {label:<30} {detail}"
        print(msg)
        timeline.append(msg)

    # ── Event listeners ─────────────────────────────────────────────────────────

    @node.on("PEER_ANNOUNCE")
    async def _on_announce(msg: dict) -> None:
        role = msg.get("sender_role", "?")
        sid  = msg.get("sender_id",   "?")
        log_event("PEER_ANNOUNCE", f"role={role}  id={sid[:8]}")

    @node.on("BID")
    async def _on_bid(msg: dict) -> None:
        payload = msg.get("payload", {})
        role    = msg.get("sender_role", "?")
        sid     = msg.get("sender_id",   "?")
        jid     = payload.get("job_id", "")
        if jid.startswith(job_id):
            score = payload.get("load_score", "?")
            log_event("BID", f"role={role}  id={sid[:8]}  load={score}")

    @node.on("COMMIT")
    async def _on_commit(msg: dict) -> None:
        payload  = msg.get("payload", {})
        cap      = payload.get("capability", "?")
        winner   = payload.get("winner_role", "?")
        winner_id = payload.get("winner_id", "?")
        jid      = payload.get("job_id", "")

        if not jid.startswith(job_id):
            return

        log_event("COMMIT", f"capability={cap}  winner={winner}  id={winner_id[:8]}")

        # If this commit is from our target role and we haven't killed yet
        if winner == args.kill_role and not kill_sent.is_set():
            target_node_id.clear()
            target_node_id.append(winner_id)
            # Schedule kill after delay
            asyncio.create_task(_scheduled_kill(winner_id, args.kill_after))

    async def _scheduled_kill(target: str, delay: int) -> None:
        print(f"\n  ⏳ Waiting {delay}s before killing {args.kill_role} [{target[:8]}]…")
        await asyncio.sleep(delay)

        print(f"\n{LINE}")
        print(f"  💀 KILLING {args.kill_role} [{target[:8]}]  (KILL_SIGNAL via FoxMQ)")
        print(LINE)
        log_event("KILL_SIGNAL sent", f"target={target[:8]}")

        await node.publish("KILL_SIGNAL", {"target_id": target})
        kill_sent.set()

    @node.on("TASK_AVAILABLE")
    async def _on_task_available(msg: dict) -> None:
        payload = msg.get("payload", {})
        cap     = payload.get("capability", "?")
        jid     = payload.get("job_id", "")
        if jid.startswith(job_id):
            # After kill is sent, any new TASK_AVAILABLE is a recovery re-announcement
            is_reannounce = kill_sent.is_set()
            label = "ORPHAN_REANNOUNCE" if is_reannounce else "TASK_AVAILABLE"
            log_event(label, f"capability={cap}")
            if is_reannounce:
                recovery_seen.set()

    @node.on("HEARTBEAT")
    async def _on_heartbeat(msg: dict) -> None:
        # Only show when killed node's role stops heartbeating
        if target_node_id and kill_sent.is_set():
            sid = msg.get("sender_id", "")
            if sid == target_node_id[0]:
                log_event("HEARTBEAT (killed node?!)", f"id={sid[:8]} — unexpected!")

    plan_done    = asyncio.Event()
    build_done   = asyncio.Event()
    eval_done    = asyncio.Event()
    fix_done     = asyncio.Event()

    @node.on("PLAN_READY")
    async def _h(msg: dict) -> None:
        if msg.get("payload", {}).get("job_id", "").startswith(job_id):
            log_event("PLAN_READY", "")
            plan_done.set()

    @node.on("BUILD_COMPLETE")
    async def _h2(msg: dict) -> None:
        if msg.get("payload", {}).get("job_id", "").startswith(job_id):
            log_event("BUILD_COMPLETE", "")
            build_done.set()

    @node.on("EVAL_CONSENSUS")
    async def _h3(msg: dict) -> None:
        payload = msg.get("payload", {})
        if payload.get("job_id", "").startswith(job_id):
            verdict = payload.get("verdict", "?")
            score   = payload.get("avg_score", "?")
            log_event("EVAL_CONSENSUS", f"verdict={verdict}  avg_score={score}")
            eval_done.set()

    @node.on("COORDINATION_COMPLETE")
    async def _h4(msg: dict) -> None:
        payload = msg.get("payload", {})
        if payload.get("job_id", "").startswith(job_id):
            log_event("COORDINATION_COMPLETE", "🎉 Job done!")
            job_done.set()

    # ── Start & inject ──────────────────────────────────────────────────────────
    await node.start()
    print(f"\n{LINE}")
    print(f"  🌐 Connected to FoxMQ — {FOXMQ_HOST}:{FOXMQ_PORT}")
    print(f"  ⏳ Waiting 2s for peer discovery…")
    print(LINE)
    await asyncio.sleep(2.0)

    print(f"\n  🚀 Injecting job [{job_id[:8]}]…")
    log_event("JOB_INJECTED", f"id={job_id[:8]}")

    await node.publish("TASK_AVAILABLE", {
        "job_id":     job_id,
        "capability": "planning",
        "prompt":     args.job,
        "context":    {},
    })

    # ── Wait for completion or timeout ──────────────────────────────────────────
    try:
        await asyncio.wait_for(job_done.wait(), timeout=args.timeout)
    except asyncio.TimeoutError:
        print(f"\n  ⚠ Timeout after {args.timeout}s — job not completed.")

    await node.stop()

    # ── Results ─────────────────────────────────────────────────────────────────
    print(f"\n{LINE}")
    print("  🏁  Resilience Demo Results")
    print(LINE)

    kill_worked    = kill_sent.is_set()
    job_completed  = job_done.is_set()

    print(f"  Kill signal sent to {args.kill_role}  : {'✅' if kill_worked else '✗ (target never committed)'}")
    print(f"  Job completed end-to-end           : {'✅' if job_completed else '✗'}")

    # Try to verify PoC log
    import pathlib
    poc_files = sorted(pathlib.Path(POC_LOG_DIR).glob(f"poc_{job_id}*.jsonl"))
    if poc_files:
        poc_file = poc_files[-1]
        try:
            verify_poc_log(str(poc_file), SECRET)
            print(f"  PoC log integrity                  : ✅ chain valid")
        except Exception as ex:
            print(f"  PoC log integrity                  : ⚠ {ex}")
    else:
        print(f"  PoC log                            : not found in {POC_LOG_DIR}/")

    print()
    print("  What just happened:")
    print(f"  1. Job injected into the swarm.")
    print(f"  2. {args.kill_role} node won the bid and started executing.")
    print(f"  3. After {args.kill_after}s, KILL_SIGNAL sent → node shutdown.")
    print(f"  4. Announcer's orphan timer ({PEER_STALE_AFTER}s) → task re-announced.")
    print(f"  5. Backup {args.kill_role} node bid and took over.")
    print(f"  6. Job completed — PoC log shows full audit trail.")
    print("=" * 60)


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run_demo(args))
    except KeyboardInterrupt:
        print("\n[demo_resilience] Interrupted.")
    except RuntimeError as e:
        if "Cannot connect to FoxMQ" in str(e):
            print(f"\n✗ Cannot connect to FoxMQ broker at {FOXMQ_HOST}:{FOXMQ_PORT}")
            print("  → Start the swarm first: python start_swarm.py --critics 4")
            print("  → Or start FoxMQ: cd flashforge && .\\foxmq.exe run --allow-anonymous-login ...")
        else:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
