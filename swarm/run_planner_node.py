"""
Planner Node — FlashForge Swarm participant.

Listens for TASK_AVAILABLE{capability="planning"}, bids, and if it wins:
  1. Calls PlannerAgent.analyze_prompt(prompt) to create an ImplementationPlan.
  2. Records the event in the PoC log.
  3. Announces TASK_AVAILABLE{capability="building"} for the next stage.

Environment variables:
  NODE_ID         (optional, auto-generated)
  FOXMQ_HOST      default "127.0.0.1"
  FOXMQ_PORT      default 1883
  SWARM_SECRET    default "swarm-secret-change-in-prod"
  POC_LOG_DIR     default "./poc_logs"
"""
import asyncio
import os
import sys
import uuid

# Allow running as `python swarm/run_planner_node.py` from the flashforge/ dir
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.foxmq_node import FoxMQNode
from swarm.bid_protocol import BidProtocol
from swarm.poc_logger import PoCLogger
from swarm.hive_memory import make_hive_payload, HIVE_TOPIC
from swarm.agent_economy import AgentEconomy

NODE_ID = os.getenv("NODE_ID", f"planner-{uuid.uuid4().hex[:8]}")
FOXMQ_HOST = os.getenv("FOXMQ_HOST", "127.0.0.1")
FOXMQ_PORT = int(os.getenv("FOXMQ_PORT", "1883"))
SWARM_SECRET = os.getenv("SWARM_SECRET", "swarm-secret-change-in-prod")
POC_LOG_DIR = os.getenv("POC_LOG_DIR", "./poc_logs")


async def main() -> None:
    from agents.planner import PlannerAgent

    _active_tasks = 0

    def _get_load() -> float:
        return min(_active_tasks / 4.0, 1.0)

    node = FoxMQNode(NODE_ID, "planner", FOXMQ_HOST, FOXMQ_PORT, SWARM_SECRET)
    economy = AgentEconomy()
    bidder = BidProtocol(node, capability="planning", load_fn=_get_load, economy=economy)
    planner = PlannerAgent()

    # Economy hook: deduct credits + broadcast LLM_SPENT on every LLM call
    def _on_llm_spend(provider: str) -> None:
        economy.spend_credits(NODE_ID, "planner", provider)
        asyncio.get_event_loop().call_soon(
            lambda: asyncio.create_task(node.publish("LLM_SPENT", {
                "agent_id": NODE_ID, "provider": provider,
            }))
        )
    planner.llm.set_spend_hook(_on_llm_spend)

    # Wire economy into MQTT events — every node maintains a local replica
    for evt in ("COMMIT", "PLAN_READY", "BUILD_COMPLETE", "EVAL_VOTE",
                "EVAL_CONSENSUS", "FIX_COMPLETE", "LLM_SPENT", "PEER_ANNOUNCE"):
        node.on(evt, lambda msg, _et=evt: economy.process_swarm_event(
            _et, msg.get("sender_id", ""), msg.get("sender_role", ""),
            msg.get("payload", {}),
        ))

    async def on_commit(job_id: str, won: bool, task_payload: dict | None) -> None:
        nonlocal _active_tasks
        if not won or task_payload is None:
            return
        _active_tasks += 1

        prompt: str = task_payload.get("prompt", "Build a web app")
        print(f"[planner] 🏆 Won job {job_id[:8]} — analysing: {prompt[:60]!r}")

        poc = PoCLogger(job_id, SWARM_SECRET, POC_LOG_DIR)
        poc.record("TASK_COMMITTED", NODE_ID, {"stage": "planning", "prompt": prompt[:120]})

        try:
            plan = await planner.analyze_prompt(prompt)
            plan_dict = plan.to_dict()
            poc.record("PLAN_READY", NODE_ID, {
                "app_type": plan_dict.get("app_type"),
                "complexity": plan_dict.get("complexity"),
                "components": plan_dict.get("components", [])[:5],
            })

            # Hand off to builder via the swarm.
            # next_stage_job_id: if builder dies after commit, re-announce building.
            await bidder.announce_task(
                prompt=prompt,
                capability="building",
                context={"plan": plan_dict},
                job_id=f"{job_id}:build",
                next_stage_job_id=f"{job_id}:eval",
            )

            # Publish to Hive Memory — share planning context with the swarm
            await node.publish(HIVE_TOPIC, make_hive_payload(
                namespace="plan", key=f"plan:{job_id[:8]}",
                value={
                    "app_type": plan_dict.get("app_type"),
                    "complexity": plan_dict.get("complexity"),
                    "components": plan_dict.get("components", [])[:5],
                    "prompt_summary": prompt[:100],
                },
                author_id=NODE_ID, author_role="planner", job_id=job_id,
            ))

            print(f"[planner] ✓ Plan ready — announced building task for job {job_id[:8]}")

        except Exception as exc:
            print(f"[planner] ✗ Planning failed: {exc}")
            poc.record("PLAN_FAILED", NODE_ID, {"error": str(exc)})
        finally:
            _active_tasks -= 1

    bidder.on_commit(on_commit)
    await node.start()

    print(f"[planner] Listening — FoxMQ {FOXMQ_HOST}:{FOXMQ_PORT}")
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("[planner] Shutting down…")
        await node.stop()


if __name__ == "__main__":
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    while True:
        try:
            asyncio.run(main())
            break
        except SystemExit as e:
            if e.code == 42:
                import time as _time
                NODE_ID = f"planner-{uuid.uuid4().hex[:8]}"
                print(f"[planner] 🔄 Auto-respawn in 3s as {NODE_ID[:12]}…")
                _time.sleep(3)
                continue
            raise
