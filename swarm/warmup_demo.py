"""
Warm-Up Demo — FlashForge Agent Swarm
Vertex Swarm Challenge 2026 · Track 3 | Agent Economy

Proves all 5 warm-up criteria in ~75 seconds:
  1. Peer discovery + signed handshake
  2. Active heartbeats for 30 s
  3. Role-state change published by Agent A, acknowledged by Agent B in < 1 s
  4. Failure injection: Agent A killed → B marks it STALE after PEER_STALE_AFTER
  5. Recovery: Agent A reconnects → B marks it ONLINE automatically

Requires FoxMQ broker running on localhost:1883.
Run:  .\foxmq.exe run --allow-anonymous-login -f foxmq.d/key_0.pem -L 0.0.0.0:1883 -C 0.0.0.0:19793 foxmq.d
"""
import asyncio
import os
import sys
import time
import uuid

# Allow running from flashforge/ root or flashforge/swarm/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.foxmq_node import FoxMQNode, PEER_STALE_AFTER, HEARTBEAT_INTERVAL

FOXMQ_HOST  = os.getenv("FOXMQ_HOST", "127.0.0.1")
FOXMQ_PORT  = int(os.getenv("FOXMQ_PORT", "1883"))
SECRET      = os.getenv("SWARM_SECRET", "swarm-secret-change-in-prod")

LINE = "─" * 60

def header(title: str) -> None:
    print(f"\n{LINE}")
    print(f"  {title}")
    print(LINE)


async def run_warmup() -> None:
    print("=" * 60)
    print("  🤝  Warm-Up: Stateful Handshake")
    print("  Vertex Swarm Challenge 2026 — Track 3 | Agent Economy")
    print(f"  broker : FoxMQ {FOXMQ_HOST}:{FOXMQ_PORT}")
    print(f"  stale threshold : {PEER_STALE_AFTER}s  |  heartbeat : {HEARTBEAT_INTERVAL}s")
    print("=" * 60)

    # ── Phase 1: Discovery & Handshake ─────────────────────────────────────────
    header("Phase 1 — Discovery & Handshake")

    node_a = FoxMQNode(
        node_id=f"agent-A",
        role="scout",
        foxmq_host=FOXMQ_HOST,
        foxmq_port=FOXMQ_PORT,
        hmac_secret=SECRET,
    )
    node_b = FoxMQNode(
        node_id=f"agent-B",
        role="carrier",
        foxmq_host=FOXMQ_HOST,
        foxmq_port=FOXMQ_PORT,
        hmac_secret=SECRET,
    )

    await node_a.start()
    await node_b.start()

    # Give nodes time to exchange PEER_ANNOUNCE messages
    await asyncio.sleep(2.0)

    # Verify mutual discovery
    a_peers = node_a.peer_summary()
    b_peers = node_b.peer_summary()
    print(f"\n  [agent-A] peers: {a_peers}")
    print(f"  [agent-B] peers: {b_peers}")

    a_sees_b = any(
        s["role"] == "carrier"
        for s in node_a._peer_states.values()
        if s["status"] == "online"
    )
    b_sees_a = any(
        s["role"] == "scout"
        for s in node_b._peer_states.values()
        if s["status"] == "online"
    )

    if not (a_sees_b and b_sees_a):
        print("\n  ✗ FAIL — peers did not discover each other within 2 s")
        print("  → Is FoxMQ running? Check FOXMQ_HOST / FOXMQ_PORT.")
        await node_a.stop()
        await node_b.stop()
        sys.exit(1)

    print("\n  ✓ PASS — mutual peer discovery via signed PEER_ANNOUNCE")

    # ── Phase 2: Heartbeat watch (30 s) ────────────────────────────────────────
    header("Phase 2 — Heartbeat watch (30 s)")

    start = time.monotonic()
    tick  = 0
    all_healthy = True
    while tick < 15:               # 15 ticks × 2 s = 30 s
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        tick += 1
        elapsed = int(time.monotonic() - start)
        a_ok = node_a.peer_summary()
        b_ok = node_b.peer_summary()
        print(f"  [{elapsed:2d}s]  A sees: {a_ok}  |  B sees: {b_ok}")

        if "✗" in a_ok or "✗" in b_ok:
            all_healthy = False
            break

    if all_healthy:
        print("\n  ✓ PASS — both nodes healthy for 30 s")
    else:
        print("\n  ✗ FAIL — peer went stale during heartbeat watch")

    # ── Phase 3: Role-state replication ────────────────────────────────────────
    header("Phase 3 — Role-state replication (target: <1 000 ms)")

    role_change_latency_ms: list[float] = []

    async def _on_role_change(msg: dict) -> None:
        payload = msg.get("payload", {})
        if payload.get("event") == "ROLE_CHANGE" and msg.get("sender_id") == "agent-A":
            sent_ms   = payload.get("timestamp_ms", 0)
            recv_ms   = int(time.time() * 1000)
            latency   = recv_ms - sent_ms
            role_change_latency_ms.append(latency)
            print(
                f"  [agent-B] ✓ ROLE_CHANGE ← agent-A  "
                f"new_role='{payload.get('new_role')}'  latency={latency}ms"
            )

    node_b.on("HIVE_MEMORY", _on_role_change)

    sent_at = int(time.time() * 1000)
    await node_a.publish("HIVE_MEMORY", {
        "event":        "ROLE_CHANGE",
        "new_role":     "scout_active",
        "timestamp_ms": sent_at,
    })
    await asyncio.sleep(1.5)

    if role_change_latency_ms:
        latency = role_change_latency_ms[0]
        if latency < 1000:
            print(f"\n  ✓ B acknowledged in {latency} ms  ✅ PASS")
        else:
            print(f"\n  ⚠ B acknowledged in {latency} ms — over 1 000 ms threshold")
    else:
        print("\n  ✗ FAIL — B did not receive ROLE_CHANGE message")

    # ── Phase 4: Failure injection ──────────────────────────────────────────────
    header(f"Phase 4 — Failure injection: killing Agent A for {int(PEER_STALE_AFTER) + 3} s")

    print(f"  💀 Agent A stopped (simulating crash / dead battery / network loss)")
    await node_a.stop()

    stale_detected = False
    deadline = int(PEER_STALE_AFTER) + 5
    for sec in range(1, deadline + 1):
        await asyncio.sleep(1.0)
        b_view = node_b._peer_states.get("agent-A", {})
        status = b_view.get("status", "unknown")
        print(f"  [{sec:2d}s]  B's view of agent-A: {status}")
        if status == "stale":
            stale_detected = True
            # continue printing for a few more seconds
            for s2 in range(sec + 1, min(sec + 4, deadline + 1)):
                await asyncio.sleep(1.0)
                b_view2 = node_b._peer_states.get("agent-A", {})
                print(f"  [{s2:2d}s]  B's view of agent-A: {b_view2.get('status', 'unknown')}")
            break

    if stale_detected:
        print(f"\n  ✓ PASS — agent-A marked STALE after {PEER_STALE_AFTER}s silence")
    else:
        print(f"\n  ⚠ WARN — agent-A not yet marked STALE (may need more time)")

    # ── Phase 5: Recovery ───────────────────────────────────────────────────────
    header("Phase 5 — Recovery: Agent A reconnects")

    node_a2 = FoxMQNode(
        node_id="agent-A",
        role="scout",
        foxmq_host=FOXMQ_HOST,
        foxmq_port=FOXMQ_PORT,
        hmac_secret=SECRET,
    )
    await node_a2.start()
    print("  🟢 Agent A back online...")

    online_detected = False
    for sec in range(1, 8):
        await asyncio.sleep(1.5)
        b_view = node_b._peer_states.get("agent-A", {})
        status = b_view.get("status", "unknown")
        print(f"  [{sec * 2 - 1}s]  B's view of agent-A: {status}")
        if status == "online":
            online_detected = True
            break

    if online_detected:
        print("\n  ✓ PASS — agent-A automatically marked ONLINE again")
    else:
        print("\n  ⚠ WARN — agent-A not yet seen ONLINE by B")

    await node_a2.stop()
    await node_b.stop()

    # ── Summary ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  🏁  Warm-Up Complete")
    print("=" * 60)
    print("  1. Peer discovery + signed handshake       ✅")
    print(f"  2. Active heartbeats for 30 s              {'✅' if all_healthy else '⚠'}")
    print(f"  3. Role-state replication < 1 000 ms       {'✅' if role_change_latency_ms and role_change_latency_ms[0] < 1000 else '⚠'}")
    print(f"  4. Failure injection → STALE detected       {'✅' if stale_detected else '⚠'}")
    print(f"  5. Recovery → ONLINE restored               {'✅' if online_detected else '⚠'}")
    print()
    print("  See warmup_proof.txt for pre-recorded sample output.")
    print("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(run_warmup())
    except KeyboardInterrupt:
        print("\n[warmup_demo] Interrupted.")
    except RuntimeError as e:
        print(f"\n[warmup_demo] ✗ Error: {e}")
        sys.exit(1)
