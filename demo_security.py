"""
Security Demo — FlashForge Agent Swarm
Vertex Swarm Challenge 2026 · Track 3 | Agent Economy

Demonstrates all security features WITHOUT requiring FoxMQ:
  1. HMAC-SHA256 message signing (canonical JSON, sorted keys)
  2. HMAC verification — tampered messages are rejected
  3. Replay attack rejection (nonce ring buffer)
  4. Timestamp TTL — old messages dropped automatically
  5. Tamper-evident PoC log — any modification breaks the chain

Run: python demo_security.py
"""
import hashlib
import hmac as _hmac
import json
import os
import sys
import tempfile
import time
import uuid

# Allow running from flashforge/ root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from swarm.poc_logger import PoCLogger, verify_poc_log

LINE = "─" * 60

def _sign(msg: dict, secret: str) -> str:
    """Reproduce FoxMQNode._sign: HMAC-SHA256 over canonical JSON (sorted keys, no hmac field)."""
    body = {k: v for k, v in msg.items() if k != "hmac"}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return _hmac.new(secret.encode(), canonical, hashlib.sha256).hexdigest()


def _verify(msg: dict, secret: str) -> bool:
    expected = msg.get("hmac", "")
    actual   = _sign(msg, secret)
    return _hmac.compare_digest(expected, actual)


def section(title: str) -> None:
    print(f"\n{LINE}")
    print(f"  {title}")
    print(LINE)


def main() -> None:
    print("=" * 60)
    print("  🔐  Security Demo — FlashForge Agent Swarm")
    print("  Vertex Swarm Challenge 2026 — Track 3 | Agent Economy")
    print("  (No FoxMQ required)")
    print("=" * 60)

    SECRET = "demo-swarm-secret-for-test"
    results: dict[str, bool] = {}

    # ── 1. HMAC-SHA256 message signing ─────────────────────────────────────────
    section("1. HMAC-SHA256 Message Signing")

    msg = {
        "type":         "BID",
        "sender_id":    "planner-abc123",
        "sender_role":  "planner",
        "timestamp_ms": int(time.time() * 1000),
        "nonce":        str(uuid.uuid4()),
        "payload":      {"job_id": "job-001", "load_score": 0.12},
    }
    msg["hmac"] = _sign(msg, SECRET)

    print(f"  Message type  : {msg['type']}")
    print(f"  Sender        : {msg['sender_id']}")
    print(f"  HMAC (sha256) : {msg['hmac'][:32]}…")

    verified = _verify(msg, SECRET)
    print(f"\n  Verify with correct secret : {'✅ PASS' if verified else '✗ FAIL'}")
    results["signing"] = verified

    # ── 2. Tamper detection ────────────────────────────────────────────────────
    section("2. Tamper Detection")

    tampered = dict(msg)
    tampered["payload"] = dict(tampered["payload"])
    tampered["payload"]["load_score"] = 0.0  # attacker lowers load score to win the bid
    tampered_ok = _verify(tampered, SECRET)

    print(f"  Original load_score : 0.12")
    print(f"  Tampered load_score : 0.0  (attacker lowers score to win bid)")
    print(f"  HMAC check on tampered message : {'PASS ✗ (BUG!)' if tampered_ok else '✅ REJECTED — HMAC mismatch'}")
    results["tamper"] = not tampered_ok

    wrong_secret = _verify(msg, "wrong-secret")
    print(f"  HMAC check with wrong secret   : {'PASS ✗ (BUG!)' if wrong_secret else '✅ REJECTED — wrong key'}")
    results["wrong_secret"] = not wrong_secret

    # ── 3. Replay attack rejection ─────────────────────────────────────────────
    section("3. Replay Attack Rejection (Nonce Ring Buffer)")

    from collections import deque
    nonce_ring: deque[str] = deque(maxlen=1024)

    def process_message(m: dict) -> str:
        """Returns 'accepted' or 'rejected: <reason>'."""
        nonce = m.get("nonce", "")
        if nonce in nonce_ring:
            return "rejected: duplicate nonce (replay)"
        nonce_ring.append(nonce)
        return "accepted"

    # First delivery — should succeed
    result1 = process_message(msg)
    print(f"  First delivery  : {result1}")

    # Replay the same message — nonce already seen
    result2 = process_message(msg)
    print(f"  Replay attempt  : {result2}")

    results["replay"] = (result1 == "accepted" and "rejected" in result2)
    print(f"\n  Replay prevention : {'✅ PASS' if results['replay'] else '✗ FAIL'}")

    # ── 4. Timestamp TTL — old messages dropped ────────────────────────────────
    section("4. Timestamp TTL (Messages > 120 s Dropped)")

    MSG_TTL_MS = 120_000

    # Fresh message — should pass TTL check
    fresh = dict(msg)
    fresh["nonce"] = str(uuid.uuid4())
    fresh["timestamp_ms"] = int(time.time() * 1000)
    fresh["hmac"] = _sign(fresh, SECRET)

    now_ms = int(time.time() * 1000)
    fresh_age = abs(now_ms - fresh["timestamp_ms"])
    fresh_ok = fresh_age <= MSG_TTL_MS

    # Artificially old message
    old = dict(msg)
    old["nonce"] = str(uuid.uuid4())
    old["timestamp_ms"] = int(time.time() * 1000) - 150_000  # 150 s old
    old["hmac"] = _sign(old, SECRET)

    old_age = abs(now_ms - old["timestamp_ms"])
    old_rejected = old_age > MSG_TTL_MS

    print(f"  Fresh message age  : {fresh_age} ms  → {'✅ accepted' if fresh_ok else '✗ rejected'}")
    print(f"  150 s old message  : {old_age} ms  → {'✅ rejected' if old_rejected else '✗ accepted (BUG!)'}")
    results["ttl"] = fresh_ok and old_rejected
    print(f"\n  TTL check : {'✅ PASS' if results['ttl'] else '✗ FAIL'}")

    # ── 5. PoC log integrity — tamper-evident chain ────────────────────────────
    section("5. PoC Log Integrity (Tamper-Evident Hash Chain)")

    job_id = f"demo-{uuid.uuid4().hex[:8]}"
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = PoCLogger(job_id=job_id, secret=SECRET, log_dir=tmpdir)

        logger.record("TASK_COMMITTED", "planner-abc", {"capability": "planning"})
        logger.record("PLAN_READY",     "planner-abc", {"sections": 4})
        logger.record("BUILD_STARTED",  "builder-xyz", {})
        logger.record("BUILD_COMPLETE", "builder-xyz", {"html_bytes": 3200})
        logger.record("EVAL_CONSENSUS", "critic-111",  {"verdict": "PASS", "avg_score": 78.5})
        logger.finalize(["planner-abc", "builder-xyz", "critic-111"])

        import pathlib
        log_path = pathlib.Path(tmpdir) / f"poc_{job_id}.jsonl"
        entries = log_path.read_text(encoding="utf-8").splitlines()
        print(f"  Log file      : poc_{job_id}.jsonl")
        print(f"  Entries       : {len(entries)}")
        print(f"  Chain preview :")
        for line in entries[:3]:
            e = json.loads(line)
            print(f"    seq={e['seq']}  event={e['event']:<20}  hmac={e['hmac'][:16]}…")
        print(f"    …")

        # Verify intact chain
        intact_result = verify_poc_log(str(log_path), SECRET, verbose=False)
        chain_ok = intact_result.get("valid", False)
        print(f"\n  Intact chain verification : {'✅ PASS' if chain_ok else '✗ FAIL'}")

        # Tamper one entry and try to verify
        lines = log_path.read_text(encoding="utf-8").splitlines()
        entry1 = json.loads(lines[1])
        entry1["data"]["sections"] = 99  # attacker inflates complexity
        lines[1] = json.dumps(entry1)
        log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        tamper_result = verify_poc_log(str(log_path), SECRET, verbose=False)
        tamper_detected = not tamper_result.get("valid", True)
        if tamper_detected:
            print(f"  Tampered entry detection  : ✅ CHAIN BREAK DETECTED")
        else:
            print(f"  Tampered entry detection  : ✗ NOT DETECTED (BUG!)")

        results["poc_chain"] = chain_ok and tamper_detected

    # ── Summary ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  🏁  Security Demo Results")
    print("=" * 60)
    checks = [
        ("HMAC signing + canonical JSON",   results.get("signing", False)),
        ("Tamper detection",                results.get("tamper", False) and results.get("wrong_secret", False)),
        ("Replay attack rejection",         results.get("replay", False)),
        ("Timestamp TTL enforcement",       results.get("ttl", False)),
        ("PoC log tamper-evident chain",    results.get("poc_chain", False)),
    ]
    for label, passed in checks:
        icon = "✅" if passed else "✗"
        print(f"  {icon}  {label}")

    total = sum(1 for _, p in checks if p)
    print(f"\n  Result: {total}/{len(checks)} checks passed")
    print("=" * 60)

    if total < len(checks):
        sys.exit(1)


if __name__ == "__main__":
    main()
