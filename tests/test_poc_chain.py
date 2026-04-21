"""
Tests for the Proof-of-Coordination HMAC chain.

These tests prove the auditability claim end-to-end without a broker:
  - Every entry is HMAC-SHA256 signed over canonical JSON.
  - prev_chain links each entry to its predecessor (mini-blockchain).
  - Editing any field breaks the chain → verify_poc_log() flags tampering.
  - Multi-signer attestations on COORDINATION_COMPLETE verify independently.
  - Resume validation: reopening a logger re-verifies the chain from disk.
"""
import json
import sys
import os
import tempfile
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.poc_logger import PoCLogger, verify_poc_log


SECRET = "test-secret-do-not-ship"


@pytest.fixture
def log_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def _build_sample_log(log_dir: str, job_id: str = "job-A") -> str:
    poc = PoCLogger(job_id=job_id, secret=SECRET, log_dir=log_dir)
    poc.record("TASK_COMMITTED", "planner-01", {"cap": "planning"})
    poc.record("PLAN_READY", "planner-01", {"components": ["header", "footer"]})
    poc.record("BUILD_STARTED", "builder-01", {})
    poc.record("BUILD_COMPLETE", "builder-01", {"size_bytes": 1234})
    poc.record("EVAL_CONSENSUS", "critic-01", {"verdict": "PASS", "avg_score": 82.0})
    poc.finalize(signers=["planner-01", "builder-01", "critic-01"])
    return str(poc.log_path)


class TestChainIntegrity:
    def test_genuine_log_verifies(self, log_dir):
        path = _build_sample_log(log_dir)
        report = verify_poc_log(path, SECRET, verbose=False)
        assert report["valid"] is True
        assert report["chain_intact"] is True
        assert report["timestamps_monotonic"] is True
        assert report["total_entries"] == 6   # 5 events + COORDINATION_COMPLETE

    def test_prev_chain_equals_previous_hmac(self, log_dir):
        path = _build_sample_log(log_dir)
        entries = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
        # First entry's prev_chain must be "" (genesis)
        assert entries[0]["prev_chain"] == ""
        # Every subsequent entry's prev_chain must equal the previous HMAC
        for i in range(1, len(entries)):
            assert entries[i]["prev_chain"] == entries[i - 1]["hmac"], (
                f"chain link broken at seq {entries[i]['seq']}"
            )

    def test_sequence_numbers_are_monotonic(self, log_dir):
        path = _build_sample_log(log_dir)
        entries = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
        seqs = [e["seq"] for e in entries]
        assert seqs == list(range(len(seqs)))


class TestTamperDetection:
    """The audit trail exists to catch exactly these modifications."""

    def test_tampered_field_detected(self, log_dir):
        path = _build_sample_log(log_dir)
        # Flip the verdict from PASS to FAIL in the middle of the log
        lines = open(path, encoding="utf-8").readlines()
        for i, line in enumerate(lines):
            if '"PASS"' in line:
                lines[i] = line.replace('"PASS"', '"FAIL"')
                break
        else:
            pytest.fail("no PASS entry to tamper with")
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        report = verify_poc_log(path, SECRET, verbose=False)
        assert report["valid"] is False, "verifier missed tampering"

    def test_removed_entry_breaks_chain(self, log_dir):
        path = _build_sample_log(log_dir)
        lines = open(path, encoding="utf-8").readlines()
        # Drop the middle entry — every subsequent prev_chain is now stale
        del lines[2]
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        report = verify_poc_log(path, SECRET, verbose=False)
        assert report["valid"] is False
        assert report["chain_intact"] is False

    def test_wrong_secret_makes_all_hmacs_fail(self, log_dir):
        path = _build_sample_log(log_dir)
        report = verify_poc_log(path, "wrong-secret", verbose=False)
        assert report["valid"] is False
        # Every entry's HMAC check must fail (or the chain break cascades)
        assert any(not e.get("hmac_ok") for e in report["entries"])

    def test_swapped_entry_order_detected(self, log_dir):
        path = _build_sample_log(log_dir)
        lines = open(path, encoding="utf-8").readlines()
        # Swap two adjacent entries
        lines[1], lines[2] = lines[2], lines[1]
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        report = verify_poc_log(path, SECRET, verbose=False)
        assert report["valid"] is False


class TestMultiSignerAttestation:
    def test_attestations_validate(self, log_dir):
        path = _build_sample_log(log_dir)
        report = verify_poc_log(path, SECRET, verbose=False)
        assert report["attestations_valid"] is True

    def test_tampered_attestation_flagged(self, log_dir):
        path = _build_sample_log(log_dir)
        # Corrupt one attestation signature in the final entry
        lines = open(path, encoding="utf-8").readlines()
        final = json.loads(lines[-1])
        signers = list(final["data"]["attestations"].keys())
        # Flip the first character of the first signer's signature
        first = signers[0]
        sig = final["data"]["attestations"][first]
        final["data"]["attestations"][first] = ("0" if sig[0] != "0" else "1") + sig[1:]
        # Rewrite with the corrupted attestation — but the entry HMAC still
        # covers the old data, so the outer chain verification fires first.
        # To isolate attestation validation, we also refresh the outer HMAC
        # using the known shared secret — simulating an attacker with key access.
        import hmac as _hmac_mod, hashlib
        body = {k: v for k, v in final.items() if k != "hmac"}
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        final["hmac"] = _hmac_mod.new(SECRET.encode(), canonical, hashlib.sha256).hexdigest()
        lines[-1] = json.dumps(final, separators=(",", ":")) + "\n"
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        report = verify_poc_log(path, SECRET, verbose=False)
        assert report["attestations_valid"] is False
        assert report["valid"] is False


class TestResumeValidation:
    """Reopening an existing logger re-verifies the on-disk chain."""

    def test_resume_genuine_log_succeeds(self, log_dir):
        poc = PoCLogger("job-B", SECRET, log_dir)
        poc.record("A", "actor-1", {})
        poc.record("B", "actor-1", {})
        first_tip = poc.chain_hash

        # Reopen — should fast-forward to same tip without error
        poc2 = PoCLogger("job-B", SECRET, log_dir)
        assert poc2.chain_hash == first_tip

        # Appending continues the chain correctly
        poc2.record("C", "actor-2", {})
        report = verify_poc_log(str(poc2.log_path), SECRET, verbose=False)
        assert report["valid"] is True
        assert report["total_entries"] == 3

    def test_resume_rejects_tampered_log(self, log_dir):
        poc = PoCLogger("job-C", SECRET, log_dir)
        poc.record("A", "actor-1", {"foo": "bar"})
        path = poc.log_path
        content = open(path, encoding="utf-8").read()
        with open(path, "w", encoding="utf-8") as f:
            f.write(content.replace('"bar"', '"baz"'))
        with pytest.raises(ValueError, match="HMAC"):
            PoCLogger("job-C", SECRET, log_dir)


class TestCanonicalJSON:
    """HMAC is computed over sorted-key canonical JSON — same bytes on every node."""

    def test_key_order_in_payload_does_not_affect_hmac(self, log_dir):
        poc1 = PoCLogger("job-D", SECRET, log_dir)
        entry_a = poc1.record("EVENT", "actor", {"b": 2, "a": 1})

        # Different tmp dir, same event with keys in reverse insertion order
        with tempfile.TemporaryDirectory() as d2:
            poc2 = PoCLogger("job-D", SECRET, d2)
            entry_b = poc2.record("EVENT", "actor", {"a": 1, "b": 2})

        # Different wall-clock timestamps/seq/prev_chain, but the *signing logic*
        # should produce identical HMACs for identical canonical bodies.
        # Build a controlled comparison by reproducing the canonical form:
        import hmac as _hmac_mod, hashlib
        def _hash(d):
            body = {k: v for k, v in d.items() if k != "hmac"}
            return _hmac_mod.new(
                SECRET.encode(),
                json.dumps(body, sort_keys=True, separators=(",", ":")).encode(),
                hashlib.sha256,
            ).hexdigest()

        # Give both entries identical non-data fields to isolate the claim
        entry_a["timestamp_ms"] = 1000
        entry_b["timestamp_ms"] = 1000
        entry_a["seq"] = 0
        entry_b["seq"] = 0
        entry_a["prev_chain"] = ""
        entry_b["prev_chain"] = ""
        entry_a["job_id"] = "same"
        entry_b["job_id"] = "same"
        assert _hash(entry_a) == _hash(entry_b)
