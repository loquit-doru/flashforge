"""
Tests for BFT critic consensus.

Track 3 claim: 4 critics vote independently and a supermajority
`floor(2n/3)+1` settles the verdict, tolerating 1 Byzantine failure.
These tests cover quorum arithmetic, settlement (pass/fail), timeout-driven
force_majority, idempotency, and the "split vote" waiting state.
"""
import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from swarm.critic_consensus import CriticConsensus, Vote, VOTE_WINDOW_S


def _vote(critic_id: str, score: float, passed: bool, job: str = "j1") -> Vote:
    return Vote(critic_id=critic_id, job_id=job, score=score, passed=passed)


class TestQuorumFormula:
    """quorum = floor(2n/3) + 1 — BFT supermajority."""

    @pytest.mark.parametrize("n,q", [
        (1, 1),   # single critic fallback
        (2, 2),   # both must agree
        (3, 3),   # unanimous, tolerates 0 failures
        (4, 3),   # tolerates 1 Byzantine failure  ← demo config
        (5, 4),
        (6, 5),
        (7, 5),   # tolerates 2 failures
        (10, 7),
    ])
    def test_quorum_calculation(self, n, q):
        c = CriticConsensus(job_id="j1", n_critics=n)
        assert c.quorum == q, f"n={n}: expected quorum={q}, got {c.quorum}"

    def test_n_critics_floored_to_one(self):
        """n<1 is clamped to 1 so quorum doesn't go to zero."""
        c = CriticConsensus(job_id="j1", n_critics=0)
        assert c.n_critics == 1
        assert c.quorum == 1


class TestPassSettlement:
    def test_pass_quorum_reached_four_critics(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        assert c.add_vote(_vote("c1", 80, True)) is None   # 1/3 — below quorum
        assert c.add_vote(_vote("c2", 75, True)) is None   # 2/3 — below quorum
        result = c.add_vote(_vote("c3", 82, True))         # 3/3 → QUORUM
        assert result is not None
        verdict, avg, votes = result
        assert verdict is True
        assert 77.0 < avg < 80.0
        assert len(votes) == 3

    def test_settled_result_is_immutable(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        c.add_vote(_vote("c1", 90, True))
        c.add_vote(_vote("c2", 90, True))
        r1 = c.add_vote(_vote("c3", 90, True))
        # A fourth vote after settlement must not change the locked result.
        r2 = c.add_vote(_vote("c4", 0, False))
        assert r1 is r2
        assert c.has_consensus is True


class TestFailSettlement:
    def test_fail_quorum_ends_evaluation_early(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        c.add_vote(_vote("c1", 20, False))
        c.add_vote(_vote("c2", 30, False))
        result = c.add_vote(_vote("c3", 10, False))
        verdict, avg, _ = result
        assert verdict is False
        assert avg == 20.0


class TestByzantineFaultTolerance:
    """With n=4, quorum=3 → 1 dissenting critic cannot block the verdict."""

    def test_one_dissenter_cannot_block_pass(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        c.add_vote(_vote("c1", 80, True))
        c.add_vote(_vote("c2", 85, True))
        c.add_vote(_vote("byz", 5, False))   # Byzantine / outlier
        result = c.add_vote(_vote("c3", 78, True))
        verdict, _, _ = result
        assert verdict is True

    def test_one_dissenter_cannot_block_fail(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        c.add_vote(_vote("c1", 20, False))
        c.add_vote(_vote("byz", 95, True))
        c.add_vote(_vote("c2", 25, False))
        result = c.add_vote(_vote("c3", 30, False))
        verdict, _, _ = result
        assert verdict is False


class TestSplitVote:
    def test_split_vote_does_not_settle(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        assert c.add_vote(_vote("c1", 80, True)) is None
        assert c.add_vote(_vote("c2", 20, False)) is None
        assert c.add_vote(_vote("c3", 75, True)) is None   # 2 pass vs 1 fail — still below quorum=3
        assert c.has_consensus is False
        # Only when a 4th vote tips either side past quorum does it settle.
        result = c.add_vote(_vote("c4", 82, True))
        assert result is not None and result[0] is True


class TestForceMajority:
    """On quorum timeout, force a verdict from whatever votes arrived."""

    def test_force_majority_from_two_of_four(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        c.add_vote(_vote("c1", 80, True))
        c.add_vote(_vote("c2", 70, True))
        # Two more critics never voted — we hit QUORUM_TIMEOUT_S
        result = c.force_majority()
        assert result is not None
        verdict, avg, votes = result
        assert verdict is True
        assert avg == 75.0
        assert len(votes) == 2

    def test_force_majority_tie_resolves_to_fail(self):
        """`pass_count > len(votes) / 2` — a perfect tie is NOT a pass."""
        c = CriticConsensus(job_id="j1", n_critics=4)
        c.add_vote(_vote("c1", 90, True))
        c.add_vote(_vote("c2", 10, False))
        verdict, _, _ = c.force_majority()
        assert verdict is False

    def test_force_majority_with_no_votes_returns_none(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        assert c.force_majority() is None
        assert c.has_consensus is False

    def test_force_majority_idempotent_after_normal_settle(self):
        c = CriticConsensus(job_id="j1", n_critics=3)
        c.add_vote(_vote("c1", 80, True))
        c.add_vote(_vote("c2", 85, True))
        normal = c.add_vote(_vote("c3", 90, True))
        forced = c.force_majority()
        assert forced is normal   # already-settled result returned as-is


class TestVoteUpdateSemantics:
    """Re-submitting a vote from the same critic replaces the old one."""

    def test_same_critic_last_vote_wins(self):
        c = CriticConsensus(job_id="j1", n_critics=3)
        c.add_vote(_vote("c1", 50, False))
        c.add_vote(_vote("c1", 80, True))   # same critic flips
        # Now 1 pass vote from c1, need 2 more pass votes for quorum=3
        c.add_vote(_vote("c2", 82, True))
        result = c.add_vote(_vote("c3", 85, True))
        assert result is not None
        verdict, _, votes = result
        assert verdict is True
        assert len(votes) == 3  # three unique critics
        # c1's score must be the updated 80, not the initial 50
        c1_vote = next(v for v in votes if v.critic_id == "c1")
        assert c1_vote.score == 80


class TestTimeoutDetection:
    def test_timeout_window(self):
        c = CriticConsensus(job_id="j1", n_critics=4)
        assert c.timed_out() is False
        # Age the tracker past VOTE_WINDOW_S without mocking time
        c._start = time.time() - (VOTE_WINDOW_S + 1)
        assert c.timed_out() is True
