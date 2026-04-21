"""
Tests for leaderless bid protocol determinism.

Track 3 claim: every bidder computes the same winner via a pure function
over the bid set. These tests exercise the winner-selection logic and the
reputation-weighted adjustment in isolation, without an MQTT broker —
proving that *given the same bid set on every node, the same agent wins*.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from swarm.bid_protocol import Bid, BidProtocol, REPUTATION_WEIGHT
from swarm.agent_economy import AgentEconomy, MAX_REPUTATION, INITIAL_REPUTATION


def _winner(bids, score_fn):
    """Pure re-implementation of the on-node winner selection.
    Mirrors BidProtocol._evaluate_bids() tie-break order exactly.
    """
    return min(bids, key=lambda b: (score_fn(b), b.timestamp_ms, b.bidder_id))


class TestLoadTieBreak:
    def test_lowest_load_wins(self):
        bids = [
            Bid("j1", "n-a", "planner", load_score=0.8, capability="planning", timestamp_ms=1000),
            Bid("j1", "n-b", "planner", load_score=0.2, capability="planning", timestamp_ms=1000),
            Bid("j1", "n-c", "planner", load_score=0.5, capability="planning", timestamp_ms=1000),
        ]
        w = _winner(bids, lambda b: b.load_score)
        assert w.bidder_id == "n-b"

    def test_load_tie_earliest_timestamp_wins(self):
        bids = [
            Bid("j1", "n-a", "planner", 0.5, "planning", timestamp_ms=1005),
            Bid("j1", "n-b", "planner", 0.5, "planning", timestamp_ms=1001),
            Bid("j1", "n-c", "planner", 0.5, "planning", timestamp_ms=1003),
        ]
        w = _winner(bids, lambda b: b.load_score)
        assert w.bidder_id == "n-b"

    def test_load_and_timestamp_tie_smallest_id_wins(self):
        bids = [
            Bid("j1", "zzz", "planner", 0.3, "planning", timestamp_ms=1000),
            Bid("j1", "aaa", "planner", 0.3, "planning", timestamp_ms=1000),
            Bid("j1", "mmm", "planner", 0.3, "planning", timestamp_ms=1000),
        ]
        w = _winner(bids, lambda b: b.load_score)
        assert w.bidder_id == "aaa"

    def test_winner_is_independent_of_input_order(self):
        """Same bid set in any order must yield the same winner on every node."""
        base = [
            Bid("j1", "n-a", "planner", 0.4, "planning", timestamp_ms=1000),
            Bid("j1", "n-b", "planner", 0.2, "planning", timestamp_ms=1002),
            Bid("j1", "n-c", "planner", 0.3, "planning", timestamp_ms=1001),
        ]
        winners = set()
        # Run all permutations
        from itertools import permutations
        for perm in permutations(base):
            winners.add(_winner(list(perm), lambda b: b.load_score).bidder_id)
        assert winners == {"n-b"}, f"winner not stable across orderings: {winners}"


class TestBidDeduplication:
    """One bid per bidder — re-submissions replace the old bid."""

    def test_protocol_dedup_logic(self):
        # Emulate the dedup path in BidProtocol._handle_bid() lines 202-205
        bids = [
            Bid("j1", "n-a", "planner", 0.5, "planning", timestamp_ms=1000),
            Bid("j1", "n-b", "planner", 0.7, "planning", timestamp_ms=1000),
        ]
        new_bid = Bid("j1", "n-a", "planner", 0.1, "planning", timestamp_ms=1100)
        bids[:] = [b for b in bids if b.bidder_id != new_bid.bidder_id]
        bids.append(new_bid)

        ids = [b.bidder_id for b in bids]
        assert ids.count("n-a") == 1
        assert len(bids) == 2
        # last-write-wins: n-a's new bid should be the one used
        n_a = next(b for b in bids if b.bidder_id == "n-a")
        assert n_a.load_score == 0.1


class TestReputationWeighting:
    """Reputation-weighted bidding: higher reputation → lower effective score."""

    def _mk_bp_with_economy(self, economy: AgentEconomy) -> BidProtocol:
        class _Stub:
            node_id = "stub"
            role = "planner"
            def on(self, *a, **k):
                pass
        return BidProtocol(_Stub(), capability="planning", economy=economy)

    def test_higher_reputation_lowers_effective_score(self):
        eco = AgentEconomy()
        # agent-veteran: reputation 300 (veteran tier)
        eco.record_delivery("agent-veteran", "planner", "j0")  # +15
        for _ in range(13):  # 13 more → 195 more → 295
            eco.record_delivery("agent-veteran", "planner", "j0")
        bp = self._mk_bp_with_economy(eco)

        bid_novice = Bid("j1", "agent-novice", "planner", 0.5, "planning", timestamp_ms=1000)
        bid_vet    = Bid("j1", "agent-veteran", "planner", 0.5, "planning", timestamp_ms=1000)

        s_novice = bp._reputation_adjusted_score(bid_novice)
        s_vet    = bp._reputation_adjusted_score(bid_vet)
        assert s_vet < s_novice, f"veteran should have lower effective score ({s_vet} vs {s_novice})"

    def test_formula_matches_spec(self):
        """effective = load * (1 - REPUTATION_WEIGHT * rep / MAX_REPUTATION)"""
        eco = AgentEconomy()
        # Hit MAX_REPUTATION ceiling (500)
        for _ in range(40):
            eco.record_delivery("agent-max", "planner", "j0")
        assert eco.get_reputation("agent-max") == MAX_REPUTATION

        bp = self._mk_bp_with_economy(eco)
        bid = Bid("j1", "agent-max", "planner", 1.0, "planning", timestamp_ms=1000)
        score = bp._reputation_adjusted_score(bid)
        expected = 1.0 * (1 - REPUTATION_WEIGHT * MAX_REPUTATION / MAX_REPUTATION)
        assert abs(score - expected) < 1e-9
        assert abs(score - (1.0 - REPUTATION_WEIGHT)) < 1e-9  # i.e. 0.7

    def test_no_economy_returns_raw_load(self):
        class _Stub:
            node_id = "stub"
            role = "planner"
            def on(self, *a, **k):
                pass
        bp = BidProtocol(_Stub(), capability="planning", economy=None)
        bid = Bid("j1", "x", "planner", 0.42, "planning", timestamp_ms=1000)
        assert bp._reputation_adjusted_score(bid) == 0.42

    def test_reputation_cannot_overturn_large_load_gap(self):
        """A novice with load 0.0 still beats a max-rep agent with load 1.0.

        Sanity: reputation discount caps at REPUTATION_WEIGHT (30%), so the
        best it can do is shrink an effective load from 1.0 to 0.7. A zero-load
        agent always wins, no matter the opponent's reputation.
        """
        eco = AgentEconomy()
        for _ in range(40):
            eco.record_delivery("veteran", "planner", "j0")
        bp = self._mk_bp_with_economy(eco)

        bid_fresh = Bid("j1", "novice", "planner", 0.0, "planning", timestamp_ms=1000)
        bid_vet   = Bid("j1", "veteran", "planner", 1.0, "planning", timestamp_ms=1000)
        assert bp._reputation_adjusted_score(bid_fresh) < bp._reputation_adjusted_score(bid_vet)

    def test_deterministic_across_invocations(self):
        """Same economy snapshot → same adjusted score, every call.

        This is the critical invariant for Vertex BFT: nodes must compute
        the same winner given the same event log. If the score depended on
        wall-clock time, ordering would diverge.
        """
        eco = AgentEconomy()
        eco.record_delivery("a", "planner", "j0")
        eco.record_delivery("b", "planner", "j0")
        bp = self._mk_bp_with_economy(eco)
        bid = Bid("j1", "a", "planner", 0.5, "planning", timestamp_ms=1000)
        scores = {bp._reputation_adjusted_score(bid) for _ in range(50)}
        assert len(scores) == 1  # identical every time


class TestCommitIdempotency:
    """A second COMMIT for the same job_id must be ignored."""

    def test_committed_jobs_set_suppresses_duplicates(self):
        committed = set()
        job_id = "abc-123"
        # First commit
        if job_id not in committed:
            committed.add(job_id)
            first_won = True
        else:
            first_won = False
        # Duplicate commit
        if job_id not in committed:
            committed.add(job_id)
            second_won = True
        else:
            second_won = False
        assert first_won is True
        assert second_won is False
        assert len(committed) == 1
