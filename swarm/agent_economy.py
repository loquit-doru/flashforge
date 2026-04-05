"""
Agent Economy — Reputation and credit tracking for FlashForge swarm agents.

Track 3 is literally called "Agent Economy". This module tracks:
  - Reputation: earned by completing tasks successfully, lost on failure/timeout
  - Credits: earned per successful delivery, spent on task submission
  - Leaderboard: real-time ranking of agents by reputation

Design:
  - No central authority — economy events propagate via FoxMQ like all other messages.
  - Every node maintains a LOCAL replica of the economy (eventual consistency via BFT ordering).
  - Reputation scores are deterministic (same events → same scores on every node).
  - MQTT topic: swarm/ECONOMY

Events:
  TASK_DELIVERED   → agent earns reputation + credits
  TASK_FAILED      → agent loses reputation
  TASK_TIMEOUT     → agent loses some reputation
  BID_WON          → agent earns small reputation (selected by swarm)
  CONSENSUS_LED    → leader earns reputation for successful consensus
"""
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ── Scoring Constants ──────────────────────────────────────────────────────────
INITIAL_REPUTATION     = 100
REPUTATION_DELIVER     = 15     # successfully delivered a task phase
REPUTATION_BID_WON     = 3      # won a bid (selected by swarm)
REPUTATION_CONSENSUS   = 8      # led a successful consensus
REPUTATION_FAIL        = -10    # failed a task
REPUTATION_TIMEOUT     = -5     # timed out / orphaned
CREDITS_PER_DELIVERY   = 10     # credits earned per successful delivery
CREDITS_PER_EVAL       = 5      # credits earned per evaluation
MAX_REPUTATION         = 500
MIN_REPUTATION         = 0

# ── Economy Event Types ────────────────────────────────────────────────────────
ECONOMY_TOPIC = "ECONOMY"


@dataclass
class AgentProfile:
    """Economy profile for a single agent."""
    agent_id: str
    role: str
    reputation: int = INITIAL_REPUTATION
    credits: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    bids_won: int = 0
    consensuses_led: int = 0
    last_active_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "role": self.role,
            "reputation": self.reputation,
            "credits": self.credits,
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "bids_won": self.bids_won,
            "consensuses_led": self.consensuses_led,
            "last_active_ms": self.last_active_ms,
            "tier": self.tier,
        }

    @property
    def tier(self) -> str:
        if self.reputation >= 300:
            return "elite"
        if self.reputation >= 200:
            return "veteran"
        if self.reputation >= 100:
            return "standard"
        return "novice"


class AgentEconomy:
    """
    Decentralized economy tracker — maintains agent reputation and credits.

    Deterministic: same sequence of events → same state on every node
    (guaranteed by FoxMQ/Vertex BFT ordering).
    """

    def __init__(self):
        self._agents: Dict[str, AgentProfile] = {}
        self._events: List[Dict[str, Any]] = []   # audit trail
        self._total_credits_minted: int = 0
        self._total_reputation_delta: int = 0

    def _ensure_agent(self, agent_id: str, role: str = "unknown") -> AgentProfile:
        if agent_id not in self._agents:
            self._agents[agent_id] = AgentProfile(agent_id=agent_id, role=role)
        else:
            # Update role if provided
            if role != "unknown":
                self._agents[agent_id].role = role
        self._agents[agent_id].last_active_ms = int(time.time() * 1000)
        return self._agents[agent_id]

    # ── Economy Events ─────────────────────────────────────────────────────────

    def record_delivery(self, agent_id: str, role: str, job_id: str) -> None:
        """Agent successfully delivered a task phase."""
        agent = self._ensure_agent(agent_id, role)
        agent.reputation = min(MAX_REPUTATION, agent.reputation + REPUTATION_DELIVER)
        agent.credits += CREDITS_PER_DELIVERY
        agent.tasks_completed += 1
        self._total_credits_minted += CREDITS_PER_DELIVERY
        self._total_reputation_delta += REPUTATION_DELIVER
        self._log("TASK_DELIVERED", agent_id, role, job_id, REPUTATION_DELIVER, CREDITS_PER_DELIVERY)

    def record_evaluation(self, agent_id: str, role: str, job_id: str) -> None:
        """Agent evaluated a build (critics earn credits for voting)."""
        agent = self._ensure_agent(agent_id, role)
        agent.reputation = min(MAX_REPUTATION, agent.reputation + 2)
        agent.credits += CREDITS_PER_EVAL
        self._total_credits_minted += CREDITS_PER_EVAL
        self._total_reputation_delta += 2
        self._log("EVAL_COMPLETED", agent_id, role, job_id, 2, CREDITS_PER_EVAL)

    def record_bid_won(self, agent_id: str, role: str, job_id: str) -> None:
        """Agent won a bid (selected by the swarm)."""
        agent = self._ensure_agent(agent_id, role)
        agent.reputation = min(MAX_REPUTATION, agent.reputation + REPUTATION_BID_WON)
        agent.bids_won += 1
        self._total_reputation_delta += REPUTATION_BID_WON
        self._log("BID_WON", agent_id, role, job_id, REPUTATION_BID_WON, 0)

    def record_consensus_led(self, agent_id: str, role: str, job_id: str) -> None:
        """Agent led a successful BFT consensus."""
        agent = self._ensure_agent(agent_id, role)
        agent.reputation = min(MAX_REPUTATION, agent.reputation + REPUTATION_CONSENSUS)
        agent.credits += CREDITS_PER_EVAL
        agent.consensuses_led += 1
        self._total_credits_minted += CREDITS_PER_EVAL
        self._total_reputation_delta += REPUTATION_CONSENSUS
        self._log("CONSENSUS_LED", agent_id, role, job_id, REPUTATION_CONSENSUS, CREDITS_PER_EVAL)

    def record_failure(self, agent_id: str, role: str, job_id: str) -> None:
        """Agent failed a task."""
        agent = self._ensure_agent(agent_id, role)
        agent.reputation = max(MIN_REPUTATION, agent.reputation + REPUTATION_FAIL)
        agent.tasks_failed += 1
        self._total_reputation_delta += REPUTATION_FAIL
        self._log("TASK_FAILED", agent_id, role, job_id, REPUTATION_FAIL, 0)

    def record_timeout(self, agent_id: str, role: str, job_id: str) -> None:
        """Agent timed out on a task."""
        agent = self._ensure_agent(agent_id, role)
        agent.reputation = max(MIN_REPUTATION, agent.reputation + REPUTATION_TIMEOUT)
        self._total_reputation_delta += REPUTATION_TIMEOUT
        self._log("TASK_TIMEOUT", agent_id, role, job_id, REPUTATION_TIMEOUT, 0)

    # ── Queries ────────────────────────────────────────────────────────────────

    def leaderboard(self) -> List[Dict[str, Any]]:
        """Return agents sorted by reputation (descending)."""
        agents = sorted(self._agents.values(), key=lambda a: a.reputation, reverse=True)
        return [a.to_dict() for a in agents]

    def get_agent(self, agent_id: str) -> Optional[Dict[str, Any]]:
        agent = self._agents.get(agent_id)
        return agent.to_dict() if agent else None

    @property
    def total_agents(self) -> int:
        return len(self._agents)

    @property
    def total_credits(self) -> int:
        return self._total_credits_minted

    # ── Snapshot for Dashboard ─────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        return {
            "leaderboard": self.leaderboard(),
            "total_agents": self.total_agents,
            "total_credits_minted": self._total_credits_minted,
            "total_reputation_delta": self._total_reputation_delta,
            "recent_events": self._events[-30:],
            "tier_distribution": self._tier_distribution(),
        }

    def _tier_distribution(self) -> Dict[str, int]:
        dist: Dict[str, int] = {"elite": 0, "veteran": 0, "standard": 0, "novice": 0}
        for agent in self._agents.values():
            dist[agent.tier] = dist.get(agent.tier, 0) + 1
        return dist

    # ── Internal ───────────────────────────────────────────────────────────────

    def _log(self, event_type: str, agent_id: str, role: str, job_id: str,
             rep_delta: int, credits_delta: int) -> None:
        self._events.append({
            "event": event_type,
            "agent_id": agent_id,
            "role": role,
            "job_id": job_id,
            "reputation_delta": rep_delta,
            "credits_delta": credits_delta,
            "timestamp_ms": int(time.time() * 1000),
        })
        # Keep last 200 events
        if len(self._events) > 200:
            self._events = self._events[-200:]

    # ── Process MQTT events (called from dashboard or agent nodes) ─────────────

    def process_swarm_event(self, msg_type: str, sender_id: str, sender_role: str,
                             payload: dict) -> None:
        """Update economy state from swarm MQTT events (deterministic)."""
        job_id = payload.get("job_id", "")

        if msg_type == "COMMIT":
            winner_id = payload.get("winner_id", sender_id)
            winner_role = payload.get("winner_role", sender_role)
            self.record_bid_won(winner_id, winner_role, job_id)

        elif msg_type == "PLAN_READY":
            self.record_delivery(sender_id, sender_role, job_id)

        elif msg_type == "BUILD_COMPLETE":
            self.record_delivery(sender_id, sender_role, job_id)

        elif msg_type == "EVAL_VOTE":
            critic_id = payload.get("critic_id", sender_id)
            self.record_evaluation(critic_id, "critic", job_id)

        elif msg_type == "EVAL_CONSENSUS":
            self.record_consensus_led(sender_id, sender_role, job_id)
            if payload.get("verdict") == "FAIL":
                # The builder gets a small reputation hit on FAIL
                pass  # no punitive action — focus on positive reinforcement

        elif msg_type == "FIX_COMPLETE":
            self.record_delivery(sender_id, sender_role, job_id)

        elif msg_type == "COORDINATION_COMPLETE":
            # Bonus for all participants
            pass

        elif msg_type == "PEER_ANNOUNCE":
            self._ensure_agent(sender_id, sender_role)
