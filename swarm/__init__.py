"""FlashForge Agent Swarm — FoxMQ-compatible P2P coordination layer.

Architecture (Vertex Swarm Challenge 2026, Track 3: Agent Economy):
  - Each agent runs as an independent process (FoxMQ node).
  - Nodes discover each other via configured peer endpoints.
  - Tasks are distributed via leaderless bidding (no central orchestrator).
  - All coordination events are logged as HMAC-signed Proof of Coordination.

Pipeline:
  job_injector → [bid] → planner_node → [bid] → builder_node
               → [bid] → critic_node  → [bid] → fixer_node (if needed)
               → COORDINATION_COMPLETE

Quick start (local):
  Terminal 1: python -m swarm.run_planner_node
  Terminal 2: python -m swarm.run_builder_node
  Terminal 3: python -m swarm.run_critic_node
  Terminal 4: python -m swarm.run_fixer_node
  Terminal 5: python -m swarm.job_injector "Build a landing page for a coffee shop"
"""
from .foxmq_node import FoxMQNode
from .bid_protocol import BidProtocol
from .poc_logger import PoCLogger, verify_poc_log

__all__ = ["FoxMQNode", "BidProtocol", "PoCLogger", "verify_poc_log"]
