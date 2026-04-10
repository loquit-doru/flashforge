# FlashForge — DoraHacks BUIDL Submission

> **Vertex Swarm Challenge 2026 · Track 3: Agent Economy**
> Deadline: April 6, 2026

---

## Project Name
FlashForge Agent Swarm

## One-liner
Leaderless AI agent swarm with cryptographic Proof of Coordination — the audit trail infrastructure for AI pipelines that regulators, security teams, and enterprises actually need.

## GitHub Repository
https://github.com/loquit-doru/flashforge

---

## Description (for BUIDL form)

> Paste this into the "Project Description" field on DoraHacks.

Companies deploying AI agents in production face a problem that regulators are starting to enforce: **you must prove what an AI agent decided, when, and why.** GDPR, SOX, and HIPAA require audit trails. Most multi-agent frameworks have no answer.

FlashForge is a **leaderless multi-agent coordination system** built natively on a **3-node FoxMQ cluster with Tashi Vertex BFT consensus**. Every job produces a tamper-evident, HMAC-chained **Proof of Coordination** — a mini-blockchain proving which agent did what, in what order, with cryptographic attestations from each participant. Modify a single field and the chain breaks.

Applied here to autonomous full-stack app generation, but the coordination layer is domain-agnostic: security audits, compliance pipelines, infrastructure automation — any workflow where AI decisions must be attributable and tamper-evident.

### What it does

A user submits a natural-language prompt (e.g., *"Build a todo list app with dark theme"*). Six autonomous AI agents — Planner, Builder, 3 Critics (BFT voters), Fixer — receive the job through FoxMQ and self-organize to complete it. There is no master orchestrator. Every agent is an independent process that bids for the task, commits to it, executes it, and records the event on a tamper-evident chain.

### The negotiate → commit → execute → verify loop

| Phase | What happens |
|-------|-------------|
| **Negotiate** | Job announced on `swarm/TASK_AVAILABLE`. All capable agents broadcast a `BID{load_score, timestamp_ms, node_id}`. After 500 ms, every bidder independently computes `min(load_score, timestamp_ms, node_id)` — the same winner, no coordination required. |
| **Commit** | Winner broadcasts `COMMIT`. Losers stand down. Idempotency key prevents double-assignment even if the COMMIT message is duplicated. |
| **Execute** | Winning agent (Planner, then Builder, then Critics, then Fixer) calls its LLM and publishes results back through FoxMQ. |
| **Verify** | Every event is appended to a HMAC-SHA256 chained JSONL log (Proof of Coordination). Any verifier with the shared secret can independently re-compute every HMAC and chain link. |

### FoxMQ / Vertex integration points

- **All inter-agent messages** go through a **3-node FoxMQ cluster** (MQTT 5.0, QoS 1, BFT ordered delivery).
- **Vertex BFT consensus** across the 3 FoxMQ brokers ensures every agent sees messages in the exact same order — eliminating coordination races at the transport level.
- **Peer discovery**: agents announce via `PEER_ANNOUNCE` on MQTT topics + periodic `HEARTBEAT` (2 s interval, `PEER_STALE_AFTER=10 s`).
- **Node failure**: stale node detected via missed heartbeats → orphaned task re-announced with `ORPHAN_TIMEOUT_S=30 s`.

### Track 3 requirement coverage

| Requirement | Implementation |
|-------------|----------------|
| ✅ ≥ 3 agents, full loop | 6 agents (Planner, Builder, Critic ×3, Fixer) |
| ✅ Discover & Form | `PEER_ANNOUNCE` + `HEARTBEAT` — ad-hoc swarms form in < 5 s |
| ✅ Negotiate & Commit | Leaderless bid auction, 500 ms window, deterministic winner |
| ✅ Execute & Prove | PoC HMAC-chain with multi-agent attestations |
| ✅ FoxMQ transport | paho-mqtt over 3-node FoxMQ cluster (MQTT 5.0 + Vertex BFT) |
| ✅ BFT quorum (critics) | 3 critics vote independently; consensus = floor(2n/3)+1 |
| ✅ Resilience | Stale detection, orphan timeout, automatic re-bid |
| ✅ Security | HMAC-SHA256 per message, nonce ring 10K, timestamp TTL 2 min |
| ✅ Auditability | Standalone `verify_poc.py` verifier, no FoxMQ needed |
| ✅ Developer clarity | One-command Docker Compose, live dashboard at :5050 |
| ✅ Hive Memory | Decentralized shared state — agents publish plan/build/eval/fix context to `swarm/HIVE_MEMORY`, no central DB |
| ✅ Agent Economy | Reputation + credits tracked per agent; tiers (novice→elite), deterministic scoring from MQTT events |
| ✅ Coordination Metrics | Real-time latency tracking (bid, eval, pipeline), overhead analysis in dashboard |

---

## Technical Highlights (for judges)

### 1. Coordination Correctness
The bid winner is computed by `min(load_score, timestamp_ms, node_id)` — a pure function applied identically by every bidder. No vote is needed. The 3-node FoxMQ cluster with Vertex BFT ordering guarantees every node saw the same set of bids in the same order before the 500 ms window closes.

**File:** [`swarm/bid_protocol.py`](swarm/bid_protocol.py) → `_evaluate_bids()`

### 2. Resilience
- Heartbeats every 2 s; peer marked `STALE` after 10 s silence.
- If the committed agent disappears, `ORPHAN_TIMEOUT_S` (30 s) triggers a re-announcement — another agent bids and continues the job.
- BFT critic quorum: if 1 of 3 critics drops mid-vote, the remaining 2 still meet quorum.

**Demo:** `python swarm/warmup_demo.py` (runs kill/revive cycle automatically)

### 3. Auditability — Proof of Coordination
Every job produces a HMAC-SHA256 chained `.jsonl` log. Each entry contains `seq`, `actor`, `timestamp_ms`, `prev_chain`, and `hmac`. Modifying any field breaks the chain.

```bash
# Independently verify any completed job:
python swarm/verify_poc.py poc_logs/poc_<job_id>.jsonl
```

Sample entry:
```json
{"seq":4,"event":"EVAL_CONSENSUS","actor":"critic-0f725cc5","timestamp_ms":1774527694215,
 "data":{"verdict":"PASS","avg_score":74.43,"votes":[...],"quorum_met":true},
 "prev_chain":"0b5a17fc...","hmac":"588937ac..."}
```

### 4. Security Posture
| Mechanism | Implementation |
|-----------|----------------|
| Message integrity | HMAC-SHA256, canonical JSON (sorted keys) |
| Replay prevention | Nonce ring buffer (10,000 entries, FIFO), plus timestamp TTL (2 min) |
| Double-commit prevention | `_committed_jobs: Set[str]` idempotency key per node |
| Audit integrity | Hash-chained PoC log — edit any entry → chain breaks |

### 5. Developer Clarity

**Quick start (Docker Compose — zero deps except Docker):**
```bash
cp .env.example .env   # set GROQ_API_KEY (free from console.groq.com)
docker compose up --build
# In another terminal:
docker compose run --rm injector "Build a landing page for a coffee shop"
```

**Live dashboard:** `http://localhost:5050` — shows peer topology, BFT vote table, live event stream, PoC viewer, Hive Memory browser, Agent Economy leaderboard, and Coordination Metrics.

### 6. Hive Memory — Decentralized Shared State

Agents share intermediate state through FoxMQ without a central database. Each agent publishes context to `swarm/HIVE_MEMORY` after completing its work phase:

- **Planner** → plan context (app type, complexity, components)
- **Builder** → build metadata (HTML size, build time, features)
- **Critic** → evaluation consensus (verdict, scores, pass rate)
- **Fixer** → fix results (fixes applied, iterations)

Memory is partitioned by namespace (`plan/build/eval/fix/meta`), has TTL-based expiration, and FIFO eviction at 500 entries. Any agent can query the hive for context from prior pipeline stages.

**File:** [`swarm/hive_memory.py`](swarm/hive_memory.py)

### 7. Agent Economy — Reputation & Credits

Every agent builds a reputation profile tracked deterministically from MQTT events:

| Event | Reputation | Credits |
|-------|-----------|---------|
| Task delivery | +15 | +10 |
| Bid won | +3 | — |
| Consensus led | +8 | — |
| Evaluation cast | +2 | +5 |
| Failure | -10 | — |
| Timeout | -5 | — |

Agents are ranked into tiers: **Novice** (0-99) → **Standard** (100-199) → **Veteran** (200-299) → **Elite** (300+). All agents start at reputation 100 (Standard tier). The economy dashboard tab shows a live leaderboard with tier badges, reputation bars, credit balances, and event feed.

**File:** [`swarm/agent_economy.py`](swarm/agent_economy.py)

**Warm-up demo (single command):**
```bash
python swarm/warmup_demo.py
# Proves: discovery, handshake, heartbeats (30 s), role-state replication (<1 ms), kill/revive
```

---

## Warm-Up Proof

The `warmup_proof.txt` file in the repo contains proof of the 5/5 Stateful Handshake acceptance criteria.

---

## Demo Flow (for judges who want to run it)

```
1. git clone https://github.com/loquit-doru/flashforge
2. cd flashforge
3. cp .env.example .env && echo "GROQ_API_KEY=<your-key>" >> .env
4. docker compose up --build   # starts 3 FoxMQ brokers + 6 agents + dashboard
5. docker compose run --rm injector "Build a portfolio for a developer"
6. Open http://localhost:5050 — watch bidding, coordination, and PoC build live
7. python swarm/verify_poc.py poc_logs/poc_<job_id>.jsonl  # tamper-evident audit
```

---

## Team
Solo submission — [@loquit-doru](https://github.com/loquit-doru)
