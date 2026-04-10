# ⚡ FlashForge — Leaderless AI Agent Swarm

> **Vertex Swarm Challenge 2026 · Track 3: Agent Economy**
>
> A production-grade, leaderless multi-agent coordination system built on **FoxMQ + Vertex BFT consensus**.
> Agents discover each other, bid for tasks, execute in parallel, and produce a cryptographically verifiable **Proof of Coordination** — all without a central orchestrator.

---

## Why FlashForge?

Every multi-agent system has a dirty secret: a hidden coordinator that, if it dies, takes the whole swarm with it.

FlashForge eliminates that. Six specialized AI agents — Planner, Builder, three Critics, and a Fixer — self-organize entirely through message passing. No master process. No single point of failure. When a node dies mid-job, the swarm detects it via missed heartbeats and reassigns the work automatically.

**Real-world impact:** Autonomous software generation pipelines where uptime matters. Distributed code review systems. Any multi-agent workflow that needs a cryptographic audit trail proving exactly what ran, when, and by whom.

---

## 🚀 Quick Start — One Command (Docker)

```bash
# 1. Clone and configure
git clone https://github.com/loquit-doru/flashforge && cd flashforge

# 2. Add your API key (free at console.groq.com)
cp .env.example .env
echo "GROQ_API_KEY=your-key-here" >> .env

# 3. Start the full swarm (3 FoxMQ brokers + 6 agents + dashboard)
docker compose up --build

# 4. Inject a job (new terminal)
PROMPT="Build a weather dashboard with dark theme" docker compose run --rm injector

# Dashboard: http://localhost:5050
```

**No Docker?** See [local setup](#local-setup-no-docker) below.

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FlashForge Swarm                             │
│                                                                     │
│  User Prompt                                                        │
│      │                                                              │
│      ▼                                                              │
│  ┌──────────┐   TASK_AVAILABLE    ┌─────────────────────────────┐  │
│  │ Injector │ ─────────────────▶  │   3-node FoxMQ Cluster      │  │
│  └──────────┘                     │   (MQTT 5.0 + Vertex BFT)   │  │
│                                   └──────────────┬──────────────┘  │
│                    ┌─────────────────────────────┼──────────────┐  │
│                    │ BID / COMMIT                │              │  │
│                    ▼                             ▼              ▼  │
│             ┌────────────┐   ┌──────────────┐   ┌────────────────┐ │
│             │  🧠 Planner │   │  🏗 Builder  │   │  🔧 Fixer      │ │
│             │  (Groq)    │   │  (Gemini)    │   │  (Qwen)        │ │
│             └─────┬──────┘   └──────┬───────┘   └───────┬────────┘ │
│                   │   HIVE_MEMORY   │   HIVE_MEMORY     │          │
│                   └────────┬────────┘                   │          │
│                            │ EVAL_VOTE (3x)              │          │
│                            ▼                             │          │
│                   ┌─────────────────┐                   │          │
│                   │  🔍 Critics (×3) │  BFT Quorum (2/3)│          │
│                   │  (Groq mixtral) │──────────────────┘          │
│                   └────────┬────────┘                              │
│                            │ EVAL_CONSENSUS                        │
│                            ▼                                       │
│                   ┌────────────────┐                               │
│                   │  PoC Logger    │ HMAC chain + multi-sig        │
│                   └────────────────┘                               │
└─────────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**
- **No master orchestrator** — every agent discovers peers via `PEER_ANNOUNCE` + `HEARTBEAT`
- **Leaderless bidding** — agents compete on `load_score`; FoxMQ/Vertex orders bids deterministically
- **BFT evaluation** — 3 critics vote independently; consensus requires ≥ 2/3 agreement
- **Verifiable coordination** — every event is HMAC-chained and multi-signed (mini-blockchain)
- **Hive Memory** — agents share state through FoxMQ, no central database

---

## 🎯 Track 3 Requirements — Full Coverage

| Requirement | Implementation |
|-------------|----------------|
| ✅ Discover & Form | `PEER_ANNOUNCE` + `HEARTBEAT` — ad-hoc swarms form in < 5 s |
| ✅ Negotiate & Commit | Leaderless bid auction → deterministic `COMMIT` via Vertex ordering |
| ✅ Execute & Prove | PoC HMAC hash-chain with multi-agent attestations |
| ✅ ≥ 3 agents, full loop | Planner → Builder → 3 Critics → Fixer (6 agents) |
| ✅ FoxMQ / Vertex | All coordination over MQTT 5.0 → 3-node FoxMQ cluster → Vertex BFT |
| ✅ Hive Memory | Decentralized shared state — no central DB, TTL + FIFO eviction |
| ✅ Agent Economy | Reputation + credits tracked per agent; tiers novice→elite |
| ✅ Coordination Metrics | Bid latency, eval latency, pipeline time — live in dashboard |

---

## 🤝 Warm-Up Proof (5/5 Criteria — One Command)

```bash
# Requires FoxMQ running (docker compose up foxmq OR .\foxmq.exe run ...)
python swarm/warmup_demo.py
```

**What it proves in ~75 seconds:**
1. Peer discovery + signed handshake
2. Active heartbeats for 30 s
3. Role-state change published by Agent A, acknowledged by Agent B in < 1 s
4. Failure injection: Agent A killed → B marks it STALE after `PEER_STALE_AFTER`
5. Recovery: Agent A reconnects → B marks it ONLINE automatically

See `warmup_proof.txt` in the repo for sample output.

---

## 📊 Proof of Coordination

Every job produces a tamper-evident, HMAC-chained log:

```jsonl
{"seq":0,"event":"TASK_COMMITTED","actor":"planner-03c1de38","timestamp_ms":1743000001200,"prev_chain":"genesis","hmac":"a3f8..."}
{"seq":1,"event":"PLAN_READY","actor":"planner-03c1de38","timestamp_ms":1743000003500,"prev_chain":"a3f8...","hmac":"b7c2..."}
{"seq":2,"event":"BUILD_STARTED","actor":"builder-45b648a9","timestamp_ms":1743000003600,"prev_chain":"b7c2...","hmac":"d12e..."}
{"seq":3,"event":"BUILD_COMPLETE","actor":"builder-45b648a9","timestamp_ms":1743000071100,"prev_chain":"d12e...","hmac":"e9a1..."}
{"seq":4,"event":"EVAL_CONSENSUS","actor":"critic-33e8c6f2","timestamp_ms":1743000071400,"prev_chain":"e9a1...","hmac":"f4b3...","data":{"verdict":"PASS","avg_score":74.43,"quorum_met":true}}
{"seq":5,"event":"COORDINATION_COMPLETE","actor":"swarm","timestamp_ms":1743000071500,"prev_chain":"f4b3...","hmac":"9c7d..."}
```

**Verify any log (no FoxMQ needed):**
```bash
python swarm/verify_poc.py poc_logs/poc_<job-id>.jsonl
```

**View in dashboard:** `http://localhost:5050` → **Proof of Coordination** tab

Modifying any field in any entry breaks the chain — the verifier catches it.

---

## 🔐 Security Posture

| Feature | Details |
|---------|---------|
| **HMAC-SHA256 signing** | Every MQTT message signed; canonical JSON (sorted keys) for deterministic HMACs |
| **Nonce ring buffer** | 1,024-nonce deque prevents replay within TTL window |
| **Timestamp TTL** | Messages > 120 s old dropped at dispatch layer |
| **Double-guard** | Nonce check + TTL check are independent — both must pass |
| **Double-commit prevention** | `_committed_jobs: Set[str]` idempotency key per node |
| **Audit integrity** | Hash-chained PoC log — edit any entry → chain breaks |

**Security demo (no FoxMQ needed):**
```bash
python demo_security.py
# Shows: HMAC signing, replay rejection, tamper detection, TTL expiry
```

---

## 🛡 Resilience

```bash
# Kill a planner mid-job — backup planner takes over automatically
python demo_resilience.py --job "Build a weather dashboard" --kill-role planner

# Kill a builder
python demo_resilience.py --kill-role builder

# Kill a critic (BFT tolerates 1/3 failures — 2 critics still reach quorum)
python demo_resilience.py --kill-role critic
```

**What happens:**
1. Active node is killed 3 seconds into its task
2. Backup node detects stale peer (after `PEER_STALE_AFTER=10 s`)
3. Re-bids on the orphaned task
4. Job completes — PoC log shows full audit trail

**Failure modes documented in full:** [FAILURE_MODES.md](FAILURE_MODES.md)

---

## 🧠 Hive Memory — Decentralized Shared State

Agents share intermediate context through FoxMQ — no central database. Each agent publishes to `swarm/HIVE_MEMORY` after completing its phase:

| Agent | Publishes |
|-------|-----------|
| Planner | App type, complexity, components |
| Builder | HTML size, build time, feature list |
| Critic | Consensus verdict, scores, pass rate |
| Fixer | Fixes applied, iterations taken |

Memory is partitioned by namespace (`plan/build/eval/fix/meta`), has TTL-based expiration, and FIFO eviction at 500 entries. Any agent can query prior pipeline stages without a shared database.

**File:** [`swarm/hive_memory.py`](flashforge/swarm/hive_memory.py)

**Dashboard tab:** `http://localhost:5050` → **Hive Memory** — namespace bars + entry browser

---

## 💰 Agent Economy — Reputation & Credits

Every agent builds a reputation tracked deterministically from MQTT events:

| Event | Reputation | Credits |
|-------|-----------|---------|
| Task delivery | +15 | +10 |
| Bid won | +3 | — |
| Consensus led | +8 | — |
| Evaluation cast | +2 | +5 |
| Failure | −10 | — |
| Timeout | −5 | — |

Tiers: **Novice** (0–99) → **Standard** (100–199) → **Veteran** (200–299) → **Elite** (300+)

**File:** [`swarm/agent_economy.py`](flashforge/swarm/agent_economy.py)

**Dashboard tab:** `http://localhost:5050` → **Agent Economy** — live leaderboard with tier badges

---

## 🤖 Agent Roles

| Agent | LLM | Responsibility |
|-------|-----|----------------|
| 🧠 **Planner** | Groq (llama-3.3-70b) | Decomposes prompt → `ImplementationPlan` |
| 🏗 **Builder** | Gemini 2.0 Flash | Generates full application code |
| 🔍 **Critic** (×3) | Groq (mixtral-8x7b) | Scores output: functionality, design, speed |
| 🔧 **Fixer** | Qwen (free tier) | Patches code when consensus score < threshold |

All agents are **interchangeable** — multiple instances bid on each task; the lowest-load node wins.

---

## 📊 Dashboard — Real-Time Observability

`http://localhost:5050` — 7 tabs:

| Tab | Shows |
|-----|-------|
| **Peers** | Live network topology, heartbeat status |
| **Jobs** | Active and completed jobs |
| **Events** | Real-time MQTT event stream |
| **Proof of Coordination** | Hash-chain viewer + verifier |
| **Hive Memory** | Namespace bars + entry browser |
| **Agent Economy** | Reputation leaderboard + tier badges |
| **Coordination Metrics** | Bid latency, eval latency, pipeline time |

---

## ⚙️ Configuration

```env
# .env
GROQ_API_KEY=...        # Primary LLM (planner/critic) — fast, cheap, free tier
GEMINI_API_KEY=...      # Builder LLM — free, large context
QWEN_API_KEY=...        # Fixer LLM — free, 5M tokens/min
ANTHROPIC_API_KEY=...   # Fallback — highest quality

FOXMQ_HOST=127.0.0.1
FOXMQ_PORT=1883
PASS_THRESHOLD=65       # Min BFT consensus score to pass (0-100)
SWARM_SECRET=...        # Shared HMAC key (change in production)
```

---

## Local Setup (No Docker)

```bash
# Terminal 1 — FoxMQ 3-node Vertex BFT cluster
cd flashforge
.\foxmq.exe run --allow-anonymous-login -f foxmq.d/key_0.pem -L 0.0.0.0:1883 -C 0.0.0.0:19793 foxmq.d
.\foxmq.exe run --allow-anonymous-login -f foxmq.d/key_1.pem -L 0.0.0.0:1884 -C 0.0.0.0:19794 foxmq.d
.\foxmq.exe run --allow-anonymous-login -f foxmq.d/key_2.pem -L 0.0.0.0:1885 -C 0.0.0.0:19795 foxmq.d

# Terminal 2 — Install deps
pip install -r requirements.txt

# Terminals 3-8 — Start agents
python swarm/run_planner_node.py
python swarm/run_builder_node.py
CRITICS_EXPECTED=3 python swarm/run_critic_node.py
CRITICS_EXPECTED=3 NODE_ID=critic-002 python swarm/run_critic_node.py
CRITICS_EXPECTED=3 NODE_ID=critic-003 python swarm/run_critic_node.py
python swarm/run_fixer_node.py

# Terminal 9 — Dashboard
python swarm/dashboard_server.py

# Terminal 10 — Inject a job
python swarm/job_injector.py "Build a todo list app with dark theme"
```

---

## 📁 Project Structure

```
flashforge/
├── swarm/
│   ├── foxmq_node.py        # FoxMQ/Vertex transport + HMAC + nonce ring
│   ├── bid_protocol.py      # Leaderless auction engine
│   ├── critic_consensus.py  # BFT quorum (≥2/3 critics must agree)
│   ├── poc_logger.py        # HMAC-chained Proof of Coordination
│   ├── verify_poc.py        # Standalone verifier (no FoxMQ needed)
│   ├── hive_memory.py       # Decentralized shared state
│   ├── agent_economy.py     # Reputation + credit tracking
│   ├── warmup_demo.py       # Warm-Up: 5/5 criteria in ~75s
│   ├── dashboard_server.py  # FastAPI SSE + 7-tab dashboard
│   ├── job_injector.py      # Job entry point
│   ├── run_planner_node.py
│   ├── run_builder_node.py
│   ├── run_critic_node.py
│   └── run_fixer_node.py
├── agents/                  # LLM agent implementations
├── utils/llm_manager.py     # Multi-provider LLM router (Groq → Gemini → Qwen → Anthropic)
├── config.py
├── docker-compose.yml       # Full stack (3 FoxMQ brokers + 6 agents + dashboard)
└── .env.example

demo_resilience.py           # Node failure demo (kill any role live)
demo_security.py             # Security demo (replay, tamper, TTL)
FAILURE_MODES.md             # All 7 failure modes with reproduction steps
JUDGING.md                   # Judging criteria → code file mapping
```

---

## 🏆 Why FlashForge Wins Track 3

1. **True leaderless** — no coordinator, no SPOF; Vertex BFT inside FoxMQ orders everything
2. **Real resilience** — `demo_resilience.py` kills nodes live and proves recovery
3. **Cryptographic audit trail** — HMAC hash-chained PoC with multi-agent attestations
4. **Double replay protection** — HMAC + nonce ring + TTL (belt and suspenders)
5. **Production observability** — 7-tab live dashboard: topology, BFT votes, PoC, Hive Memory, Economy, Metrics
6. **Agent Economy** — emergent reputation market with deterministic scoring from MQTT events
7. **Multi-LLM cost optimization** — routes to cheapest capable model per task type
8. **One-command Docker** — full 3-broker cluster + 6 agents in `docker compose up`

---

## For Judges

- **Criteria → code mapping:** [JUDGING.md](flashforge/JUDGING.md)
- **Full submission description:** [SUBMISSION.md](flashforge/SUBMISSION.md)
- **Failure modes + reproduction:** [FAILURE_MODES.md](FAILURE_MODES.md)
- **Warm-Up proof output:** [warmup_proof.txt](flashforge/warmup_proof.txt)

---

*Built for Vertex Swarm Challenge 2026 · Track 3: Agent Economy · Solo — [@loquit-doru](https://github.com/loquit-doru)*
