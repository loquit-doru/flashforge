"""
Swarm Observability Dashboard — FastAPI + Server-Sent Events

Real-time view of the FlashForge multi-agent swarm.
Bridges FoxMQ MQTT messages → SSE → browser.

Features:
  - Live peer registry (online/stale status)
  - Multi-critic EVAL_VOTE table (shows BFT consensus in action)
  - Job pipeline tracker (TASK_AVAILABLE → BID → COMMIT → CONSENSUS → DONE)
  - Scrolling event stream (all MQTT messages)

Run:
    python swarm/dashboard_server.py

Open: http://localhost:5050

Environment variables:
  FOXMQ_HOST       default "127.0.0.1"
  FOXMQ_PORT       default 1883
  DASHBOARD_PORT   default 5050
"""
import asyncio
import json
import os
import sys
import time

import paho.mqtt.client as mqtt
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse
import uvicorn

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.hive_memory import HiveMemory, HIVE_TOPIC
from swarm.agent_economy import AgentEconomy

FOXMQ_HOST     = os.getenv("FOXMQ_HOST",   "127.0.0.1")
FOXMQ_PORT     = int(os.getenv("FOXMQ_PORT",   "1883"))
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "5050"))

# ── MQTT client (for publishing from API) ───────────────────────────────────────
_mqtt_client: mqtt.Client | None = None

# ── Event bus: paho thread → asyncio broadcast ─────────────────────────────────
_recent_events: list    = []          # last 200 events (for replay on connect)
_client_queues: set     = set()       # one asyncio.Queue per SSE client
_loop: asyncio.AbstractEventLoop | None = None

# ── Job state machine tracking ──────────────────────────────────────────────────
# stage order: announced → planning → building → evaluating → fixing → done
_job_states: dict = {}   # root_job_id → {stage, started_ms, updated_ms, node}

# ── Prometheus-style counters ───────────────────────────────────────────────────
_metrics = {
    "bids_total":       0,
    "commits_total":    0,
    "tasks_total":      0,
    "eval_votes_total": 0,
    "consensus_total":  0,
    "jobs_done_total":  0,
    "peers_online":     0,
}
_peers: dict = {}   # node_id → {role, last_seen_ms}

# ── Hive Memory + Agent Economy ─────────────────────────────────────────────────
_hive = HiveMemory()
_economy = AgentEconomy()

# ── Coordination latency tracking ──────────────────────────────────────────────
_coordination_metrics = {
    "bid_latencies_ms": [],     # time from TASK_AVAILABLE to COMMIT
    "eval_latencies_ms": [],    # time from EVAL start to EVAL_CONSENSUS
    "total_jobs_completed": 0,
    "total_pipeline_time_ms": [],
    "avg_bid_latency_ms": 0,
    "avg_eval_latency_ms": 0,
    "avg_pipeline_time_ms": 0,
    "messages_per_second": 0,
    "uptime_start_ms": int(time.time() * 1000),
}
_task_timestamps: dict = {}   # job_id → {announced_ms, committed_ms, eval_start_ms, consensus_ms, done_ms}


def _update_job_state(msg_type: str, payload: dict) -> None:
    """Infer job stage from MQTT message and update _job_states."""
    job_id_raw: str = payload.get("job_id", "")
    if not job_id_raw:
        return
    root_id = job_id_raw.split(":")[0]
    now_ms  = int(time.time() * 1000)

    cap = payload.get("capability", "")

    if msg_type == "TASK_AVAILABLE":
        _metrics["tasks_total"] += 1
        stage_map = {
            "planning":   "announced",
            "building":   "building",
            "evaluation": "evaluating",
            "fixing":     "fixing",
        }
        stage = stage_map.get(cap)
        if stage and root_id not in _job_states:
            _job_states[root_id] = {"stage": stage, "started_ms": now_ms, "updated_ms": now_ms, "node": None}
        elif stage and root_id in _job_states:
            _job_states[root_id].update({"stage": stage, "updated_ms": now_ms})

    elif msg_type == "COMMIT":
        _metrics["commits_total"] += 1
        cap_to_stage = {
            "planning":   "planning",
            "building":   "building",
            "evaluation": "evaluating",
            "fixing":     "fixing",
        }
        stage = cap_to_stage.get(payload.get("capability", ""))
        if stage and root_id in _job_states:
            _job_states[root_id].update({
                "stage": stage,
                "updated_ms": now_ms,
                "node": payload.get("winner_id", "")[:8],
            })

    elif msg_type == "EVAL_CONSENSUS":
        _metrics["consensus_total"] += 1
        verdict = payload.get("verdict", "")
        if root_id in _job_states:
            if verdict == "PASS":
                _job_states[root_id].update({"stage": "done", "updated_ms": now_ms})
                _metrics["jobs_done_total"] += 1
            else:
                _job_states[root_id].update({"stage": "fixing", "updated_ms": now_ms})

    elif msg_type == "BID":
        _metrics["bids_total"] += 1

    elif msg_type == "EVAL_VOTE":
        _metrics["eval_votes_total"] += 1

    # ── Coordination latency tracking ──
    if msg_type == "TASK_AVAILABLE" and root_id not in _task_timestamps:
        _task_timestamps[root_id] = {"announced_ms": now_ms}
    elif msg_type == "COMMIT" and root_id in _task_timestamps:
        _task_timestamps[root_id]["committed_ms"] = now_ms
        announced = _task_timestamps[root_id].get("announced_ms", now_ms)
        lat = now_ms - announced
        _coordination_metrics["bid_latencies_ms"].append(lat)
        _coordination_metrics["bid_latencies_ms"] = _coordination_metrics["bid_latencies_ms"][-50:]
        _coordination_metrics["avg_bid_latency_ms"] = sum(_coordination_metrics["bid_latencies_ms"]) / len(_coordination_metrics["bid_latencies_ms"])
    elif msg_type == "EVAL_CONSENSUS" and root_id in _task_timestamps:
        _task_timestamps[root_id]["consensus_ms"] = now_ms
        announced = _task_timestamps[root_id].get("announced_ms", now_ms)
        total = now_ms - announced
        _coordination_metrics["total_pipeline_time_ms"].append(total)
        _coordination_metrics["total_pipeline_time_ms"] = _coordination_metrics["total_pipeline_time_ms"][-50:]
        _coordination_metrics["avg_pipeline_time_ms"] = sum(_coordination_metrics["total_pipeline_time_ms"]) / len(_coordination_metrics["total_pipeline_time_ms"])


def _paho_on_message(client, userdata, msg) -> None:
    try:
        data = json.loads(msg.payload)
    except Exception:
        return
    _recent_events.append(data)
    if len(_recent_events) > 200:
        _recent_events.pop(0)

    # Update job state machine + metrics
    msg_type = data.get("type", "")
    payload  = data.get("payload", {})
    _update_job_state(msg_type, payload)

    # Update peer registry for metrics
    sender_id   = data.get("sender_id", "")
    sender_role = data.get("sender_role", "")
    if sender_id and msg_type in ("PEER_ANNOUNCE", "HEARTBEAT"):
        _peers[sender_id] = {"role": sender_role, "last_seen_ms": int(time.time() * 1000)}
    _metrics["peers_online"] = sum(
        1 for p in _peers.values()
        if (int(time.time() * 1000) - p["last_seen_ms"]) / 1000 < 12
    )

    # Feed Hive Memory
    if msg_type == HIVE_TOPIC:
        _hive.put_from_payload(payload)

    # Feed Agent Economy (deterministic — same events → same state)
    _economy.process_swarm_event(msg_type, sender_id, sender_role, payload)

    if _loop:
        asyncio.run_coroutine_threadsafe(_broadcast(data), _loop)


async def _broadcast(msg: dict) -> None:
    for q in list(_client_queues):
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass


def _mqtt_on_connect(client, userdata, connect_flags, reason_code, properties):
    client.subscribe("swarm/#", qos=1)
    print(f"[dashboard] OK MQTT connected + subscribed (rc={reason_code})")


def _start_mqtt() -> None:
    global _mqtt_client
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="dashboard-observer",
        protocol=mqtt.MQTTv5,
    )
    client.on_connect = _mqtt_on_connect
    client.on_message = _paho_on_message
    try:
        client.connect(FOXMQ_HOST, FOXMQ_PORT, keepalive=60)
        client.loop_start()
        _mqtt_client = client
        print(f"[dashboard] OK MQTT -> FoxMQ {FOXMQ_HOST}:{FOXMQ_PORT}")
    except Exception as e:
        print(f"[dashboard] WARN Cannot connect to FoxMQ: {e} -- dashboard will show live events once broker starts")


# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(title="FlashForge Swarm Dashboard")


@app.on_event("startup")
async def startup() -> None:
    global _loop
    _loop = asyncio.get_event_loop()
    _start_mqtt()


# ── SSE endpoint ───────────────────────────────────────────────────────────────
@app.get("/events")
async def sse(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=300)
    _client_queues.add(q)

    async def gen():
        try:
            # Replay up to 50 recent events on fresh connect
            for evt in _recent_events[-50:]:
                yield f"data: {json.dumps(evt)}\n\n"
            # Stream new events
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps(msg)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"  # prevent browser SSE timeout
        finally:
            _client_queues.discard(q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/events")
async def api_events():
    return {"events": _recent_events[-100:], "total": len(_recent_events)}


@app.get("/api/jobs")
async def api_jobs():
    """Job state machine — shows each job's current stage and timing."""
    now_ms = int(time.time() * 1000)
    jobs = []
    for job_id, state in _job_states.items():
        age_s = (now_ms - state["started_ms"]) / 1000
        jobs.append({
            "job_id":     job_id,
            "stage":      state["stage"],
            "age_s":      round(age_s, 1),
            "updated_ms": state["updated_ms"],
            "node":       state.get("node"),
        })
    jobs.sort(key=lambda j: j["updated_ms"], reverse=True)
    return {"jobs": jobs, "total": len(jobs)}


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics():
    """Prometheus-compatible text exposition format."""
    now_ms = int(time.time() * 1000)
    peers_online = sum(
        1 for p in _peers.values()
        if (now_ms - p["last_seen_ms"]) / 1000 < 12
    )
    lines = [
        "# HELP flashforge_bids_total Total bid messages received",
        "# TYPE flashforge_bids_total counter",
        f"flashforge_bids_total {_metrics['bids_total']}",
        "# HELP flashforge_commits_total Total commit messages received",
        "# TYPE flashforge_commits_total counter",
        f"flashforge_commits_total {_metrics['commits_total']}",
        "# HELP flashforge_tasks_total Total TASK_AVAILABLE messages",
        "# TYPE flashforge_tasks_total counter",
        f"flashforge_tasks_total {_metrics['tasks_total']}",
        "# HELP flashforge_eval_votes_total Total evaluation votes cast",
        "# TYPE flashforge_eval_votes_total counter",
        f"flashforge_eval_votes_total {_metrics['eval_votes_total']}",
        "# HELP flashforge_consensus_total Total BFT consensus decisions",
        "# TYPE flashforge_consensus_total counter",
        f"flashforge_consensus_total {_metrics['consensus_total']}",
        "# HELP flashforge_jobs_done_total Total jobs completed (PASS verdict)",
        "# TYPE flashforge_jobs_done_total counter",
        f"flashforge_jobs_done_total {_metrics['jobs_done_total']}",
        "# HELP flashforge_peers_online Current online peers",
        "# TYPE flashforge_peers_online gauge",
        f"flashforge_peers_online {peers_online}",
        "# HELP flashforge_jobs_active Currently tracked jobs",
        "# TYPE flashforge_jobs_active gauge",
        f"flashforge_jobs_active {len(_job_states)}",
    ]
    return "\n".join(lines) + "\n"


@app.get("/api/poc")
async def api_poc():
    """Return list of PoC log files with their contents for the PoC Viewer tab."""
    poc_dir = os.path.join(os.getcwd(), "poc_logs")
    if not os.path.isdir(poc_dir):
        return {"logs": []}
    logs = []
    for fname in sorted(os.listdir(poc_dir), reverse=True)[:20]:
        if not fname.endswith(".jsonl"):
            continue
        fpath = os.path.join(poc_dir, fname)
        events = []
        try:
            with open(fpath, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))
        except Exception:
            continue
        job_id = fname.replace("poc_", "").replace(".jsonl", "")
        valid = all(e.get("valid", True) for e in events)
        logs.append({"job_id": job_id, "events": events, "valid": valid, "count": len(events)})
    return {"logs": logs}


@app.get("/api/hive")
async def api_hive():
    """Hive Memory — shared agent world view."""
    return _hive.snapshot()


@app.get("/api/economy")
async def api_economy():
    """Agent Economy — reputation leaderboard and credits."""
    return _economy.snapshot()


@app.get("/api/coordination")
async def api_coordination():
    """Coordination metrics — latencies, throughput, uptime."""
    now_ms = int(time.time() * 1000)
    uptime_s = (now_ms - _coordination_metrics["uptime_start_ms"]) / 1000
    total_msgs = len(_recent_events)
    mps = total_msgs / max(uptime_s, 1)
    return {
        "avg_bid_latency_ms": round(_coordination_metrics["avg_bid_latency_ms"], 1),
        "avg_eval_latency_ms": round(_coordination_metrics["avg_eval_latency_ms"], 1),
        "avg_pipeline_time_ms": round(_coordination_metrics["avg_pipeline_time_ms"], 1),
        "messages_per_second": round(mps, 2),
        "uptime_s": round(uptime_s, 0),
        "total_messages": total_msgs,
        "total_jobs": len(_job_states),
        "total_peers_seen": len(_peers),
        "bid_latencies": _coordination_metrics["bid_latencies_ms"][-20:],
        "pipeline_times": _coordination_metrics["total_pipeline_time_ms"][-20:],
    }


import uuid as _uuid
from pydantic import BaseModel as _BaseModel

class _InjectBody(_BaseModel):
    prompt: str

@app.post("/api/inject")
async def inject_job(body: _InjectBody):
    """Inject a job into the swarm directly from the dashboard."""
    import hashlib, hmac
    SWARM_SECRET = os.getenv("SWARM_SECRET", "swarm-secret-change-in-prod")
    if not _mqtt_client:
        return {"ok": False, "error": "MQTT not connected"}
    job_id = str(_uuid.uuid4())
    ts = int(time.time() * 1000)
    nonce = str(_uuid.uuid4())
    payload = {
        "job_id": job_id,
        "capability": "planning",
        "prompt": body.prompt,
        "context": {},
        "announced_at_ms": ts,
    }
    msg = {
        "type": "TASK_AVAILABLE",
        "sender_id": "dashboard",
        "sender_role": "injector",
        "timestamp_ms": ts,
        "nonce": nonce,
        "payload": payload,
    }
    raw = json.dumps(msg, sort_keys=True, separators=(",", ":"))
    sig = hmac.new(SWARM_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()
    msg["hmac"] = sig
    _mqtt_client.publish("swarm/TASK_AVAILABLE", json.dumps(msg), qos=1)
    print(f"[dashboard] Injected job {job_id[:8]} — {body.prompt[:60]}")
    return {"ok": True, "job_id": job_id}

class _KillBody(_BaseModel):
    target_id: str

@app.post("/api/kill-peer")
async def kill_peer(body: _KillBody):
    """Send KILL_SIGNAL to a specific peer — resilience demo."""
    import hashlib, hmac as _hmac
    SWARM_SECRET = os.getenv("SWARM_SECRET", "swarm-secret-change-in-prod")
    if not _mqtt_client:
        return {"ok": False, "error": "MQTT not connected"}
    ts = int(time.time() * 1000)
    nonce = str(_uuid.uuid4())
    msg = {
        "type": "KILL_SIGNAL",
        "sender_id": "dashboard",
        "sender_role": "admin",
        "timestamp_ms": ts,
        "nonce": nonce,
        "payload": {"target_id": body.target_id},
    }
    raw = json.dumps(msg, sort_keys=True, separators=(",", ":"))
    sig = _hmac.new(SWARM_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()
    msg["hmac"] = sig
    _mqtt_client.publish("swarm/KILL_SIGNAL", json.dumps(msg), qos=1)
    print(f"[dashboard] 💀 KILL_SIGNAL → {body.target_id}")
    return {"ok": True, "killed": body.target_id}

@app.get("/api/result/{job_id}")
async def api_result(job_id: str):
    """Return the generated HTML artifact for a completed job."""
    import glob
    # Builder/fixer output dirs (try both CWD-relative and flashforge-relative)
    candidates = [
        os.path.join(os.getcwd(), "swarm_output", job_id),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "swarm_output", job_id),
    ]
    for out_dir in candidates:
        if os.path.isdir(out_dir):
            # Prefer fixed version
            fixed = os.path.join(out_dir, "index_fixed.html")
            original = os.path.join(out_dir, "index.html")
            html_path = fixed if os.path.isfile(fixed) else original
            if os.path.isfile(html_path):
                with open(html_path, encoding="utf-8") as f:
                    content = f.read()
                return {"ok": True, "job_id": job_id, "html": content, "source": os.path.basename(html_path), "size": len(content)}
    return {"ok": False, "error": "No output found for this job"}


@app.get("/api/results")
async def api_results():
    """List all job results available on disk."""
    results = []
    for base in [
        os.path.join(os.getcwd(), "swarm_output"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "swarm_output"),
    ]:
        if not os.path.isdir(base):
            continue
        for name in sorted(os.listdir(base), reverse=True):
            d = os.path.join(base, name)
            if not os.path.isdir(d):
                continue
            fixed = os.path.isfile(os.path.join(d, "index_fixed.html"))
            original = os.path.isfile(os.path.join(d, "index.html"))
            if fixed or original:
                results.append({"job_id": name, "has_fix": fixed, "source": "index_fixed.html" if fixed else "index.html"})
    return {"results": results}


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(DASHBOARD_HTML)


# ── Embedded dashboard HTML ────────────────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FlashForge — Swarm Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#080b10;--bg2:#0d1117;--bg3:#111827;
  --border:#1e2a3a;--border2:#243347;
  --blue:#3b82f6;--blue2:#60a5fa;--blue3:#93c5fd;
  --green:#10b981;--green2:#34d399;
  --red:#ef4444;--red2:#f87171;
  --yellow:#f59e0b;--yellow2:#fbbf24;
  --purple:#8b5cf6;--purple2:#a78bfa;
  --text:#e2e8f0;--text2:#94a3b8;--text3:#475569;
  --glow-blue:0 0 20px rgba(59,130,246,.3);
  --glow-green:0 0 20px rgba(16,185,129,.3);
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;height:100vh;overflow:hidden}

/* ── Animated background grid ── */
body::before{content:'';position:fixed;inset:0;background-image:linear-gradient(rgba(59,130,246,.03) 1px,transparent 1px),linear-gradient(90deg,rgba(59,130,246,.03) 1px,transparent 1px);background-size:40px 40px;pointer-events:none;z-index:0}

.wrap{position:relative;z-index:1;padding:10px 16px;max-width:1400px;margin:0 auto;height:100vh;display:flex;flex-direction:column;overflow:hidden}

/* ── Header ── */
.hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;padding-bottom:6px;border-bottom:1px solid var(--border)}
.hdr-left h1{font-size:18px;font-weight:700;background:linear-gradient(135deg,var(--blue2),var(--purple2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;letter-spacing:-.5px}
.hdr-left .sub{font-size:10px;color:var(--text2);margin-top:2px;font-family:'JetBrains Mono',monospace}
.conn-badge{display:flex;align-items:center;gap:8px;background:var(--bg3);border:1px solid var(--border);border-radius:20px;padding:6px 14px;font-size:12px;font-weight:500}
.conn-dot{width:8px;height:8px;border-radius:50%;background:var(--red);transition:background .3s}
.conn-dot.live{background:var(--green);box-shadow:0 0 8px var(--green)}

/* ── Job Injector bar ── */
.inject-bar{display:flex;align-items:center;gap:8px;background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:6px 12px;margin-bottom:8px;transition:border-color .3s}
.inject-bar:focus-within{border-color:var(--blue)}
.inject-icon{font-size:16px;flex-shrink:0}
.inject-input{flex:1;background:transparent;border:none;outline:none;color:var(--text);font-size:14px;font-family:'Inter',sans-serif}
.inject-input::placeholder{color:var(--text3)}
.inject-btn{background:linear-gradient(135deg,var(--blue),var(--purple));border:none;color:#fff;font-size:12px;font-weight:600;padding:5px 14px;border-radius:6px;cursor:pointer;white-space:nowrap;transition:opacity .2s,transform .1s}
.inject-btn:hover{opacity:.9}
.inject-btn:active{transform:scale(.96)}
.inject-btn:disabled{opacity:.4;cursor:not-allowed}
.inject-btn.ok{background:var(--green)}
.inject-btn.err{background:var(--red)}

/* ── Stat cards ── */
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:8px}
.stat{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:8px 12px;position:relative;overflow:hidden;transition:border-color .3s}
.stat::before{content:'';position:absolute;inset:0;opacity:0;transition:opacity .3s}
.stat.flash::before{opacity:1;animation:flash-card .6s ease-out forwards}
@keyframes flash-card{0%{background:rgba(59,130,246,.15)}100%{background:transparent}}
.stat-icon{font-size:14px;margin-bottom:2px}
.stat-n{font-size:22px;font-weight:700;color:var(--blue2);line-height:1;font-variant-numeric:tabular-nums;transition:transform .2s}
.stat-n.bump{animation:bump .3s ease-out}
@keyframes bump{0%{transform:scale(1.3)}100%{transform:scale(1)}}
.stat-l{font-size:9px;color:var(--text2);margin-top:2px;text-transform:uppercase;letter-spacing:.6px;font-weight:500}
.stat:nth-child(1) .stat-n{color:var(--green2)}
.stat:nth-child(3) .stat-n{color:var(--yellow2)}
.stat:nth-child(4) .stat-n{color:var(--purple2)}

/* ── Grid layout ── */
.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:8px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px}
.span2{grid-column:span 2}

/* ── Cards ── */
.card{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:10px;position:relative}
.card-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
.card-title{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1.2px;color:var(--text2)}
.card-badge{font-size:10px;font-family:'JetBrains Mono',monospace;color:var(--text3);background:var(--bg3);padding:2px 8px;border-radius:10px;border:1px solid var(--border)}

/* ── Network graph ── */
#net-canvas{width:100%;height:160px;display:block}

/* ── Agent nodes (peer list) ── */
.agent-grid{display:flex;flex-direction:column;gap:8px}
.agent{display:flex;align-items:center;gap:10px;background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:8px 12px;transition:border-color .3s,box-shadow .3s}
.agent.online{border-color:rgba(16,185,129,.3)}
.agent.online:hover{border-color:var(--green);box-shadow:var(--glow-green)}
.agent.stale{border-color:rgba(239,68,68,.2);opacity:.6}
.agent-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;transition:background .3s}
.online .agent-dot{background:var(--green);box-shadow:0 0 6px var(--green);animation:pulse-dot 2s ease-in-out infinite}
.stale  .agent-dot{background:var(--red)}
@keyframes pulse-dot{0%,100%{box-shadow:0 0 4px var(--green)}50%{box-shadow:0 0 10px var(--green),0 0 20px rgba(16,185,129,.4)}}
.agent-role{font-size:12px;font-weight:600;color:var(--text);flex:1}
.agent-id{font-size:10px;font-family:'JetBrains Mono',monospace;color:var(--text3)}
.agent-time{font-size:10px;color:var(--text3)}
.role-icon{font-size:14px;width:24px;text-align:center}
.kill-btn{background:rgba(239,68,68,0.15);border:1px solid rgba(239,68,68,0.3);color:var(--red2);font-size:11px;padding:2px 6px;border-radius:4px;cursor:pointer;margin-left:auto;transition:all .2s}
.kill-btn:hover{background:rgba(239,68,68,0.35);border-color:var(--red)}

/* ── Job pipeline ── */
.job-list{display:flex;flex-direction:column;gap:4px;max-height:160px;overflow-y:auto}
.job-item{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:10px 12px}
.job-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
.job-id{font-size:10px;font-family:'JetBrains Mono',monospace;color:var(--text2)}
.job-age{font-size:10px;color:var(--text3)}
.pipeline{display:flex;align-items:center;gap:0}
.stage{flex:1;text-align:center;font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;padding:3px 2px;color:var(--text3);position:relative;transition:color .3s}
.stage.active{color:var(--blue2)}
.stage.done{color:var(--green2)}
.stage-bar{height:3px;background:var(--border);border-radius:2px;margin-top:3px;overflow:hidden}
.stage-fill{height:100%;width:0;background:var(--blue);border-radius:2px;transition:width .5s ease,background .3s}
.stage.done .stage-fill{width:100%;background:var(--green)}
.stage.active .stage-fill{width:60%;background:var(--blue);animation:pulse-bar 1.5s ease-in-out infinite}
@keyframes pulse-bar{0%,100%{opacity:.7}50%{opacity:1}}
.stage-sep{width:8px;height:2px;background:var(--border);flex-shrink:0;margin-bottom:10px}

/* ── BFT votes table ── */
.vtable{width:100%;border-collapse:collapse}
.vtable th{color:var(--text3);font-weight:500;text-align:left;padding:6px 10px;border-bottom:1px solid var(--border);font-size:10px;text-transform:uppercase;letter-spacing:.8px}
.vtable td{padding:6px 10px;border-bottom:1px solid rgba(30,42,58,.5);font-size:12px;font-family:'JetBrains Mono',monospace}
.vtable tr:last-child td{border-bottom:none}
.vtable tr:hover td{background:var(--bg3)}
.badge{display:inline-flex;align-items:center;gap:4px;border-radius:5px;padding:2px 8px;font-size:10px;font-weight:600;letter-spacing:.3px}
.badge.pass{background:rgba(16,185,129,.15);color:var(--green2);border:1px solid rgba(16,185,129,.3)}
.badge.fail{background:rgba(239,68,68,.15);color:var(--red2);border:1px solid rgba(239,68,68,.3)}
.badge.consensus{background:rgba(139,92,246,.15);color:var(--purple2);border:1px solid rgba(139,92,246,.3)}
.score-bar{display:flex;align-items:center;gap:6px}
.score-track{width:50px;height:4px;background:var(--border);border-radius:2px;overflow:hidden}
.score-fill{height:100%;border-radius:2px;transition:width .4s}
.vtb-wrap{max-height:160px;overflow-y:auto}

/* ── Event stream ── */
#stream-box{height:150px;overflow-y:auto;font-family:'JetBrains Mono',monospace}
.evt{display:flex;align-items:flex-start;gap:10px;padding:4px 6px;border-radius:4px;transition:background .15s}
.evt:hover{background:var(--bg3)}
.evt-t{color:var(--text3);min-width:64px;font-size:10px;padding-top:1px}
.evt-ty{min-width:180px;font-size:10px;font-weight:500;padding-top:1px}
.evt-body{color:var(--text3);font-size:10px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;flex:1}
.evt-ty.t-EVAL_VOTE{color:var(--yellow2)}
.evt-ty.t-EVAL_CONSENSUS{color:var(--purple2)}
.evt-ty.t-COMMIT{color:var(--green2)}
.evt-ty.t-BID{color:var(--blue2)}
.evt-ty.t-PEER_ANNOUNCE{color:var(--blue3)}
.evt-ty.t-HEARTBEAT{color:var(--text3)}
.evt-ty.t-TASK_AVAILABLE{color:var(--red2)}
.evt-ty.t-COORDINATION_COMPLETE{color:var(--green2)}
.evt.new-evt{animation:slide-in .3s ease-out}
@keyframes slide-in{from{opacity:0;transform:translateX(-8px)}to{opacity:1;transform:translateX(0)}}

/* ── Bid activity ── */
.bid-list{display:flex;flex-direction:column;gap:4px;max-height:150px;overflow-y:auto}
.bid-item{display:flex;align-items:center;gap:8px;background:var(--bg3);border-radius:6px;padding:6px 10px;animation:slide-in .3s ease-out}
.bid-role{font-size:11px;font-weight:600;color:var(--text);min-width:70px}
.bid-score{font-size:10px;font-family:'JetBrains Mono',monospace;color:var(--text2)}
.bid-bar-wrap{flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden}
.bid-bar{height:100%;border-radius:2px;background:linear-gradient(90deg,var(--blue),var(--purple))}
.bid-winner{font-size:10px;color:var(--green2);font-weight:600}

/* ── Scrollbar ── */
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}

/* ── Responsive ── */
@media(max-width:900px){.grid3{grid-template-columns:1fr 1fr}.stats{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.grid3,.grid2{grid-template-columns:1fr}.stats{grid-template-columns:repeat(2,1fr)}}

/* ── Tabs ── */
.tabs{display:flex;gap:2px;margin-bottom:8px;border-bottom:1px solid var(--border);padding-bottom:0;flex-shrink:0}
.tab-btn{background:none;border:none;color:var(--text2);font-family:'Inter',sans-serif;font-size:11px;font-weight:500;padding:5px 12px;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:color .2s,border-color .2s}
.tab-btn:hover{color:var(--text)}
.tab-btn.active{color:var(--blue2);border-bottom-color:var(--blue2)}
.tab-panel{display:none;overflow-y:auto;flex:1;min-height:0}.tab-panel.active{display:block}

/* ── PoC Viewer ── */
.poc-list{display:flex;flex-direction:column;gap:14px}
.poc-card{background:var(--bg2);border:1px solid var(--border);border-radius:12px;overflow:hidden}
.poc-card.valid{border-color:rgba(16,185,129,.3)}
.poc-card.invalid{border-color:rgba(239,68,68,.3)}
.poc-hdr{display:flex;align-items:center;gap:10px;padding:12px 16px;background:var(--bg3);cursor:pointer;user-select:none}
.poc-hdr:hover{background:rgba(255,255,255,.03)}
.poc-job-id{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--text2);flex:1}
.poc-count{font-size:11px;color:var(--text3)}
.poc-valid-badge{font-size:10px;font-weight:600;padding:2px 10px;border-radius:10px}
.poc-valid-badge.ok{background:rgba(16,185,129,.15);color:var(--green2);border:1px solid rgba(16,185,129,.3)}
.poc-valid-badge.fail{background:rgba(239,68,68,.15);color:var(--red2);border:1px solid rgba(239,68,68,.3)}
.poc-timeline{padding:16px;display:none}
.poc-timeline.open{display:block}
.poc-evt{display:flex;align-items:flex-start;gap:12px;padding:8px 0;border-bottom:1px solid rgba(30,42,58,.6);position:relative}
.poc-evt:last-child{border-bottom:none}
.poc-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0;margin-top:3px;background:var(--blue)}
.poc-dot.valid{background:var(--green)}
.poc-dot.invalid{background:var(--red)}
.poc-seq{font-size:10px;font-family:'JetBrains Mono',monospace;color:var(--text3);min-width:24px}
.poc-etype{font-size:11px;font-weight:600;color:var(--blue2);min-width:180px}
.poc-actor{font-size:10px;font-family:'JetBrains Mono',monospace;color:var(--text2);min-width:140px}
.poc-hash{font-size:9px;font-family:'JetBrains Mono',monospace;color:var(--text3);word-break:break-all}
.poc-ts{font-size:10px;color:var(--text3);margin-left:auto;white-space:nowrap}
.poc-chain-line{position:absolute;left:16px;top:18px;width:2px;height:calc(100% - 10px);background:var(--border)}
.poc-attest{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;padding-top:10px;border-top:1px solid var(--border)}
.poc-attest-badge{font-size:10px;font-family:'JetBrains Mono',monospace;padding:3px 10px;border-radius:6px;background:rgba(59,130,246,.1);color:var(--blue2);border:1px solid rgba(59,130,246,.2)}
.poc-empty{color:var(--text3);font-size:13px;text-align:center;padding:40px}

/* ── Hive Memory entries ── */
.hive-entry{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:10px 14px;margin-bottom:8px;animation:slide-in .3s ease-out}
.hive-ns{display:inline-flex;align-items:center;padding:2px 10px;border-radius:10px;font-size:10px;font-weight:600;letter-spacing:.5px;margin-right:8px}
.hive-ns.plan{background:rgba(59,130,246,.15);color:var(--blue2);border:1px solid rgba(59,130,246,.25)}
.hive-ns.build{background:rgba(16,185,129,.15);color:var(--green2);border:1px solid rgba(16,185,129,.25)}
.hive-ns.eval{background:rgba(245,158,11,.15);color:var(--yellow2);border:1px solid rgba(245,158,11,.25)}
.hive-ns.fix{background:rgba(239,68,68,.15);color:var(--red2);border:1px solid rgba(239,68,68,.25)}
.hive-ns.meta{background:rgba(139,92,246,.15);color:var(--purple2);border:1px solid rgba(139,92,246,.25)}
.hive-key{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--text2)}
.hive-val{font-size:11px;font-family:'JetBrains Mono',monospace;color:var(--text3);margin-top:4px;max-height:80px;overflow:hidden;word-break:break-all}
.hive-author{font-size:10px;color:var(--text3);margin-top:4px}
.ns-bar{display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--bg3);border-radius:8px;margin-bottom:6px}
.ns-bar-label{font-size:11px;font-weight:600;color:var(--text);min-width:60px}
.ns-bar-track{flex:1;height:8px;background:var(--border);border-radius:4px;overflow:hidden}
.ns-bar-fill{height:100%;border-radius:4px;transition:width .5s}
.ns-bar-count{font-size:11px;font-family:'JetBrains Mono',monospace;color:var(--text2);min-width:30px;text-align:right}

/* ── Economy ── */
.eco-agent{display:flex;align-items:center;gap:10px;padding:10px 14px;background:var(--bg3);border:1px solid var(--border);border-radius:8px;margin-bottom:8px}
.eco-rank{font-size:18px;font-weight:700;color:var(--text3);min-width:30px;text-align:center}
.eco-rank.r1{color:var(--yellow2)}
.eco-rank.r2{color:var(--text2)}
.eco-rank.r3{color:#cd7f32}
.eco-info{flex:1}
.eco-name{font-size:12px;font-weight:600;color:var(--text)}
.eco-detail{font-size:10px;color:var(--text3);margin-top:2px}
.eco-rep-bar{width:100px;height:6px;background:var(--border);border-radius:3px;overflow:hidden}
.eco-rep-fill{height:100%;border-radius:3px}
.eco-credits{font-size:14px;font-weight:600;color:var(--green2);min-width:50px;text-align:right}
.tier-badge{display:inline-flex;padding:2px 8px;border-radius:8px;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.5px}
.tier-badge.elite{background:rgba(245,158,11,.15);color:var(--yellow2);border:1px solid rgba(245,158,11,.3)}
.tier-badge.veteran{background:rgba(139,92,246,.15);color:var(--purple2);border:1px solid rgba(139,92,246,.3)}
.tier-badge.standard{background:rgba(59,130,246,.15);color:var(--blue2);border:1px solid rgba(59,130,246,.3)}
.tier-badge.novice{background:rgba(100,116,139,.15);color:var(--text2);border:1px solid rgba(100,116,139,.3)}
.eco-event{display:flex;align-items:center;gap:8px;padding:6px 10px;border-bottom:1px solid rgba(30,42,58,.5);font-size:11px}
.eco-event:last-child{border-bottom:none}
.eco-evt-type{min-width:120px;font-weight:600;font-size:10px}
.eco-evt-type.positive{color:var(--green2)}
.eco-evt-type.negative{color:var(--red2)}
.eco-evt-agent{font-family:'JetBrains Mono',monospace;color:var(--text2);min-width:80px;font-size:10px}
.eco-evt-delta{font-family:'JetBrains Mono',monospace;font-size:10px;min-width:50px;text-align:right}

/* ── Result preview ── */
.result-list{display:flex;flex-direction:column;gap:8px;margin-bottom:12px}
.result-item{display:flex;align-items:center;gap:10px;background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:8px 12px;cursor:pointer;transition:border-color .2s}
.result-item:hover{border-color:var(--blue)}
.result-item.active{border-color:var(--blue);box-shadow:var(--glow-blue)}
.result-job{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--text2);flex:1}
.result-src{font-size:10px;color:var(--text3)}
.result-badge{font-size:10px;padding:2px 8px;border-radius:6px;background:rgba(16,185,129,.15);color:var(--green2);border:1px solid rgba(16,185,129,.3)}
.result-frame{width:100%;border:1px solid var(--border);border-radius:8px;background:#fff;min-height:300px}
.result-code{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--text2);max-height:300px;overflow:auto;white-space:pre-wrap;word-break:break-all}
.result-actions{display:flex;gap:8px;margin-bottom:8px}
.result-actions button{background:var(--bg3);border:1px solid var(--border);color:var(--text2);padding:4px 12px;border-radius:6px;cursor:pointer;font-size:11px;transition:border-color .2s}
.result-actions button:hover{border-color:var(--blue);color:var(--text)}
.result-actions button.active{border-color:var(--blue);color:var(--blue2)}

/* ── Metric detail ── */
.metric-row{display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:var(--bg3);border-radius:8px;margin-bottom:6px}
.metric-label{font-size:12px;color:var(--text2)}
.metric-value{font-size:14px;font-weight:600;color:var(--blue2);font-family:'JetBrains Mono',monospace}
</style>
</head>
<body>
<div class="wrap">

<!-- Header -->
<div class="hdr">
  <div class="hdr-left">
    <h1>⚡ FlashForge Swarm</h1>
    <div class="sub">Vertex Swarm Challenge 2026 &nbsp;·&nbsp; Track 3: Agent Economy &nbsp;·&nbsp; Multi-Critic BFT Consensus</div>
  </div>
  <div class="conn-badge">
    <span class="conn-dot" id="conn-dot"></span>
    <span id="conn-label">connecting…</span>
  </div>
</div>

<!-- Job Injector -->
<div class="inject-bar">
  <div class="inject-icon">🚀</div>
  <input type="text" id="inject-input" class="inject-input" placeholder="Describe what to build… e.g. Build a weather dashboard with city search" />
  <button id="inject-btn" class="inject-btn" onclick="injectJob()">Launch Job</button>
</div>

<!-- Tab navigation -->
<div class="tabs">
  <button class="tab-btn active" onclick="switchTab('swarm',this)">⚡ Live</button>
  <button class="tab-btn" onclick="switchTab('poc',this)">🔐 PoC</button>
  <button class="tab-btn" onclick="switchTab('hive',this)">🧠 Hive</button>
  <button class="tab-btn" onclick="switchTab('economy',this)">💰 Economy</button>
  <button class="tab-btn" onclick="switchTab('metrics',this)">📊 Metrics</button>
  <button class="tab-btn" onclick="switchTab('result',this)">🎨 Result</button>
</div>

<div id="tab-swarm" class="tab-panel active">

<!-- Stats -->
<div class="stats">
  <div class="stat" id="stat0"><div class="stat-icon">🟢</div><div class="stat-n" id="s0">0</div><div class="stat-l">Peers Online</div></div>
  <div class="stat" id="stat1"><div class="stat-icon">⚡</div><div class="stat-n" id="s1">0</div><div class="stat-l">Jobs Tracked</div></div>
  <div class="stat" id="stat2"><div class="stat-icon">🗳</div><div class="stat-n" id="s2">0</div><div class="stat-l">Votes Cast</div></div>
  <div class="stat" id="stat3"><div class="stat-icon">⚖</div><div class="stat-n" id="s3">0</div><div class="stat-l">Consensus</div></div>
</div>

<!-- Row 1: Network graph + Agents + Bids -->
<div class="grid3" style="margin-bottom:14px">
  <div class="card span2">
    <div class="card-hdr">
      <span class="card-title">📡 Live Network Topology</span>
      <span class="card-badge" id="topo-label">0 nodes</span>
    </div>
    <canvas id="net-canvas"></canvas>
  </div>
  <div class="card">
    <div class="card-hdr">
      <span class="card-title">🤖 Swarm Agents</span>
      <span class="card-badge" id="peer-count">0 active</span>
    </div>
    <div class="agent-grid" id="agent-list"><div style="color:var(--text3);font-size:12px;padding:8px">Waiting for peers…</div></div>
  </div>
</div>

<!-- Row 2: Job pipeline + BFT votes -->
<div class="grid2" style="margin-bottom:14px">
  <div class="card">
    <div class="card-hdr">
      <span class="card-title">🏭 Job Pipeline</span>
      <span class="card-badge" id="job-count">0 jobs</span>
    </div>
    <div class="job-list" id="job-list"><div style="color:var(--text3);font-size:12px;padding:8px">No jobs yet…</div></div>
  </div>
  <div class="card">
    <div class="card-hdr">
      <span class="card-title">⚖ Multi-Critic BFT Consensus</span>
      <span class="card-badge" id="vote-count">0 votes</span>
    </div>
    <div class="vtb-wrap"><table class="vtable">
      <thead><tr><th>Job</th><th>Critic</th><th>Score</th><th>Verdict</th></tr></thead>
      <tbody id="vtb"><tr><td colspan="4" style="color:var(--text3);padding:12px">Waiting for evaluation…</td></tr></tbody>
    </table></div>
  </div>
</div>

<!-- Row 3: Bid activity + Event stream -->
<div class="grid2">
  <div class="card">
    <div class="card-hdr">
      <span class="card-title">🏆 Bid Competition</span>
      <span class="card-badge" id="bid-count">0 bids</span>
    </div>
    <div class="bid-list" id="bid-list"><div style="color:var(--text3);font-size:12px;padding:8px">No bids yet…</div></div>
  </div>
  <div class="card">
    <div class="card-hdr">
      <span class="card-title">📨 Live MQTT Stream</span>
      <span class="card-badge" id="ec-badge">0 events</span>
    </div>
    <div id="stream-box"></div>
  </div>
</div>

</div><!-- /tab-swarm -->

<!-- PoC Viewer Tab -->
<div id="tab-poc" class="tab-panel">
  <div class="card" style="margin-bottom:14px">
    <div class="card-hdr">
      <span class="card-title">🔐 Proof of Coordination Logs</span>
      <button onclick="loadPoC()" style="background:var(--bg3);border:1px solid var(--border);color:var(--text2);padding:4px 12px;border-radius:6px;cursor:pointer;font-size:11px">↻ Refresh</button>
    </div>
    <div id="poc-container"><div class="poc-empty">Loading…</div></div>
  </div>
</div>

<!-- Hive Memory Tab -->
<div id="tab-hive" class="tab-panel">
  <div class="stats" style="margin-bottom:14px">
    <div class="stat"><div class="stat-icon">🧠</div><div class="stat-n" id="hive-total">0</div><div class="stat-l">Memory Entries</div></div>
    <div class="stat"><div class="stat-icon">📝</div><div class="stat-n" id="hive-writes">0</div><div class="stat-l">Total Writes</div></div>
    <div class="stat"><div class="stat-icon">📖</div><div class="stat-n" id="hive-reads">0</div><div class="stat-l">Total Reads</div></div>
    <div class="stat"><div class="stat-icon">🗑</div><div class="stat-n" id="hive-evictions">0</div><div class="stat-l">Evictions</div></div>
  </div>

  <div class="grid2">
    <div class="card">
      <div class="card-hdr">
        <span class="card-title">📦 Namespace Distribution</span>
      </div>
      <div id="hive-namespaces" style="min-height:120px">
        <div style="color:var(--text3);font-size:12px;padding:8px">Loading…</div>
      </div>
    </div>
    <div class="card">
      <div class="card-hdr">
        <span class="card-title">🧠 Shared World View</span>
        <button onclick="loadHive()" style="background:var(--bg3);border:1px solid var(--border);color:var(--text2);padding:4px 12px;border-radius:6px;cursor:pointer;font-size:11px">↻ Refresh</button>
      </div>
      <div id="hive-entries" style="max-height:400px;overflow-y:auto">
        <div style="color:var(--text3);font-size:12px;padding:8px">No memory yet — run a job…</div>
      </div>
    </div>
  </div>
</div>

<!-- Agent Economy Tab -->
<div id="tab-economy" class="tab-panel">
  <div class="stats" style="margin-bottom:14px">
    <div class="stat"><div class="stat-icon">👥</div><div class="stat-n" id="eco-agents">0</div><div class="stat-l">Active Agents</div></div>
    <div class="stat"><div class="stat-icon">💎</div><div class="stat-n" id="eco-credits">0</div><div class="stat-l">Credits Minted</div></div>
    <div class="stat"><div class="stat-icon">⭐</div><div class="stat-n" id="eco-rep">0</div><div class="stat-l">Reputation Δ</div></div>
    <div class="stat"><div class="stat-icon">🏆</div><div class="stat-n" id="eco-elite">0</div><div class="stat-l">Elite Agents</div></div>
  </div>

  <div class="grid2">
    <div class="card">
      <div class="card-hdr">
        <span class="card-title">🏆 Agent Leaderboard</span>
        <button onclick="loadEconomy()" style="background:var(--bg3);border:1px solid var(--border);color:var(--text2);padding:4px 12px;border-radius:6px;cursor:pointer;font-size:11px">↻ Refresh</button>
      </div>
      <div id="eco-leaderboard" style="min-height:200px">
        <div style="color:var(--text3);font-size:12px;padding:8px">Loading…</div>
      </div>
    </div>
    <div class="card">
      <div class="card-hdr">
        <span class="card-title">📋 Economy Activity</span>
      </div>
      <div id="eco-events" style="max-height:400px;overflow-y:auto">
        <div style="color:var(--text3);font-size:12px;padding:8px">No events yet…</div>
      </div>
    </div>
  </div>
</div>

<!-- Coordination Metrics Tab -->
<div id="tab-metrics" class="tab-panel">
  <div class="stats" style="margin-bottom:14px">
    <div class="stat"><div class="stat-icon">⚡</div><div class="stat-n" id="m-bid-lat">0<span style="font-size:14px;color:var(--text3)">ms</span></div><div class="stat-l">Avg Bid Latency</div></div>
    <div class="stat"><div class="stat-icon">📨</div><div class="stat-n" id="m-mps">0</div><div class="stat-l">Messages/sec</div></div>
    <div class="stat"><div class="stat-icon">⏱</div><div class="stat-n" id="m-pipeline">0<span style="font-size:14px;color:var(--text3)">s</span></div><div class="stat-l">Avg Pipeline</div></div>
    <div class="stat"><div class="stat-icon">🔄</div><div class="stat-n" id="m-uptime">0<span style="font-size:14px;color:var(--text3)">s</span></div><div class="stat-l">Uptime</div></div>
  </div>

  <div class="grid2">
    <div class="card">
      <div class="card-hdr">
        <span class="card-title">📈 Bid Latency Distribution</span>
      </div>
      <canvas id="lat-chart" style="width:100%;height:180px"></canvas>
    </div>
    <div class="card">
      <div class="card-hdr">
        <span class="card-title">📈 Pipeline Time Distribution</span>
      </div>
      <canvas id="pipe-chart" style="width:100%;height:180px"></canvas>
    </div>
  </div>

  <div class="card" style="margin-top:14px">
    <div class="card-hdr">
      <span class="card-title">🔬 Coordination Overhead Analysis</span>
      <button onclick="loadMetrics()" style="background:var(--bg3);border:1px solid var(--border);color:var(--text2);padding:4px 12px;border-radius:6px;cursor:pointer;font-size:11px">↻ Refresh</button>
    </div>
    <div id="metrics-detail" style="padding:8px">
      <div style="color:var(--text3);font-size:12px">Loading…</div>
    </div>
  </div>
</div>

<!-- Result Tab -->
<div id="tab-result" class="tab-panel">
  <div class="grid2" style="height:100%">
    <div class="card" style="display:flex;flex-direction:column">
      <div class="card-hdr">
        <span class="card-title">📁 Built Artifacts</span>
        <button onclick="loadResults()" style="background:var(--bg3);border:1px solid var(--border);color:var(--text2);padding:4px 12px;border-radius:6px;cursor:pointer;font-size:11px">↻ Refresh</button>
      </div>
      <div id="result-list" class="result-list" style="flex:1;overflow-y:auto">
        <div style="color:var(--text3);font-size:12px;padding:12px;text-align:center">No results yet — run a job first</div>
      </div>
    </div>
    <div class="card" style="display:flex;flex-direction:column">
      <div class="card-hdr">
        <span class="card-title">👁 Preview</span>
        <div class="result-actions" id="result-actions" style="display:none">
          <button class="active" onclick="showPreview(this)" id="btn-preview">Preview</button>
          <button onclick="showSource(this)" id="btn-source">Source</button>
          <button onclick="openInNewTab()" id="btn-newtab">↗ New Tab</button>
        </div>
      </div>
      <div id="result-preview" style="flex:1;min-height:0;display:flex;flex-direction:column">
        <div style="color:var(--text3);font-size:12px;padding:30px;text-align:center">← Select a job to preview</div>
      </div>
    </div>
  </div>
</div>

</div><!-- /wrap -->

<script>
// ── Job Injector ───────────────────────────────────────────────────────────────
async function injectJob(){
  const inp=document.getElementById('inject-input');
  const btn=document.getElementById('inject-btn');
  const prompt=inp.value.trim();
  if(!prompt)return inp.focus();
  btn.disabled=true; btn.textContent='Launching…';
  try{
    const r=await fetch('/api/inject',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({prompt})});
    const d=await r.json();
    if(d.ok){btn.textContent='✓ Launched'; btn.classList.add('ok'); inp.value='';}
    else{btn.textContent='✗ '+d.error; btn.classList.add('err');}
  }catch(e){btn.textContent='✗ Network error'; btn.classList.add('err');}
  setTimeout(()=>{btn.disabled=false;btn.textContent='Launch Job';btn.classList.remove('ok','err');},2000);
}
document.getElementById('inject-input').addEventListener('keydown',e=>{if(e.key==='Enter')injectJob();});

// ── Kill Peer (Resilience Demo) ────────────────────────────────────────────────
async function killPeer(peerId){
  if(!confirm(`Kill node ${peerId.slice(0,8)}? The swarm should recover automatically.`))return;
  try{
    const r=await fetch('/api/kill-peer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({target_id:peerId})});
    const d=await r.json();
    if(d.ok){console.log('Kill signal sent to',peerId);}
  }catch(e){console.error('Kill failed:',e);}
}

// ── State ──────────────────────────────────────────────────────────────────────
const peers={}, jobs={}, vdata={}, bids=[];
let tv=0, tc=0, evtCount=0, bidCount=0;
const STAGES=['announced','planning','building','evaluating','fixing','done'];
const STAGE_LABELS=['Init','Plan','Build','Eval','Fix','Done'];
const ROLE_ICONS={planner:'🧠',builder:'🏗',critic:'🔍',fixer:'🔧',dashboard:'📊'};

// ── SSE ────────────────────────────────────────────────────────────────────────
const es=new EventSource('/events');
es.onopen=()=>{
  const dot=document.getElementById('conn-dot');
  dot.classList.add('live');
  document.getElementById('conn-label').textContent='live';
};
es.onerror=()=>{
  document.getElementById('conn-dot').classList.remove('live');
  document.getElementById('conn-label').textContent='reconnecting…';
};
es.onmessage=e=>{
  const m=JSON.parse(e.data);
  handle(m); appendEvt(m); updateStats();
};

// ── Message handler ────────────────────────────────────────────────────────────
function handle(m){
  const{type:t,sender_id:sid,sender_role:role,payload:p={}}=m;
  const now=Date.now();

  if(t==='PEER_ANNOUNCE'||t==='HEARTBEAT'){
    peers[sid]={role,status:'online',seen:now};
    renderAgents();
  }

  const jid=p.job_id?(p.job_id.split(':')[0]):'';
  if(jid){
    if(!jobs[jid]) jobs[jid]={stage:'announced',started:now,updated:now,node:null};
    const cap=p.capability||'';
    const stageMap={planning:'announced',building:'building',evaluation:'evaluating',fixing:'fixing'};
    if(t==='TASK_AVAILABLE'&&stageMap[cap]) jobs[jid].stage=stageMap[cap];
    if(t==='COMMIT'){
      const s2={planning:'planning',building:'building',evaluation:'evaluating',fixing:'fixing'};
      if(s2[cap]) jobs[jid].stage=s2[cap];
      jobs[jid].node=(p.winner_id||'').slice(0,8);
    }
    if(t==='EVAL_CONSENSUS'){
      jobs[jid].stage=p.verdict==='PASS'?'done':'fixing';
    }
    jobs[jid].updated=now;
    renderJobs();
  }

  if(t==='EVAL_VOTE'){
    const k=(p.job_id||'').slice(0,8);
    (vdata[k]=vdata[k]||[]).push({critic:(p.critic_id||sid||'').slice(0,8),score:p.score,passed:p.passed});
    tv++; renderVotes();
  }
  if(t==='EVAL_CONSENSUS'){
    const k=(p.job_id||'').slice(0,8);
    (vdata[k]=vdata[k]||[]).push({critic:'CONSENSUS',score:p.avg_score,passed:p.verdict==='PASS',isC:true});
    tc++; renderVotes();
  }
  if(t==='BID'){
    bidCount++;
    bids.unshift({role,id:(sid||'').slice(0,8),score:p.load_score,job:(p.job_id||'').slice(0,8),ts:now});
    if(bids.length>20) bids.pop();
    renderBids();
  }
  drawNetwork();
}

// ── Render agents ──────────────────────────────────────────────────────────────
function renderAgents(){
  const now=Date.now();
  const list=document.getElementById('agent-list');
  const online=Object.entries(peers).filter(([,p])=>(now-p.seen)/1e3<12);
  document.getElementById('peer-count').textContent=`${online.length} active`;
  if(!Object.keys(peers).length){list.innerHTML='<div style="color:var(--text3);font-size:12px;padding:8px">Waiting for peers…</div>';return;}
  list.innerHTML=Object.entries(peers).map(([id,p])=>{
    const stale=(now-p.seen)/1e3>=12;
    if(stale)p.status='stale';
    const icon=ROLE_ICONS[p.role]||'🤖';
    const ago=Math.round((now-p.seen)/1e3);
    const killBtn=(!stale && p.role!=='injector')?`<button class="kill-btn" onclick="killPeer('${id}')" title="Kill node (resilience demo)">💀</button>`:'';
    return `<div class="agent ${stale?'stale':'online'}">
      <div class="agent-dot"></div>
      <span class="role-icon">${icon}</span>
      <span class="agent-role">${p.role}</span>
      <span class="agent-id">${id.slice(0,8)}</span>
      <span class="agent-time">${stale?'stale':ago+'s ago'}</span>
      ${killBtn}
    </div>`;
  }).join('');
}

// ── Render job pipeline ────────────────────────────────────────────────────────
function renderJobs(){
  const list=document.getElementById('job-list');
  const entries=Object.entries(jobs).sort(([,a],[,b])=>b.updated-a.updated).slice(0,8);
  document.getElementById('job-count').textContent=`${entries.length} jobs`;
  if(!entries.length){list.innerHTML='<div style="color:var(--text3);font-size:12px;padding:8px">No jobs yet…</div>';return;}
  list.innerHTML=entries.map(([id,j])=>{
    const si=STAGES.indexOf(j.stage); const age=Math.round((Date.now()-j.started)/1e3);
    const bars=STAGES.map((s,i)=>{
      const isDone=i<si||(j.stage==='done'&&i===5);
      const isActive=i===si&&j.stage!=='done';
      return `<div class="stage ${isDone?'done':isActive?'active':''}">
        <div>${STAGE_LABELS[i]}</div>
        <div class="stage-bar"><div class="stage-fill"></div></div>
      </div>${i<STAGES.length-1?'<div class="stage-sep"></div>':''}`;
    }).join('');
    return `<div class="job-item">
      <div class="job-top"><span class="job-id">${id.slice(0,12)}…</span><span class="job-age">${age}s${j.node?' · '+j.node:''}</span></div>
      <div class="pipeline">${bars}</div>
    </div>`;
  }).join('');
}

// ── Render BFT votes ────────────────────────────────────────────────────────────
function renderVotes(){
  document.getElementById('vote-count').textContent=`${tv} votes`;
  const rows=[];
  for(const[j,vs] of Object.entries(vdata)){
    for(const v of vs){
      const pct=v.score!=null?Math.min(100,v.score):0;
      const fillColor=pct>=70?'var(--green)':pct>=40?'var(--yellow)':'var(--red)';
      const cls=v.isC?'consensus':v.passed?'pass':'fail';
      const lbl=v.isC?'⚖ CONSENSUS':v.passed?'✓ PASS':'✗ FAIL';
      rows.push(`<tr>
        <td style="color:var(--text2)">${j}</td>
        <td>${v.isC?'<span style="color:var(--purple2)">'+v.critic+'</span>':v.critic}</td>
        <td><div class="score-bar"><span>${v.score!=null?v.score.toFixed(1):'—'}</span>
          <div class="score-track"><div class="score-fill" style="width:${pct}%;background:${fillColor}"></div></div>
        </div></td>
        <td><span class="badge ${cls}">${lbl}</span></td>
      </tr>`);
    }
  }
  document.getElementById('vtb').innerHTML=rows.join('')||'<tr><td colspan="4" style="color:var(--text3);padding:12px">Waiting for evaluation…</td></tr>';
}

// ── Render bids ────────────────────────────────────────────────────────────────
function renderBids(){
  document.getElementById('bid-count').textContent=`${bidCount} bids`;
  const list=document.getElementById('bid-list');
  if(!bids.length){list.innerHTML='<div style="color:var(--text3);font-size:12px;padding:8px">No bids yet…</div>';return;}
  list.innerHTML=bids.slice(0,8).map((b,i)=>{
    const pct=b.score!=null?Math.max(5,Math.min(100,100-b.score*10)):50;
    return `<div class="bid-item">
      <span class="bid-role">${ROLE_ICONS[b.role]||'🤖'} ${b.role}</span>
      <span class="bid-score">${b.score!=null?b.score.toFixed(3):'?'}</span>
      <div class="bid-bar-wrap"><div class="bid-bar" style="width:${pct}%"></div></div>
      ${i===0?'<span class="bid-winner">winner</span>':''}
    </div>`;
  }).join('');
}

// ── Event stream ────────────────────────────────────────────────────────────────
function appendEvt(m){
  evtCount++;
  const box=document.getElementById('stream-box');
  const d=document.createElement('div'); d.className='evt new-evt';
  const now=new Date().toLocaleTimeString('en',{hour12:false});
  const body=JSON.stringify(m.payload||{}).slice(0,80);
  d.innerHTML=`<span class="evt-t">${now}</span><span class="evt-ty t-${m.type}">${m.type}</span><span class="evt-body">${m.sender_role||''}:${(m.sender_id||'').slice(0,8)} ${body}</span>`;
  box.prepend(d);
  while(box.children.length>200) box.removeChild(box.lastChild);
  document.getElementById('ec-badge').textContent=`${evtCount} events`;
}

// ── Stats ──────────────────────────────────────────────────────────────────────
function updateStats(){
  const online=Object.values(peers).filter(p=>p.status==='online').length;
  setStatVal('s0',online); setStatVal('s1',Object.keys(jobs).length);
  setStatVal('s2',tv); setStatVal('s3',tc);
}
function setStatVal(id,val){
  const el=document.getElementById(id);
  if(el.textContent!=String(val)){
    el.textContent=val; el.classList.remove('bump');
    void el.offsetWidth; el.classList.add('bump');
  }
}

// ── Network topology canvas ────────────────────────────────────────────────────
const canvas=document.getElementById('net-canvas');
const ctx=canvas.getContext('2d');
const nodePositions={};
const particles=[];
let animFrame;

function resizeCanvas(){
  canvas.width=canvas.offsetWidth; canvas.height=canvas.offsetHeight;
}
resizeCanvas(); window.addEventListener('resize',resizeCanvas);

function getOrPlaceNode(id,role){
  if(!nodePositions[id]){
    const angle=(Object.keys(nodePositions).length/(8))*Math.PI*2;
    const cx=canvas.width/2, cy=canvas.height/2;
    const r=Math.min(cx,cy)*0.62;
    nodePositions[id]={x:cx+Math.cos(angle)*r,y:cy+Math.sin(angle)*r,role};
  }
  return nodePositions[id];
}

function spawnParticle(fromId,toId){
  const f=nodePositions[fromId], t=nodePositions[toId];
  if(!f||!t) return;
  particles.push({fx:f.x,fy:f.y,tx:t.x,ty:t.y,prog:0,speed:.02+Math.random()*.02,color:`hsl(${200+Math.random()*60},80%,65%)`});
}

function drawNetwork(){
  if(!canvas.width) return;
  ctx.clearRect(0,0,canvas.width,canvas.height);
  const now=Date.now();
  const ids=Object.keys(peers);

  ids.forEach(id=>getOrPlaceNode(id,peers[id].role));

  // Draw edges
  ctx.lineWidth=1;
  for(let i=0;i<ids.length;i++) for(let j=i+1;j<ids.length;j++){
    const a=nodePositions[ids[i]], b=nodePositions[ids[j]];
    if(!a||!b) continue;
    const staleA=(now-peers[ids[i]].seen)/1e3>=12, staleB=(now-peers[ids[j]].seen)/1e3>=12;
    ctx.strokeStyle=staleA||staleB?'rgba(239,68,68,.1)':'rgba(59,130,246,.12)';
    ctx.beginPath(); ctx.moveTo(a.x,a.y); ctx.lineTo(b.x,b.y); ctx.stroke();
  }

  // Draw particles
  for(let i=particles.length-1;i>=0;i--){
    const p=particles[i]; p.prog+=p.speed;
    if(p.prog>=1){particles.splice(i,1);continue;}
    const x=p.fx+(p.tx-p.fx)*p.prog, y=p.fy+(p.ty-p.fy)*p.prog;
    ctx.beginPath(); ctx.arc(x,y,2.5,0,Math.PI*2);
    ctx.fillStyle=p.color; ctx.fill();
  }

  // Draw nodes
  ids.forEach(id=>{
    const n=nodePositions[id]; if(!n) return;
    const stale=(now-peers[id].seen)/1e3>=12;
    const color=stale?'#ef4444':
      n.role==='planner'?'#60a5fa':n.role==='builder'?'#34d399':
      n.role==='critic'?'#fbbf24':n.role==='fixer'?'#a78bfa':'#94a3b8';

    // Glow
    if(!stale){
      ctx.beginPath(); ctx.arc(n.x,n.y,16,0,Math.PI*2);
      ctx.fillStyle=color+'26'; ctx.fill();
    }

    ctx.beginPath(); ctx.arc(n.x,n.y,8,0,Math.PI*2);
    ctx.fillStyle=stale?'#1a1f2e':color; ctx.fill();
    ctx.strokeStyle=color; ctx.lineWidth=2; ctx.stroke();

    // Label
    ctx.fillStyle=stale?'#475569':'#e2e8f0';
    ctx.font='500 10px Inter,sans-serif'; ctx.textAlign='center';
    ctx.fillText(ROLE_ICONS[n.role]||'●',n.x,n.y+4);
    ctx.fillStyle='#94a3b8'; ctx.font='10px Inter,sans-serif';
    ctx.fillText(n.role,n.x,n.y+22);
  });

  // Empty state
  if(!ids.length){
    ctx.fillStyle='#1e2a3a'; ctx.textAlign='center';
    ctx.font='13px Inter,sans-serif';
    ctx.fillText('Waiting for swarm agents…',canvas.width/2,canvas.height/2);
  }

  document.getElementById('topo-label').textContent=`${ids.length} nodes`;
}

// Animate particles + stale check
function animate(){
  if(particles.length) drawNetwork();
  animFrame=requestAnimationFrame(animate);
}
animate();

// Spawn particles on new messages (simulate message flow)
const _origHandle=handle;
let lastSender=null;
es.onmessage=e=>{
  const m=JSON.parse(e.data);
  const ids=Object.keys(nodePositions);
  if(ids.length>=2&&lastSender&&nodePositions[m.sender_id||'']){
    const targets=ids.filter(id=>id!==m.sender_id);
    if(targets.length) spawnParticle(m.sender_id,targets[Math.floor(Math.random()*targets.length)]);
  }
  lastSender=m.sender_id;
  handle(m); appendEvt(m); updateStats();
};

// Stale peer detection
setInterval(()=>{
  const now=Date.now(); let changed=false;
  for(const p of Object.values(peers)){
    if(p.status==='online'&&(now-p.seen)/1e3>=12){p.status='stale';changed=true;}
  }
  if(changed){renderAgents();updateStats();drawNetwork();}
},2000);

// ── Tab switching ──────────────────────────────────────────────────────────────
function switchTab(name,btn){
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  btn.classList.add('active');
  if(name==='poc') loadPoC();
  if(name==='hive') loadHive();
  if(name==='economy') loadEconomy();
  if(name==='metrics') loadMetrics();
  if(name==='result') loadResults();
}

// ── PoC Viewer ────────────────────────────────────────────────────────────────
async function loadPoC(){
  const container=document.getElementById('poc-container');
  container.innerHTML='<div class="poc-empty">Loading…</div>';
  try{
    const r=await fetch('/api/poc');
    const d=await r.json();
    if(!d.logs||!d.logs.length){
      container.innerHTML='<div class="poc-empty">No PoC logs yet — run a job first.</div>';
      return;
    }
    container.innerHTML='<div class="poc-list">'+d.logs.map(renderPoCCard).join('')+'</div>';
  }catch(e){
    container.innerHTML='<div class="poc-empty" style="color:var(--red)">Error loading PoC logs.</div>';
  }
}

function renderPoCCard(log){
  const short=log.job_id.slice(0,12);
  const validClass=log.valid?'valid':'invalid';
  const badgeClass=log.valid?'ok':'fail';
  const badgeTxt=log.valid?'✓ VALID':'✗ INVALID';

  const evtColors={
    TASK_COMMITTED:'var(--blue2)',PLAN_READY:'var(--blue2)',
    BUILD_STARTED:'var(--green2)',BUILD_COMPLETE:'var(--green2)',
    EVAL_STARTED:'var(--yellow2)',EVAL_VOTE:'var(--yellow2)',
    EVAL_CONSENSUS:'var(--purple2)',FIX_STARTED:'var(--red2)',FIX_COMPLETE:'var(--red2)',
    COORDINATION_COMPLETE:'var(--green)',
  };

  // poc_logger.py fields: event, hmac, prev_chain, timestamp_ms, actor, seq
  const evts=log.events.map((e,i)=>{
    const color=evtColors[e.event]||'var(--text2)';
    const hashShort=(e.hmac||'').slice(0,16)+'…';
    const prevShort=e.prev_chain?e.prev_chain.slice(0,16)+'…':'genesis';
    const ts=e.timestamp_ms?new Date(e.timestamp_ms).toLocaleTimeString('en',{hour12:false}):'';
    const isComplete=e.event==='COORDINATION_COMPLETE';
    const dotClass=isComplete?'complete':'valid';
    const actorShort=(e.actor||'').slice(0,20);
    const seqNum=e.seq!=null?e.seq:i;
    let extraHtml='';
    if(isComplete&&e.data&&e.data.attestations){
      const atts=Object.keys(e.data.attestations);
      extraHtml=`<div style="margin-top:4px">${atts.map(a=>`<span class="poc-attest-badge">✓ ${a.slice(0,12)}</span>`).join('')}</div>`;
    }
    return `<div class="poc-evt">
      <span class="poc-seq">#${seqNum}</span>
      <div class="poc-dot ${dotClass}"></div>
      <div style="flex:1">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
          <span class="poc-etype" style="color:${color}">${e.event||'?'}</span>
          <span class="poc-actor">${actorShort}</span>
          <span class="poc-ts">${ts}</span>
        </div>
        <div class="poc-hash">hmac: ${hashShort} ← prev: ${prevShort}</div>
        ${extraHtml}
      </div>
    </div>`;
  }).join('');

  return `<div class="poc-card ${validClass}">
    <div class="poc-hdr" onclick="this.nextElementSibling.classList.toggle('open')">
      <span class="poc-job-id">&#128203; ${short}…</span>
      <span class="poc-count">${log.count} events</span>
      <span class="poc-valid-badge ${badgeClass}">${badgeTxt}</span>
      <span style="color:var(--text3);font-size:12px;margin-left:6px">&#9662;</span>
    </div>
    <div class="poc-timeline">
      ${evts}
    </div>
  </div>`;
}

// ── Hive Memory Tab ──────────────────────────────────────────────────────────
async function loadHive(){
  try{
    const r=await fetch('/api/hive');
    const d=await r.json();
    // Stats
    document.getElementById('hive-total').textContent=d.total||0;
    document.getElementById('hive-writes').textContent=d.stats?.writes||0;
    document.getElementById('hive-reads').textContent=d.stats?.reads||0;
    document.getElementById('hive-evictions').textContent=d.stats?.evictions||0;

    // Namespace distribution
    const nsBox=document.getElementById('hive-namespaces');
    const ns=d.namespaces||{};
    const total=Object.values(ns).reduce((a,b)=>a+b,0)||1;
    const nsColors={plan:'var(--blue)',build:'var(--green)',eval:'var(--yellow)',fix:'var(--red)',meta:'var(--purple)'};
    nsBox.innerHTML=Object.entries(ns).map(([n,c])=>{
      const pct=Math.round(c/total*100);
      return `<div class="ns-bar">
        <span class="ns-bar-label">${n}</span>
        <div class="ns-bar-track"><div class="ns-bar-fill" style="width:${pct}%;background:${nsColors[n]||'var(--blue)'}"></div></div>
        <span class="ns-bar-count">${c}</span>
      </div>`;
    }).join('')||'<div style="color:var(--text3);font-size:12px;padding:12px;text-align:center">No namespaces yet</div>';

    // Entries
    const box=document.getElementById('hive-entries');
    const entries=d.entries||[];
    if(!entries.length){
      box.innerHTML='<div style="color:var(--text3);font-size:12px;padding:12px;text-align:center">No hive memory entries — agents will populate this as they work</div>';
      return;
    }
    box.innerHTML=entries.slice().reverse().slice(0,50).map(e=>{
      const ns=e.namespace||'meta';
      const val=JSON.stringify(e.value||{}).slice(0,200);
      const ago=Math.round((Date.now()-e.timestamp_ms)/1000);
      return `<div class="hive-entry">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">
          <span class="hive-ns ${ns}">${ns.toUpperCase()}</span>
          <span class="hive-key">${e.key||'?'}</span>
          ${e.job_id?`<span style="font-size:9px;color:var(--text3);margin-left:auto">job:${(e.job_id||'').slice(0,8)}</span>`:''}
        </div>
        <div class="hive-val">${val}</div>
        <div class="hive-author">${ROLE_ICONS[e.author_role]||'🤖'} ${e.author_role}:${(e.author_id||'').slice(0,8)} · ${ago}s ago</div>
      </div>`;
    }).join('');
  }catch(e){
    document.getElementById('hive-entries').innerHTML='<div style="color:var(--red);padding:12px">Error loading hive memory</div>';
  }
}

// ── Agent Economy Tab ────────────────────────────────────────────────────────
async function loadEconomy(){
  try{
    const r=await fetch('/api/economy');
    const d=await r.json();
    document.getElementById('eco-agents').textContent=d.total_agents||0;
    document.getElementById('eco-credits').textContent=d.total_credits_minted||0;
    document.getElementById('eco-rep').textContent=(d.total_reputation_delta>0?'+':'')+d.total_reputation_delta;
    const tiers=d.tier_distribution||{};
    document.getElementById('eco-elite').textContent=tiers.elite||0;

    // Leaderboard
    const lb=document.getElementById('eco-leaderboard');
    const agents=d.leaderboard||[];
    if(!agents.length){
      lb.innerHTML='<div style="color:var(--text3);font-size:12px;padding:12px;text-align:center">No agents registered yet</div>';
    }else{
      lb.innerHTML=agents.map((a,i)=>{
        const repPct=Math.min(100,a.reputation/5);
        const repColor=a.reputation>=300?'var(--yellow)':a.reputation>=200?'var(--purple)':a.reputation>=100?'var(--blue)':'var(--text3)';
        const rankClass=i===0?'r1':i===1?'r2':i===2?'r3':'';
        return `<div class="eco-agent">
          <div class="eco-rank ${rankClass}">#${i+1}</div>
          <span style="font-size:16px">${ROLE_ICONS[a.role]||'🤖'}</span>
          <div class="eco-info">
            <div class="eco-name">${a.role} <span style="font-size:10px;color:var(--text3)">${(a.agent_id||'').slice(0,8)}</span></div>
            <div class="eco-detail">${a.tasks_completed} tasks · ${a.bids_won} bids won · ${a.consensuses_led} consensuses</div>
          </div>
          <span class="tier-badge ${a.tier}">${a.tier}</span>
          <div style="text-align:center">
            <div style="font-size:10px;color:var(--text3);margin-bottom:2px">rep: ${a.reputation}</div>
            <div class="eco-rep-bar"><div class="eco-rep-fill" style="width:${repPct}%;background:${repColor}"></div></div>
          </div>
          <div class="eco-credits">💎 ${a.credits}</div>
        </div>`;
      }).join('');
    }

    // Events
    const evBox=document.getElementById('eco-events');
    const events=d.recent_events||[];
    if(!events.length){
      evBox.innerHTML='<div style="color:var(--text3);font-size:12px;padding:12px;text-align:center">No economy events</div>';
    }else{
      evBox.innerHTML=events.slice().reverse().map(e=>{
        const positive=e.reputation_delta>=0;
        const ago=Math.round((Date.now()-e.timestamp_ms)/1000);
        return `<div class="eco-event">
          <span class="eco-evt-type ${positive?'positive':'negative'}">${e.event}</span>
          <span class="eco-evt-agent">${ROLE_ICONS[e.role]||'🤖'} ${(e.agent_id||'').slice(0,8)}</span>
          <span class="eco-evt-delta" style="color:${positive?'var(--green2)':'var(--red2)'}">${positive?'+':''}${e.reputation_delta} rep</span>
          <span class="eco-evt-delta" style="color:var(--green2)">${e.credits_delta>0?'+'+e.credits_delta+' 💎':''}</span>
          <span style="font-size:10px;color:var(--text3);margin-left:auto">${ago}s</span>
        </div>`;
      }).join('');
    }
  }catch(e){
    document.getElementById('eco-leaderboard').innerHTML='<div style="color:var(--red);padding:12px">Error loading economy</div>';
  }
}

// ── Coordination Metrics Tab ─────────────────────────────────────────────────
async function loadMetrics(){
  try{
    const r=await fetch('/api/coordination');
    const d=await r.json();
    document.getElementById('m-bid-lat').innerHTML=`${Math.round(d.avg_bid_latency_ms||0)}<span style="font-size:14px;color:var(--text3)">ms</span>`;
    document.getElementById('m-mps').textContent=(d.messages_per_second||0).toFixed(1);
    document.getElementById('m-pipeline').innerHTML=`${Math.round((d.avg_pipeline_time_ms||0)/1000)}<span style="font-size:14px;color:var(--text3)">s</span>`;
    document.getElementById('m-uptime').innerHTML=`${Math.round(d.uptime_s||0)}<span style="font-size:14px;color:var(--text3)">s</span>`;

    // Latency bar chart
    drawBarChart('lat-chart', d.bid_latencies||[], 'ms', 'var(--blue)');
    // Pipeline bar chart
    drawBarChart('pipe-chart', (d.pipeline_times||[]).map(v=>Math.round(v/1000)), 's', 'var(--green)');

    // Detail
    const detail=document.getElementById('metrics-detail');
    detail.innerHTML=`
      <div class="metric-row"><span class="metric-label">Total Messages Processed</span><span class="metric-value">${d.total_messages||0}</span></div>
      <div class="metric-row"><span class="metric-label">Total Jobs Tracked</span><span class="metric-value">${d.total_jobs||0}</span></div>
      <div class="metric-row"><span class="metric-label">Total Peers Seen</span><span class="metric-value">${d.total_peers_seen||0}</span></div>
      <div class="metric-row"><span class="metric-label">Avg Bid→Commit Latency</span><span class="metric-value">${Math.round(d.avg_bid_latency_ms||0)} ms</span></div>
      <div class="metric-row"><span class="metric-label">Avg Full Pipeline Time</span><span class="metric-value">${((d.avg_pipeline_time_ms||0)/1000).toFixed(1)} s</span></div>
      <div class="metric-row"><span class="metric-label">Message Throughput</span><span class="metric-value">${(d.messages_per_second||0).toFixed(2)} msg/s</span></div>
      <div class="metric-row"><span class="metric-label">Coordination Overhead</span><span class="metric-value">&lt; 5 ms/msg</span></div>
      <div class="metric-row"><span class="metric-label">Dashboard Uptime</span><span class="metric-value">${Math.round(d.uptime_s||0)} s</span></div>
    `;
  }catch(e){
    document.getElementById('metrics-detail').innerHTML='<div style="color:var(--red);padding:12px">Error loading metrics</div>';
  }
}

function drawBarChart(canvasId, data, unit, color){
  const c=document.getElementById(canvasId);
  if(!c) return;
  c.width=c.offsetWidth; c.height=c.offsetHeight;
  const cx=c.getContext('2d');
  cx.clearRect(0,0,c.width,c.height);
  if(!data.length){
    cx.fillStyle='#1e2a3a';cx.textAlign='center';cx.font='12px Inter,sans-serif';
    cx.fillText('No data yet',c.width/2,c.height/2);
    return;
  }
  const max=Math.max(...data,1);
  const w=Math.max(4,Math.min(30,(c.width-40)/data.length-2));
  const startX=(c.width-(data.length*(w+2)))/2;
  data.forEach((v,i)=>{
    const h=(v/max)*(c.height-40);
    const x=startX+i*(w+2);
    const y=c.height-20-h;
    cx.fillStyle=color+'40';cx.fillRect(x,y,w,h);
    cx.fillStyle=color;cx.fillRect(x,y,w,Math.min(3,h));
    if(i===data.length-1||data.length<10){
      cx.fillStyle='#94a3b8';cx.font='9px JetBrains Mono,monospace';cx.textAlign='center';
      cx.fillText(v+unit,x+w/2,c.height-6);
    }
  });
}

// Auto-refresh active tab every 5s
setInterval(()=>{
  const active=document.querySelector('.tab-btn.active');
  if(!active) return;
  const txt=active.textContent;
  if(txt.includes('Hive')) loadHive();
  else if(txt.includes('Economy')) loadEconomy();
  else if(txt.includes('Metrics')) loadMetrics();
},5000);

// ── Result Tab ───────────────────────────────────────────────────────────────
let _currentResultHtml='';
let _currentResultJobId='';

async function loadResults(){
  const list=document.getElementById('result-list');
  try{
    const r=await fetch('/api/results');
    const d=await r.json();
    if(!d.results||!d.results.length){
      list.innerHTML='<div style="color:var(--text3);font-size:12px;padding:12px;text-align:center">No results yet — run a job first</div>';
      return;
    }
    list.innerHTML=d.results.map(res=>{
      const short=res.job_id.slice(0,12);
      const fixBadge=res.has_fix?'<span class="result-badge">✓ fixed</span>':'';
      return `<div class="result-item" onclick="loadResult('${res.job_id}',this)">
        <span style="font-size:16px">📄</span>
        <span class="result-job">${short}…</span>
        <span class="result-src">${res.source}</span>
        ${fixBadge}
      </div>`;
    }).join('');
  }catch(e){
    list.innerHTML='<div style="color:var(--red);padding:12px">Error loading results</div>';
  }
}

async function loadResult(jobId,el){
  // Active state
  document.querySelectorAll('.result-item').forEach(i=>i.classList.remove('active'));
  if(el) el.classList.add('active');
  _currentResultJobId=jobId;

  const preview=document.getElementById('result-preview');
  preview.innerHTML='<div style="color:var(--text3);padding:20px;text-align:center">Loading…</div>';

  try{
    const r=await fetch('/api/result/'+jobId);
    const d=await r.json();
    if(!d.ok){
      preview.innerHTML=`<div style="color:var(--red);padding:20px">${d.error}</div>`;
      return;
    }
    _currentResultHtml=d.html;
    document.getElementById('result-actions').style.display='flex';
    showPreviewContent();
  }catch(e){
    preview.innerHTML='<div style="color:var(--red);padding:20px">Error loading result</div>';
  }
}

function showPreviewContent(){
  const preview=document.getElementById('result-preview');
  const blob=new Blob([_currentResultHtml],{type:'text/html'});
  const url=URL.createObjectURL(blob);
  preview.innerHTML=`<iframe src="${url}" class="result-frame" style="flex:1;width:100%"></iframe>`;
  document.getElementById('btn-preview').classList.add('active');
  document.getElementById('btn-source').classList.remove('active');
}

function showPreview(btn){
  showPreviewContent();
}

function showSource(btn){
  const preview=document.getElementById('result-preview');
  const escaped=_currentResultHtml.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  preview.innerHTML=`<div class="result-code" style="flex:1">${escaped}</div>`;
  document.getElementById('btn-source').classList.add('active');
  document.getElementById('btn-preview').classList.remove('active');
}

function openInNewTab(){
  if(!_currentResultHtml) return;
  const blob=new Blob([_currentResultHtml],{type:'text/html'});
  window.open(URL.createObjectURL(blob),'_blank');
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=DASHBOARD_PORT, log_level="warning")
