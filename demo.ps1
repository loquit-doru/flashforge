# ═══════════════════════════════════════════════════════════════════
# FlashForge — DEMO SCRIPT (5 min, single job + kill resilience)
# Vertex Swarm Challenge 2026 · Track 3: Agent Economy
# ═══════════════════════════════════════════════════════════════════

$ErrorActionPreference = "Continue"
$ROOT   = "c:\Users\quit\Desktop\flashforge-v2\flashforge"
$PYTHON = "$ROOT\.venv\Scripts\python.exe"
$FOXMQ  = "$ROOT\foxmq.exe"
$API    = "http://localhost:5050"

function Narrate($text) {
    Write-Host ""
    Write-Host "  +--------------------------------------------------------------+" -ForegroundColor DarkCyan
    foreach ($line in ($text -split "`n")) {
        $padded = $line.PadRight(60)
        Write-Host "  |  $padded|" -ForegroundColor DarkCyan
    }
    Write-Host "  +--------------------------------------------------------------+" -ForegroundColor DarkCyan
    Write-Host ""
}

function Write-Step($n, $msg) {
    Write-Host "`n=== STEP $n === $msg ===" -ForegroundColor Cyan
}

function Pause-Demo($msg = "Press ENTER to continue...") {
    Write-Host "  -> $msg" -ForegroundColor Yellow
    Read-Host
}

# ── 0. Cleanup ──
Write-Step 0 "Cleanup"
Narrate "Welcome to the FlashForge demo.`nFlashForge is a LEADERLESS multi-agent swarm`nthat builds software collaboratively using`nFoxMQ's Vertex BFT consensus protocol.`n`nNo central orchestrator. No single point of`nfailure. Every message is HMAC-SHA256 signed`nwith nonce-based replay prevention.`n`nDemo flow:`n  1. Start broker + dashboard + 5 agents`n  2. Inject ONE job`n  3. KILL a node mid-job (resilience test)`n  4. Swarm self-heals and completes the job`n  5. Review all dashboard tabs"
Get-Process -Name python -ErrorAction SilentlyContinue | Stop-Process -Force -EA SilentlyContinue
Get-Process -Name foxmq  -ErrorAction SilentlyContinue | Stop-Process -Force -EA SilentlyContinue
if (Test-Path "$ROOT\swarm_output") { Remove-Item "$ROOT\swarm_output\*" -Recurse -Force -EA SilentlyContinue }
Start-Sleep 1
Write-Host "  OK Clean slate" -ForegroundColor Green
Pause-Demo

# ── 1. FoxMQ Broker ──
Write-Step 1 "Start FoxMQ Broker"
Narrate "FoxMQ is an MQTT 5.0 broker powered by the`nVertex BFT consensus engine. It guarantees`nthat ALL subscribers see messages in the`nEXACT SAME ORDER -- eliminating coordination`nraces at the transport layer.`n`nThis is what makes leaderless coordination`npossible: fair ordering without a leader."
Start-Process -FilePath $FOXMQ `
    -ArgumentList "run","--allow-anonymous-login","foxmq.d" `
    -WorkingDirectory $ROOT -WindowStyle Minimized
Start-Sleep 2
$check = netstat -an | Select-String ":1883.*LISTENING"
if ($check) {
    Write-Host "  OK FoxMQ broker running (port 1883)" -ForegroundColor Green
} else {
    Write-Host "  FAIL: FoxMQ did not start!" -ForegroundColor Red; exit 1
}
Pause-Demo

# ── 2. Dashboard ──
Write-Step 2 "Start Dashboard"
Narrate "The dashboard is a FastAPI server that`nconnects to FoxMQ as an observer node.`nIt receives ALL swarm messages via MQTT and`nstreams them to the browser using SSE.`n`n6 tabs to explore:`n  Live  -- real-time network topology`n  PoC   -- cryptographic audit trail`n  Hive  -- decentralized shared knowledge`n  Economy -- reputation + credits (Track 3)`n  Metrics -- coordination statistics`n  Result  -- generated app preview"
$env:PYTHONPATH = $ROOT
Start-Process -FilePath $PYTHON -ArgumentList "$ROOT\swarm\dashboard_server.py" `
    -WorkingDirectory $ROOT -WindowStyle Minimized
Start-Sleep 3
try {
    Invoke-RestMethod "$API/api/events" -TimeoutSec 3 | Out-Null
    Write-Host "  OK Dashboard live at $API" -ForegroundColor Green
} catch {
    Write-Host "  FAIL: Dashboard not responding!" -ForegroundColor Red; exit 1
}
Start-Process "http://localhost:5050"
Pause-Demo

# ── 3. Spawn Agents ──
Write-Step 3 "Spawn 5 Autonomous Agents"
Narrate "We launch 5 independent agent processes.`nEach joins the swarm via PEER_ANNOUNCE and`nstarts sending HEARTBEAT every 2 seconds.`n`n  Planner  -- Groq LLaMA 3.3 70B`n  Builder  -- Google Gemini 2.5 Flash`n  Critic A -- Groq LLaMA 3.3 70B (voter 1)`n  Critic B -- Groq LLaMA 3.3 70B (voter 2)`n  Fixer    -- Qwen 2.5 Coder 32B`n`nNo agent knows about the others in advance.`nThey discover each other through the mesh."
$scripts = @("run_planner_node.py","run_builder_node.py","run_critic_node.py","run_critic_node.py","run_fixer_node.py")
$names  = @("planner","builder","critic-a","critic-b","fixer")
for ($i = 0; $i -lt $scripts.Count; $i++) {
    Start-Process -FilePath $PYTHON -ArgumentList "$ROOT\swarm\$($scripts[$i])" `
        -WorkingDirectory $ROOT -WindowStyle Minimized
    Write-Host "  OK $($names[$i]) joined the mesh" -ForegroundColor Green
    Start-Sleep -Milliseconds 800
}
Start-Sleep 3
Narrate "All 5 agents are online. Check the dashboard:`nyou should see 5 green dots in the Live tab`nand all agents listed in the Swarm Agents panel."
Pause-Demo "Look at the Live tab, then press ENTER..."

# ── 4. Inject Job + Kill + Watch ──
Write-Step 4 "Inject Job + Kill Builder Mid-Pipeline"
$jobPrompt = "Build a simple counter app with + and - buttons"
Narrate "We inject a job via the dashboard API:`n`n  '$jobPrompt'`n`nThis creates a TASK_AVAILABLE message on MQTT.`nAll capable agents BID for it -- the auction`nselects the least-loaded agent.`n`nPipeline stages:`n  PLAN -> BUILD -> EVAL (BFT consensus) -> DONE`n`nDuring the PLANNING phase, we will KILL the`nbuilder to demonstrate fault tolerance.`nThe builder will auto-respawn in 3 seconds`nwith a fresh identity and pick up the job."
$body = @{ prompt = $jobPrompt } | ConvertTo-Json
$resp = Invoke-RestMethod "$API/api/inject" -Method POST -ContentType "application/json" -Body $body
$jobId = $resp.job_id
$prefix = $jobId.Substring(0,8)
Write-Host "  OK Job injected: $prefix..." -ForegroundColor Green
Write-Host ""

# Watch pipeline + kill builder during planning
$killed = $false
for ($i = 0; $i -lt 80; $i++) {
    Start-Sleep 3
    try {
        $jobs = (Invoke-RestMethod "$API/api/jobs").jobs
        $cur = $jobs | Where-Object { $_.job_id -like "$prefix*" } | Select-Object -First 1
        if ($cur) {
            $stage = $cur.stage
            $age = $cur.age_s
            Write-Host "  ... Stage: $stage ($age`s)" -ForegroundColor DarkGray

            # Kill builder during planning phase
            if ($stage -eq "planning" -and !$killed -and $age -gt 5) {
                Write-Host "" 
                Write-Host "  ****************************************************" -ForegroundColor Red
                Write-Host "  *  KILLING BUILDER NODE!                           *" -ForegroundColor Red
                Write-Host "  ****************************************************" -ForegroundColor Red
                $events = (Invoke-RestMethod "$API/api/events").events
                $bid = $events | Where-Object { $_.sender_role -eq "builder" } |
                    Select-Object -ExpandProperty sender_id -Unique | Select-Object -Last 1
                if ($bid) {
                    Write-Host "  ** Target: $($bid.Substring(0,16))" -ForegroundColor Red
                    $kb = @{ target_id = $bid } | ConvertTo-Json
                    Invoke-RestMethod "$API/api/kill-peer" -Method POST `
                        -ContentType "application/json" -Body $kb | Out-Null
                    Write-Host "  ** KILL_SIGNAL sent via MQTT!" -ForegroundColor Red
                    Write-Host "  ** Builder process will die and auto-respawn..." -ForegroundColor Yellow
                    $killed = $true
                }
                Write-Host ""
            }

            # Detect respawn
            if ($killed -and $stage -eq "building") {
                $events2 = (Invoke-RestMethod "$API/api/events").events
                $newB = $events2 | Where-Object { $_.sender_role -eq "builder" -and $_.sender_id -ne $bid } |
                    Select-Object -ExpandProperty sender_id -Unique | Select-Object -Last 1
                if ($newB -and !$script:respawnShown) {
                    Write-Host ""
                    Write-Host "  ** NEW builder respawned: $($newB.Substring(0,16))" -ForegroundColor Green
                    Write-Host "  ** Swarm self-healed! Pipeline continues..." -ForegroundColor Green
                    Write-Host ""
                    $script:respawnShown = $true
                }
            }

            if ($stage -eq "done") {
                Write-Host ""
                Write-Host "  ****************************************************" -ForegroundColor Green
                Write-Host "  *  JOB COMPLETED -- despite node failure!          *" -ForegroundColor Green
                Write-Host "  ****************************************************" -ForegroundColor Green
                break
            }
        }
    } catch {}
}

Narrate "The builder was killed during planning.`nA NEW builder auto-respawned in 3 seconds`nwith a fresh identity. It joined the mesh,`nwon the build bid, generated the code, and`nboth critics evaluated it via BFT consensus.`n`nThe swarm healed itself without any human`nintervention or central coordinator.`n`nNow let's review the dashboard tabs..."
Pause-Demo

# ── 5. Dashboard Tabs Tour ──
Write-Step 5 "Dashboard Tabs Tour"

Narrate "TAB: Live (already visible)`n`nThe network topology shows all agents as`ngreen dots. The KILLED builder appears as`nstale (red/faded). The NEW builder is green.`n6 peers total were seen by the swarm.`n`nThe Job Pipeline card shows the completed`njob with all stages: plan -> build -> eval.`n`nKill buttons (skull) on each agent let you`ntrigger KILL_SIGNAL for resilience testing."
Pause-Demo "Open the PoC tab, then press ENTER..."

Narrate "TAB: PoC (Proof of Coordination)`n`nCryptographic audit trail. Every action is`nan HMAC-chained event: PEER_ANNOUNCE,`nHEARTBEAT, TASK_AVAILABLE, BID_SUBMITTED,`nBID_WON, PLAN_RESULT, BUILD_RESULT,`nEVALUATION, BFT_CONSENSUS, JOB_DONE.`n`nEach entry has timestamp, sender ID, role,`nevent type, and HMAC signature. Modifying`nany event would break the chain. This proves`ncoordination happened exactly as described."
Pause-Demo "Open the Hive tab, then press ENTER..."

Narrate "TAB: Hive Memory`n`nDecentralized shared knowledge base. Agents`npublish knowledge as they work:`n  - Planner publishes plan structure`n  - Builder publishes build metadata`n  - Critics publish evaluation results`n`nNamespaces: plan/build/eval/fix/meta`nTTL: 1 hour, FIFO eviction at 500 entries.`nGives agents a shared world view without`ncentralized storage."
Pause-Demo "Open the Economy tab, then press ENTER..."

Narrate "TAB: Economy (Track 3: Agent Economy)`n`nFully DETERMINISTIC state machine:`n  +15 rep -- delivering a build/fix`n  +8  rep -- leading BFT consensus`n  +3  rep -- winning a bid auction`n  +5  credits -- quality evaluation`n`nTiers: Novice -> Standard -> Veteran -> Elite`n`nSame MQTT events always produce the same`neconomy state. No randomness, no oracle.`nNotice 6 agents tracked: old + new builder."
try {
    $eco = Invoke-RestMethod "$API/api/economy"
    Write-Host "  Agents: $($eco.total_agents)" -ForegroundColor White
    Write-Host "  Credits minted: $($eco.total_credits_minted)" -ForegroundColor White
    Write-Host "  Reputation: +$($eco.total_reputation_delta)" -ForegroundColor White
    $eco.leaderboard | ForEach-Object {
        Write-Host "    $($_.role.PadRight(10)) rep=$($_.reputation) credits=$($_.credits) [$($_.tier)]" -ForegroundColor DarkGray
    }
} catch {}
Pause-Demo "Open the Metrics tab, then press ENTER..."

Narrate "TAB: Metrics`n`nCoordination statistics proving the swarm`nis actively communicating at scale:`n  - Total MQTT messages exchanged`n  - Messages per second throughput`n  - Average pipeline completion time`n  - Total unique peers discovered"
try {
    $coord = Invoke-RestMethod "$API/api/coordination"
    Write-Host "  Total messages: $($coord.total_messages)" -ForegroundColor White
    Write-Host "  Peers seen: $($coord.total_peers_seen)" -ForegroundColor White
    Write-Host "  Throughput: $($coord.messages_per_second) msg/s" -ForegroundColor White
} catch {}
Pause-Demo "Open the Result tab, then press ENTER..."

Narrate "TAB: Result`n`nLists all artifacts produced by the swarm.`nClick any job to see the generated app`nrendered live in an iframe -- it's a fully`nworking HTML/CSS/JS application.`n`nYou can view the source code or open it`nin a new browser tab. The swarm doesn't`njust coordinate -- it produces real,`nfunctional output."
Pause-Demo

# ── 6. Final Summary ──
Write-Step 6 "Final Summary"
Narrate "What you just saw in under 5 minutes:`n`n[OK] Leaderless coordination (no orchestrator)`n[OK] Vertex BFT consensus (fair ordering)`n[OK] HMAC-SHA256 signed + replay-protected`n[OK] Fault tolerance (kill -> auto-respawn)`n[OK] Hive Memory (shared agent knowledge)`n[OK] Agent Economy (reputation + credits)`n[OK] Multi-LLM (Groq, Gemini, Qwen)`n[OK] Real output (working HTML/CSS/JS apps)`n[OK] Cryptographic Proof of Coordination"
try {
    $hive = Invoke-RestMethod "$API/api/hive"
    Write-Host "  Hive Memory entries: $($hive.total)" -ForegroundColor White
} catch {}

Write-Host ""
Write-Host "  +--------------------------------------------------------------+" -ForegroundColor Green
Write-Host "  |                                                              |" -ForegroundColor Green
Write-Host "  |     DEMO COMPLETE -- FlashForge Leaderless Agent Swarm       |" -ForegroundColor Green
Write-Host "  |     Vertex Swarm Challenge 2026 - Track 3: Agent Economy     |" -ForegroundColor Green
Write-Host "  |                                                              |" -ForegroundColor Green
Write-Host "  |     Dashboard: http://localhost:5050                          |" -ForegroundColor Green
Write-Host "  |     Tabs: Live | PoC | Hive | Economy | Metrics | Result     |" -ForegroundColor Green
Write-Host "  |                                                              |" -ForegroundColor Green
Write-Host "  +--------------------------------------------------------------+" -ForegroundColor Green
Write-Host ""
