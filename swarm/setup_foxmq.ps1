# setup_foxmq.ps1 — Start a 3-node FoxMQ cluster for local swarm development
#
# FoxMQ is a Rust MQTT 5.0 broker powered by Tashi Vertex BFT consensus.
# 3 broker nodes form a BFT cluster. Agents connect to any node (localhost:1883).
# Vertex orders messages before delivery, so every agent sees the EXACT same
# event sequence across all 3 brokers.
#
# Usage (from flashforge/ root):
#   .\swarm\setup_foxmq.ps1
#
# Prerequisites:
#   foxmq.exe must be in the flashforge/ root directory.
#   If missing, download from:
#   https://github.com/tashigit/foxmq/releases/tag/v0.3.1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot   # flashforge/ root

$FoxmqBin  = Join-Path $Root "foxmq.exe"
$FoxmqDir  = Join-Path $Root "foxmq.d"
$KeyFile0  = Join-Path $FoxmqDir "key_0.pem"
$KeyFile1  = Join-Path $FoxmqDir "key_1.pem"
$KeyFile2  = Join-Path $FoxmqDir "key_2.pem"

# ── 1. Check binary ────────────────────────────────────────────────────────────
if (-not (Test-Path $FoxmqBin)) {
    Write-Error "foxmq.exe not found at $FoxmqBin`nDownload from: https://github.com/tashigit/foxmq/releases/tag/v0.3.1"
    exit 1
}

# ── 2. Generate 3-node address book if not present ────────────────────────────
if (-not (Test-Path $KeyFile0)) {
    Write-Host "Generating FoxMQ address book (3-node cluster)..." -ForegroundColor Cyan
    Push-Location $Root
    & $FoxmqBin address-book from-list -O foxmq.d -f 127.0.0.1:19793 127.0.0.1:19794 127.0.0.1:19795
    Pop-Location
    Write-Host "  → Created key_0.pem, key_1.pem, key_2.pem" -ForegroundColor Green
    Write-Host "  → Created address-book.toml (3 nodes)" -ForegroundColor Green
} else {
    Write-Host "FoxMQ address book already present — skipping generation." -ForegroundColor Gray
}

# ── 3. Start 3-node cluster ────────────────────────────────────────────────────
Write-Host ""
Write-Host "Starting 3-node FoxMQ cluster..." -ForegroundColor Cyan
Write-Host "  Node 0: MQTT 1883 | BFT UDP 19793"
Write-Host "  Node 1: MQTT 1884 | BFT UDP 19794"
Write-Host "  Node 2: MQTT 1885 | BFT UDP 19795"
Write-Host "  Auth  : anonymous (--allow-anonymous-login)"
Write-Host ""

Push-Location $Root
# Node 0 — primary (agents connect to 1883 by default)
Start-Process -FilePath $FoxmqBin `
    -ArgumentList "run","--allow-anonymous-login","-f","foxmq.d/key_0.pem","-L","0.0.0.0:1883","-C","0.0.0.0:19793","foxmq.d" `
    -WindowStyle Minimized
Start-Sleep 1
# Node 1
Start-Process -FilePath $FoxmqBin `
    -ArgumentList "run","--allow-anonymous-login","-f","foxmq.d/key_1.pem","-L","0.0.0.0:1884","-C","0.0.0.0:19794","foxmq.d" `
    -WindowStyle Minimized
Start-Sleep 1
# Node 2
Start-Process -FilePath $FoxmqBin `
    -ArgumentList "run","--allow-anonymous-login","-f","foxmq.d/key_2.pem","-L","0.0.0.0:1885","-C","0.0.0.0:19795","foxmq.d" `
    -WindowStyle Minimized
Pop-Location

Write-Host ""
Write-Host "3-node FoxMQ cluster started. Press Ctrl+C to stop." -ForegroundColor Green
