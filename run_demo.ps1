chcp 65001 | Out-Null
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

Write-Host "=== FlashForge Agent Swarm - Vertex Swarm Challenge 2026 ===" -ForegroundColor Cyan
Write-Host "    Warm-Up: Stateful Handshake Proof" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press ENTER to start..." -ForegroundColor Yellow
Read-Host

& "$PSScriptRoot\.venv\Scripts\python.exe" -X utf8 "$PSScriptRoot\swarm\warmup_demo.py"

Write-Host ""
Write-Host "Demo complete. Press ENTER to close." -ForegroundColor Green
Read-Host
