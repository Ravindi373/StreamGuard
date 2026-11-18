# Failure-injection demo (PowerShell): kill the processor mid-stream, restart, prove no data loss.
$ErrorActionPreference = "Stop"

Write-Host "==> starting stack"
docker compose up -d --build
Write-Host "==> letting data flow for 20s"; Start-Sleep 20

Write-Host "==> KILLING the processor (simulator keeps producing)"
docker compose kill processor
Start-Sleep 15

Write-Host "==> restarting the processor"
docker compose start processor
Start-Sleep 20

Write-Host "==> stopping the simulator gracefully (flushes in-flight messages)"
docker compose stop simulator

Write-Host "==> verifying: no gaps in any sensor's sequence"
docker compose run --rm --no-deps --entrypoint streamguard processor verify --settle 10
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
