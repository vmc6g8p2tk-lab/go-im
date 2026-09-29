param([int]$Seconds = 60, [string]$Output)
$deadline = (Get-Date).AddSeconds($Seconds)
'at|name|cpu_percent|memory|pids' | Set-Content -LiteralPath $Output
while ((Get-Date) -lt $deadline) {
    $at = (Get-Date).ToString('o')
    $lines = docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.PIDs}}'
    foreach ($line in $lines) {
        Add-Content -LiteralPath $Output -Value "$at|$line"
    }
    Start-Sleep -Milliseconds 500
}
