param([switch]$Multi)

$ErrorActionPreference = 'Stop'
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker CLI is not installed. Install Docker Desktop and start its engine first.'
}
if (-not (Test-Path -LiteralPath '.env')) {
    throw 'Missing .env. Run scripts/init-env.ps1 first.'
}

$composeArgs = @('compose', '--env-file', '.env', '-f', 'deployments/compose.yaml')
if ($Multi) { $composeArgs += @('--profile', 'multi') }

& docker @composeArgs config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose configuration validation failed.' }
& docker @composeArgs up --build -d
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose startup failed.' }

function Wait-HTTP([string]$Url) {
    $deadline = (Get-Date).AddMinutes(2)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 3
            if ($response.StatusCode -eq 200) { return }
        } catch { }
        Start-Sleep -Seconds 2
    }
    throw "Health check did not become ready: $Url"
}

Wait-HTTP 'http://127.0.0.1:8080/healthz'
Wait-HTTP 'http://127.0.0.1:8080/metrics'
Wait-HTTP 'http://127.0.0.1:9090/-/ready'
Wait-HTTP 'http://127.0.0.1:3000/api/health'
if ($Multi) { Wait-HTTP 'http://127.0.0.1:8081/healthz' }

& docker @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic im.message.created.v1
if ($LASTEXITCODE -ne 0) { throw 'Kafka topic inspection failed.' }
& docker @composeArgs ps
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose status check failed.' }

Write-Host 'Compose services, HTTP health, metrics, and Kafka topic are reachable.'
Write-Host 'Run the end-to-end tests with INTEGRATION_BASE_URL and DATABASE_URL set as shown in README.md.'
