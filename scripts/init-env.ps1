if (Test-Path -LiteralPath '.env') {
    $existing = Get-Content -LiteralPath '.env' -Raw
    if ($existing -notmatch '(?m)^GRAFANA_ADMIN_PASSWORD=') {
        $grafanaBytes = [byte[]]::new(24)
        [System.Security.Cryptography.RandomNumberGenerator]::Fill($grafanaBytes)
        $grafanaPassword = -join ($grafanaBytes | ForEach-Object { $_.ToString('x2') })
        Add-Content -LiteralPath '.env' -Value "GRAFANA_ADMIN_PASSWORD=$grafanaPassword" -Encoding utf8NoBOM
        Write-Host 'Added random Grafana development password to existing .env.'
    } else {
        Write-Host '.env already has the required development secrets.'
    }
    exit 0
}

$jwtBytes = [byte[]]::new(32)
$dbBytes = [byte[]]::new(24)
$grafanaBytes = [byte[]]::new(24)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($jwtBytes)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($dbBytes)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($grafanaBytes)
$jwtSecret = -join ($jwtBytes | ForEach-Object { $_.ToString('x2') })
$dbPassword = -join ($dbBytes | ForEach-Object { $_.ToString('x2') })
$grafanaPassword = -join ($grafanaBytes | ForEach-Object { $_.ToString('x2') })
@"
POSTGRES_PASSWORD=$dbPassword
JWT_SECRET=$jwtSecret
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:8080
GRAFANA_ADMIN_PASSWORD=$grafanaPassword
"@ | Set-Content -LiteralPath '.env' -Encoding utf8NoBOM
Write-Host 'Created .env with random development secrets.'
