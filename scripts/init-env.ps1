if (Test-Path -LiteralPath '.env') {
    Write-Host '.env already exists; leaving it unchanged.'
    exit 0
}

$jwtBytes = [byte[]]::new(32)
$dbBytes = [byte[]]::new(24)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($jwtBytes)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($dbBytes)
$jwtSecret = -join ($jwtBytes | ForEach-Object { $_.ToString('x2') })
$dbPassword = -join ($dbBytes | ForEach-Object { $_.ToString('x2') })
@"
POSTGRES_PASSWORD=$dbPassword
JWT_SECRET=$jwtSecret
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:8080
"@ | Set-Content -LiteralPath '.env' -Encoding utf8NoBOM
Write-Host 'Created .env with random development secrets.'
