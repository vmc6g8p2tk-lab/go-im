param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$')]
    [string]$GitHubUsername
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$oldModule = 'github.com/VMC6G8P2TK-Lab/go-im'
$newModule = "github.com/$GitHubUsername/go-im"
$goModPath = Join-Path $repoRoot 'go.mod'
$goMod = Get-Content -LiteralPath $goModPath -Raw
$expectedModuleLine = '(?m)^module ' + [regex]::Escape($oldModule) + '\s*$'
if ($goMod -notmatch $expectedModuleLine) {
    throw "Expected module $oldModule in go.mod; inspect the existing module before changing it."
}

$files = @(Get-ChildItem -LiteralPath $repoRoot -Recurse -File -Filter '*.go' | Where-Object {
    $_.FullName -notmatch '[\\/](vendor|\.git)[\\/]'
})
$changes = @()
foreach ($file in $files) {
    $content = Get-Content -LiteralPath $file.FullName -Raw
    if ($content.Contains($oldModule)) { $changes += [pscustomobject]@{ Path = $file.FullName; Content = $content } }
}

$goMod = $goMod.Replace("module $oldModule", "module $newModule")
[System.IO.File]::WriteAllText($goModPath, $goMod, [System.Text.UTF8Encoding]::new($false))
foreach ($change in $changes) {
    [System.IO.File]::WriteAllText($change.Path, $change.Content.Replace($oldModule, $newModule), [System.Text.UTF8Encoding]::new($false))
}
Write-Host "Module changed to $newModule in go.mod and $($changes.Count) Go source files."
Write-Host 'Next: update documentation, run go mod tidy, go test ./..., go vet ./..., go test -race ./..., then inspect git diff.'
