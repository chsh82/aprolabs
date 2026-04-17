$repo = "C:\Users\aproa\aprolabs"
Set-Location $repo

Write-Host "Watching for changes... (auto-push every 10s if changed)" -ForegroundColor Cyan

while ($true) {
    Start-Sleep 10
    $status = git status --porcelain 2>$null
    if ($status) {
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
        git add -A
        git commit -m "auto: $timestamp" 2>$null
        git push origin main
        Write-Host "[$timestamp] pushed" -ForegroundColor Green
    }
}
