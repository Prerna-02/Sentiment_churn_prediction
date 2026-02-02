Write-Host "MongoDB container logs (Ctrl+C to stop)..." -ForegroundColor Cyan

docker logs -f mongo
