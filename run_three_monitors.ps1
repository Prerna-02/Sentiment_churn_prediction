# Run Spark, MongoDB, and Kafka monitors in 3 separate terminals.
# Open 3 VS Code terminals, then run one command per terminal:
#
# Terminal 1 - Spark:
#   .\watch_spark_logs.ps1
#
# Terminal 2 - MongoDB:
#   .\watch_mongo_logs.ps1
#
# Terminal 3 - Kafka:
#   .\watch_kafka_logs.ps1

Write-Host "Run these in 3 separate terminal tabs:" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Terminal 1 (Spark):    .\watch_spark_logs.ps1" -ForegroundColor Yellow
Write-Host "  Terminal 2 (MongoDB):  .\watch_mongo_logs.ps1" -ForegroundColor Yellow
Write-Host "  Terminal 3 (Kafka):    .\watch_kafka_logs.ps1" -ForegroundColor Yellow
Write-Host ""
Write-Host "Dashboard: http://localhost:3000" -ForegroundColor Green
