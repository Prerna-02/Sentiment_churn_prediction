# PowerShell script to run Spark Streaming
# Run this from the project root directory

Write-Host "Starting Spark Streaming..."

docker-compose up spark_streaming

Write-Host "Spark Streaming finished."