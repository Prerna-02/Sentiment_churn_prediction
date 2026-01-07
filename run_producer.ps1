# PowerShell script to run the Kafka producer
# Run this from the project root directory

Write-Host "Running Kafka producer..."

docker-compose up producer

Write-Host "Producer finished."