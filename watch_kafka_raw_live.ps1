Write-Host "Kafka: LIVE stream from topic reviews_raw (Ctrl+C to stop)..." -ForegroundColor Cyan

docker exec -i kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reviews_raw --from-beginning
