Write-Host "Kafka: consuming 5 messages from topic reviews_enriched (enriched output)..." -ForegroundColor Cyan

docker exec -i kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reviews_enriched --from-beginning --max-messages 5

Write-Host "`nDone." -ForegroundColor Green
