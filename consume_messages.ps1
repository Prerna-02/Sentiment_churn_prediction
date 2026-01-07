# PowerShell script to consume Kafka messages
# Run this from the project root directory

Write-Host "Consuming messages from Kafka topic 'reviews_raw'..."

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reviews_raw --from-beginning

Write-Host "Consumer finished."