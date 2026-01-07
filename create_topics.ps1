# PowerShell script to create Kafka topics
# Run this from the project root directory

Write-Host "Creating Kafka topics..."

# Create reviews_raw topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic reviews_raw --partitions 3 --replication-factor 1

# Create reviews_enriched topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic reviews_enriched --partitions 3 --replication-factor 1

# List topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Write-Host "Topics created successfully."