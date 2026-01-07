# ITD Project: Real-time Sentiment & Churn Analytics

Kafka → Spark → Sentiment (Hashing + Logistic Regression) → MongoDB → FastAPI → React

## Setup

1. Ensure Docker and Docker Compose are installed.
2. Clone the repository and navigate to the project directory.

## Running the Application

1. Start the infrastructure (Kafka, MongoDB):
   ```
   docker-compose up -d kafka mongo
   ```

2. Create Kafka topics:
   ```
   .\create_topics.ps1
   ```

3. In one terminal, run the producer to send sample messages:
   ```
   .\run_producer.ps1
   ```

4. In another terminal, run Spark Streaming to process messages:
   ```
   .\run_spark.ps1
   ```

5. In a third terminal, consume messages to verify:
   ```
   .\consume_messages.ps1
   ```

## Notes

- Commands must be run in single lines to avoid Windows PowerShell parsing issues.
- Run producer and Spark in separate terminals.
- The consumer can be run to check messages in the topic.
