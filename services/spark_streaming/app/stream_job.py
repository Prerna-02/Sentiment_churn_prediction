# services/spark_streaming/app/stream_job.py
# Phase 6: Spark calls model_service HTTP API for inference

import os
import json
import requests
from typing import List, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType, DoubleType, IntegerType
)

from pymongo import MongoClient


# -------------------------
# Env / Config
# -------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IN = os.getenv("KAFKA_TOPIC_RAW", "reviews_raw")
TOPIC_OUT = os.getenv("KAFKA_TOPIC_ENRICHED", "reviews_enriched")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "itd")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "reviews_enriched")

# Phase 6: model_service HTTP endpoint
MODEL_SERVICE_URL = os.getenv("MODEL_SERVICE_URL", "http://model_service:8000")


# -------------------------
# Helper: call model_service for predictions
# -------------------------
def get_predictions(rows: List[Dict]) -> List[Dict]:
    """
    Call model_service /predict for each row and attach predictions.
    Uses a session for connection pooling across the batch.
    """
    session = requests.Session()
    enriched = []
    
    for row in rows:
        text = row.get("text", "")
        payload = {
            "text": text,
            "event_id": row.get("event_id"),
            "customer_id": row.get("customer_id")
        }
        
        try:
            resp = session.post(
                f"{MODEL_SERVICE_URL}/predict",
                json=payload,
                timeout=5
            )
            resp.raise_for_status()
            pred = resp.json()
            
            # Merge original row with predictions
            enriched_row = {**row}
            enriched_row["sentiment_label"] = pred["sentiment_label"]
            enriched_row["sentiment_score"] = pred["sentiment_score"]
            enriched_row["confidence"] = pred["confidence"]
            enriched_row["model_version"] = pred["model_version"]
            enriched.append(enriched_row)
            
        except Exception as e:
            # Fallback: attach neutral sentiment on error
            print(f"❌ Prediction failed for event {row.get('event_id')}: {e}")
            enriched_row = {**row}
            enriched_row["sentiment_label"] = "neutral"
            enriched_row["sentiment_score"] = 0
            enriched_row["confidence"] = 0.0
            enriched_row["model_version"] = "error"
            enriched.append(enriched_row)
    
    session.close()
    return enriched


# -------------------------
# Combined enrichment + dual writer (foreachBatch)
# -------------------------
def enrich_and_write_batch(batch_df: DataFrame, batch_id: int):
    """
    For each micro-batch:
    1. Collect rows as dicts
    2. Call model_service to get predictions (one API call per row)
    3. Write enriched docs to MongoDB
    4. Write enriched messages to Kafka
    """
    rows = batch_df.toJSON().collect()
    if not rows:
        return
    
    # Parse rows
    parsed_rows = [json.loads(r) for r in rows]
    
    # Call model_service for predictions
    enriched_docs = get_predictions(parsed_rows)
    
    if not enriched_docs:
        return
    
    # Write to MongoDB
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        db[MONGO_COLLECTION].insert_many(enriched_docs)
        client.close()
        print(f"✅ Batch {batch_id}: wrote {len(enriched_docs)} enriched docs to MongoDB")
    except Exception as e:
        print(f"❌ Batch {batch_id} MongoDB write failed: {e}")
    
    # Write to Kafka enriched topic
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Remove MongoDB _id (ObjectId) before sending to Kafka
        for doc in enriched_docs:
            kafka_doc = {k: v for k, v in doc.items() if k != '_id'}
            producer.send(TOPIC_OUT, value=kafka_doc)
        
        producer.flush()
        producer.close()
        print(f"✅ Batch {batch_id}: wrote {len(enriched_docs)} enriched messages to Kafka")
    except Exception as e:
        print(f"❌ Batch {batch_id} Kafka write failed: {e}")


def main():
    spark = (
        SparkSession.builder
        .appName("ITD-Spark-Streaming-Phase6")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Incoming JSON schema from producer
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("metadata", MapType(StringType(), StringType()), True),
    ])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), schema).alias("j"))
        .select("j.*")
    )

    # ---- Phase 6: Single stream → enrich via model_service → write to Mongo + Kafka
    query = (
        parsed.writeStream
        .foreachBatch(enrich_and_write_batch)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/phase6")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
