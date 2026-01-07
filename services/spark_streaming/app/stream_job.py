# services/spark_streaming/app/stream_job.py

import os
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType, DoubleType, IntegerType
)

from pymongo import MongoClient

# ✅ Phase-5 model wrapper (your file: shared/sentiment_model.py)
from shared.sentiment_model import SentimentModel3Class


# -------------------------
# Env / Config
# -------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IN = os.getenv("KAFKA_TOPIC_RAW", "reviews_raw")
TOPIC_OUT = os.getenv("KAFKA_TOPIC_ENRICHED", "reviews_enriched")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "itd")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "reviews_enriched")

# Model path inside Spark container (you will mount it via docker-compose volume)
MODEL_PATH = os.getenv("MODEL_PATH", "/models/sentiment_model_3class.pkl")
MODEL_VERSION = os.getenv("MODEL_VERSION", "hashing_sgd_3class_v1")


# -------------------------
# Load model once (driver-side)
# -------------------------
model = SentimentModel3Class.load(MODEL_PATH)


# -------------------------
# UDF: predict sentiment + confidence
# -------------------------
def predict_sentiment(text: str):
    """
    Returns:
      sentiment_label: "positive" | "negative" | "neutral"
      sentiment_score:  1 | -1 | 0
      confidence: a single number (0..1)
    """
    label, score, _confidence, probs = model.predict_one(text or "")

    # probs gives p_pos and p_neg (where p_neg = 1 - p_pos)
    p_pos = float(probs.get("p_pos", 0.5))
    p_neg = float(probs.get("p_neg", 0.5))

    # One-number confidence:
    # - if positive => p_pos
    # - if negative => p_neg
    # - if neutral  => "how close to 0.5" (highest at 0.5)
    if label == "positive":
        conf = p_pos
    elif label == "negative":
        conf = p_neg
    else:
        # peaks at 1 when p_pos=0.5; goes down as it moves away
        conf = 1.0 - (abs(p_pos - 0.5) * 2.0)

    return label, int(score), float(conf), MODEL_VERSION


sentiment_udf = udf(
    predict_sentiment,
    StructType([
        StructField("sentiment_label", StringType(), True),
        StructField("sentiment_score", IntegerType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("model_version", StringType(), True),
    ])
)


# -------------------------
# Mongo writer (foreachBatch)
# -------------------------
def write_to_mongo(batch_df, batch_id: int):
    rows = batch_df.toJSON().collect()
    if not rows:
        return

    docs = [json.loads(r) for r in rows]

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    db[MONGO_COLLECTION].insert_many(docs)
    client.close()


def main():
    spark = (
        SparkSession.builder
        .appName("ITD-Spark-Streaming-Phase5")
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

    # ✅ Phase 5 inference (3-class)
    enriched = (
        parsed
        .withColumn("pred", sentiment_udf(col("text")))
        .withColumn("sentiment_label", col("pred.sentiment_label"))
        .withColumn("sentiment_score", col("pred.sentiment_score"))
        .withColumn("confidence", col("pred.confidence"))
        .withColumn("model_version", col("pred.model_version"))
        .drop("pred")
    )

    # ---- Write to MongoDB
    mongo_query = (
        enriched.writeStream
        .foreachBatch(write_to_mongo)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/mongo")
        .start()
    )

    # ---- Write to Kafka (enriched topic)
    kafka_out = enriched.select(
        to_json(struct(*[col(c) for c in enriched.columns])).alias("value")
    )

    kafka_query = (
        kafka_out.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_OUT)
        .option("checkpointLocation", "/tmp/checkpoints/kafka")
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
