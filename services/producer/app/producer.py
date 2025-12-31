import os
import json
import time
import uuid
import csv
from pathlib import Path
import random

from datetime import datetime, timezone

from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

CSV_PATH = os.getenv("CSV_PATH", "/data/amazon_sample.csv")
INPUT_CSV = os.getenv("INPUT_CSV_PATH", CSV_PATH)
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "reviews_raw")


def make_event(customer_id: str, text: str, channel: str = "app") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "text": text,
        "channel": channel,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "metadata": {"source": "amazon_reviews_stream"}
    }

time.sleep(10)
def iter_events_from_csv(csv_path: str):
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"CSV not found inside container: {csv_path}")

    with p.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # expected columns: text, sentiment_label (optional), channel(optional)
            text = (row.get("text") or "").strip()
            if not text:
                continue

            customer_id = row.get("customer_id") or f"C{random.randint(1,999):03d}"
            channel = row.get("channel") or random.choice(["app", "web", "email", "callcenter"])

            yield make_event(customer_id=customer_id, text=text, channel=channel)


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
    )

    print(f"Streaming CSV -> Kafka. csv={INPUT_CSV} topic={TOPIC} bootstrap={BOOTSTRAP}")

    for event in iter_events_from_csv(INPUT_CSV):
        producer.send(TOPIC, value=event)
        print("Sent:", event["event_id"], event["customer_id"], event["channel"])
        time.sleep(0.2)  # simulate realtime

    producer.flush()
    producer.close()
    print("Done.")



if __name__ == "__main__":
    main()
