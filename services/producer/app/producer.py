import os
import json
import time
import uuid
import csv
import hashlib
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


def extract_product_info(text: str) -> tuple[str, str]:
    """
    Extract product_id and product_name from review text.
    
    Strategy:
    - Extract title (text before first colon or sentence)
    - Generate deterministic product_id from title hash
    - Use extracted title as product_name
    
    Returns: (product_id, product_name)
    """
    # Extract title-like portion (before colon or first sentence)
    title = text.split(':')[0].strip() if ':' in text else text.split('.')[0].strip()
    
    # Limit title length for readability
    if len(title) > 80:
        title = title[:80].strip()
    
    # If title is too short, use first 50 chars of text
    if len(title) < 10:
        title = text[:50].strip()
    
    # Generate deterministic product_id from title
    # Use MD5 hash (first 12 chars) to create unique but reproducible IDs
    product_hash = hashlib.md5(title.encode('utf-8')).hexdigest()[:12]
    product_id = f"AMZN-{product_hash.upper()}"
    
    # Clean product_name
    product_name = title.replace('"', '').replace('?', '').strip()
    if not product_name:
        product_name = "Amazon Product"
    
    return product_id, product_name


def make_event(customer_id: str, text: str, channel: str = "app") -> dict:
    """Create event with product_id and product_name extracted from review text."""
    product_id, product_name = extract_product_info(text)
    
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "product_id": product_id,
        "product_name": product_name,
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

    print(f"🔄 Streaming CSV -> Kafka in CONTINUOUS mode")
    print(f"   csv={INPUT_CSV}")
    print(f"   topic={TOPIC}")
    print(f"   bootstrap={BOOTSTRAP}")
    print(f"   Rate: ~5 msgs/sec (200ms delay)")
    print(f"   Mode: INFINITE LOOP (restart container to stop)\n")

    loop_count = 0
    total_sent = 0

    try:
        while True:  # ← INFINITE LOOP for continuous streaming
            loop_count += 1
            batch_count = 0
            
            print(f"📦 Loop #{loop_count} - Starting new batch from CSV...")
            
            for event in iter_events_from_csv(INPUT_CSV):
                # Update timestamp to current time for real-time simulation
                event['timestamp_utc'] = datetime.now(timezone.utc).isoformat()
                
                producer.send(TOPIC, value=event)
                batch_count += 1
                total_sent += 1
                
                # Print every 10th message to reduce log spam
                if batch_count % 10 == 0:
                    print(f"   Sent {batch_count} msgs (Total: {total_sent}) | Product: {event['product_name'][:30]}...")
                
                time.sleep(0.2)  # simulate realtime (5 msgs/sec)
            
            producer.flush()
            print(f"✅ Loop #{loop_count} complete - Sent {batch_count} messages. Starting next loop in 2s...\n")
            time.sleep(2)  # Brief pause between loops
    
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer (Ctrl+C detected)...")
    finally:
        producer.close()
        print(f"✅ Producer stopped. Total sent: {total_sent} messages")



if __name__ == "__main__":
    main()
