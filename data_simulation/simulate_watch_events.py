import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# ---------- Load Environment Variables ----------
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_NAME = os.getenv("TOPIC_NAME")

# ---------- Sample Data ----------
user_ids = [f"user_{i}" for i in range(1, 11)]
show_ids = [f"show_{i}" for i in range(101, 121)]
genres = ["Action", "Drama", "Comedy", "Horror", "Sci-Fi", "Documentary"]
device_types = ["mobile", "tv", "tablet", "laptop"]

# ---------- Kafka Producer ----------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------- Event Simulation Function ----------
def generate_event():
    user_id = random.choice(user_ids)
    show_id = random.choice(show_ids)
    genre = random.choice(genres)
    device = random.choice(device_types)
    duration = random.randint(60, 3600)  # seconds watched
    timestamp = datetime.utcnow().isoformat()

    event = {
        "user_id": user_id,
        "show_id": show_id,
        "genre": genre,
        "device_type": device,
        "duration_watched": duration,
        "timestamp": timestamp
    }
    return event

# ---------- Streaming Loop ----------
def stream_events(interval=1.0):
    print(f"🚀 Sending events to Kafka topic '{TOPIC_NAME}' every {interval} second(s)...")
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC_NAME, value=event)
            print("🟢 Sent:", event)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("🛑 Stopped by user.")
    finally:
        producer.close()

# ---------- Run ----------
if __name__ == "__main__":
    stream_events(interval=1)
