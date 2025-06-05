import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic
from dotenv import load_dotenv
import os

# ---------- Load Environment Variables ----------
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
#KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_NAME = os.getenv("TOPIC_NAME")

# ---------- Sample Data ----------
user_ids = [f"user_{i}" for i in range(1, 11)]
show_ids = [f"show_{i}" for i in range(101, 121)]
genres = ["Action", "Drama", "Comedy", "Horror", "Sci-Fi", "Documentary"]
device_types = ["mobile", "tv", "tablet", "laptop"]

# ---------- Create Kafka Topic if it doesn't exist ----------
# This function checks if the topic exists and creates it if not.
# It is useful for ensuring the topic is ready before sending messages.

def create_topic_if_not_exists(bootstrap_servers, topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✅ Topic '{topic_name}' created.")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Topic '{topic_name}' already exists.")
    finally:
        admin_client.close()


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
def stream_events(interval=0.1):
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
# ✅ Create topic BEFORE using it
    create_topic_if_not_exists("localhost:9092", "watch_events")
    stream_events(interval=0.1)
