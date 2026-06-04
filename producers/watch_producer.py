import json
import time
import random
from datetime import datetime, UTC
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "watch-events"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(3, 5, 0),
)

WATCH_STATUSES = ["started", "paused", "resumed", "completed", "dropped"]

def generate_watch_event():
    duration = random.randint(20, 180)
    watched = random.randint(1, duration)
    return {
        "event_id": f"WE{random.randint(100000, 999999)}",
        "user_id": random.randint(1, 1000),
        "content_id": random.randint(1, 500),
        "watch_status": random.choice(WATCH_STATUSES),
        "watch_duration_mins": watched,
        "total_duration_mins": duration,
        "completion_pct": round((watched / duration) * 100, 2),
        "device": random.choice(["mobile", "tv", "laptop", "tablet"]),
        "timestamp": datetime.now(UTC).isoformat(),
    }

print(f"[WatchProducer] Producing to: {TOPIC}")
try:
    while True:
        event = generate_watch_event()
        producer.send(TOPIC, value=event)
        print(f"  Watch: User {event['user_id']} | Content {event['content_id']} | {event['watch_status']} | {event['completion_pct']}%")
        time.sleep(0.4)
except KeyboardInterrupt:
    producer.close()