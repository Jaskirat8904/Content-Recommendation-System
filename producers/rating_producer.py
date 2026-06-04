import json
import time
import random
from datetime import datetime, UTC
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "rating-events"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(3, 5, 0),
)

def generate_rating_event():
    return {
        "event_id": f"RE{random.randint(100000, 999999)}",
        "user_id": random.randint(1, 1000),
        "content_id": random.randint(1, 500),
        "rating": round(random.uniform(1.0, 5.0), 1),
        "review_text": random.choice([
            "Amazing watch!", "Totally boring.", "Worth it!",
            "Average plot.", "Loved the cast.", "Would recommend.",
            None, None
        ]),
        "timestamp": datetime.now(UTC).isoformat(),
    }

print(f"[RatingProducer] Producing to: {TOPIC}")
try:
    while True:
        event = generate_rating_event()
        producer.send(TOPIC, value=event)
        print(f"  Rating: User {event['user_id']} | Content {event['content_id']} | ⭐ {event['rating']}")
        time.sleep(0.8)
except KeyboardInterrupt:
    producer.close()