import json
import time
import random
from datetime import datetime, UTC
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "click-events"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(3, 5, 0),
)

CLICK_ACTIONS = ["thumbnail_click", "trailer_play", "add_to_watchlist",
                 "share", "details_view", "remove_from_watchlist"]

def generate_click_event():
    return {
        "event_id": f"CE{random.randint(100000, 999999)}",
        "user_id": random.randint(1, 1000),
        "content_id": random.randint(1, 500),
        "action": random.choice(CLICK_ACTIONS),
        "source_page": random.choice(["home", "search", "genre_page", "trending", "recommended"]),
        "session_id": f"S{random.randint(1000, 9999)}",
        "timestamp": datetime.now(UTC).isoformat(),
    }

print(f"[ClickProducer] Producing to: {TOPIC}")
try:
    while True:
        event = generate_click_event()
        producer.send(TOPIC, value=event)
        print(f"  Click: User {event['user_id']} | {event['action']} | Content {event['content_id']}")
        time.sleep(0.3)
except KeyboardInterrupt:
    producer.close()