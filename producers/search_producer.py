import json
import time
import random
from datetime import datetime, UTC
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "search-events"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(3, 5, 0),
)

SEARCH_QUERIES = [
    "action movies", "hindi comedy", "thriller series",
    "new releases", "top rated", "romance drama",
    "sci fi 2026", "documentary crime", "animation kids",
    "telugu movies", "english series season 2"
]

def generate_search_event():
    return {
        "event_id": f"SE{random.randint(100000, 999999)}",
        "user_id": random.randint(1, 1000),
        "query": random.choice(SEARCH_QUERIES),
        "results_count": random.randint(0, 50),
        "clicked_result_id": random.choice([random.randint(1, 500), None]),
        "timestamp": datetime.now(UTC).isoformat(),
    }

print(f"[SearchProducer] Producing to: {TOPIC}")
try:
    while True:
        event = generate_search_event()
        producer.send(TOPIC, value=event)
        print(f"  Search: User {event['user_id']} | '{event['query']}' | Results: {event['results_count']}")
        time.sleep(0.6)
except KeyboardInterrupt:
    producer.close()