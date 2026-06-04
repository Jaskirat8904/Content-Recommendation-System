import json
import os
import random
from datetime import datetime

import redis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer

from api.schemas import WatchEvent, RatingEvent, RecommendationResponse

app = FastAPI(title="Content Recommendation API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

cache = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


@app.get("/health")
def health():
    return {"status": "ok", "service": "content-recommendation-api"}


@app.post("/events/watch")
def ingest_watch(event: WatchEvent):
    payload = event.dict()
    payload["event_id"] = f"WE{random.randint(100000, 999999)}"
    payload["completion_pct"] = round(
        (event.watch_duration_mins / max(event.total_duration_mins, 1)) * 100, 2
    )
    producer.send("watch-events", value=payload)
    cache.setex(
        f"last_watch:{event.user_id}",
        3600,
        json.dumps({"content_id": event.content_id, "status": event.watch_status}),
    )
    return {"status": "accepted", "event_id": payload["event_id"]}


@app.post("/events/rating")
def ingest_rating(event: RatingEvent):
    payload = event.dict()
    payload["event_id"] = f"RE{random.randint(100000, 999999)}"
    producer.send("rating-events", value=payload)
    return {"status": "accepted", "event_id": payload["event_id"]}


@app.get("/recommend/{user_id}", response_model=RecommendationResponse)
def get_recommendations(user_id: int, top_n: int = 10):
    cached = cache.get(f"recs:{user_id}")
    if cached:
        return {
            "user_id": user_id,
            "recommendations": json.loads(cached),
            "source": "cache",
        }
    fallback = [
        {"content_id": random.randint(1, 500), "score": round(random.uniform(0.5, 1.0), 3)}
        for _ in range(top_n)
    ]
    return {
        "user_id": user_id,
        "recommendations": fallback,
        "source": "fallback_popular",
    }


@app.post("/recommend/cache/{user_id}")
def cache_recommendations(user_id: int, recs: list):
    cache.setex(f"recs:{user_id}", 86400, json.dumps(recs))
    return {"status": "cached", "user_id": user_id, "count": len(recs)}


@app.get("/content/trending")
def get_trending():
    keys = cache.keys("trending:*")
    return {"trending": [cache.get(k) for k in keys[:20]]}