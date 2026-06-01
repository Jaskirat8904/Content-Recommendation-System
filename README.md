# Real-Time Content Recommendation Lakehouse

Production-grade recommendation platform built using Apache Spark, Apache Iceberg, Kafka, Airflow, Redis, and FastAPI.

The platform ingests streaming user interactions, maintains a transactional lakehouse, continuously generates recommendation features, trains collaborative filtering models, and serves low-latency personalized recommendations.

---

## Architecture

[Architecture Diagram]

---

## Key Capabilities

- Real-time event ingestion
- Streaming feature engineering
- Transactional data lake (Iceberg)
- Incremental recommendation generation
- Time-travel analytics
- Recommendation serving API
- Model retraining orchestration
- Redis-backed low latency inference

---

## Data Flow

Watch Events
Search Events
Ratings
Clicks

→ Kafka

→ Spark Structured Streaming

→ Iceberg Bronze

→ Iceberg Silver

→ Feature Store

→ ALS Training

→ Recommendation Store

→ FastAPI + Redis

---

## Why Iceberg

Traditional data lakes struggle with:

- Upserts
- Model reproducibility
- Historical feature retrieval
- Schema evolution

Iceberg provides:

- ACID transactions
- Snapshot isolation
- Time travel
- Partition evolution
- Efficient metadata pruning

These capabilities are critical for recommendation systems where feature reproducibility directly impacts model quality.

---

## Recommendation Pipeline

### Interaction Scoring

Weighted user interactions are transformed into implicit feedback signals.

| Event | Weight |
|---------|---------|
| Watch | 5 |
| Completion >90% | 10 |
| Rating 5 | 8 |
| Search Click | 3 |
| Like | 7 |

### Feature Generation

Spark continuously computes:

- User affinity vectors
- Genre preferences
- Completion distributions
- Recency scores
- Popularity trends

### Model Training

ALS collaborative filtering is retrained on Gold-layer interaction matrices.

Generated artifacts:

- User embeddings
- Content embeddings
- Similarity vectors
- Top-N recommendations

---

## Lakehouse Design

### Bronze

Immutable event storage.

Retention:
365 days

Data Volume:
~10M events/day

### Silver

Validated interaction records.

Operations:

- Deduplication
- Sessionization
- Data quality enforcement
- Feature normalization

### Gold

ML-ready datasets.

Examples:

- user_content_affinity
- content_popularity
- recommendation_features
- user_genre_preferences

---

## Scalability

Designed for:

- 10M+ daily interactions
- 1M+ users
- 100K+ content assets

Horizontal scaling achieved through:

- Kafka partition expansion
- Spark executor autoscaling
- Distributed Iceberg metadata
- Stateless API serving

---

## Performance Optimizations

### Spark

- AQE
- Broadcast joins
- Dynamic partition pruning
- Vectorized Parquet scans

### Iceberg

- Snapshot expiration
- File compaction
- Manifest optimization

### Serving

- Redis recommendation cache
- Precomputed Top-N candidate sets
- API response latency <50ms

---

## Observability

Metrics collected:

- Kafka consumer lag
- Processing throughput
- Recommendation freshness
- Model training duration
- API latency
- Cache hit ratio

Stack:

- Prometheus
- Grafana
- Spark UI

---

## Example Recommendation API

GET /recommendations/U10234

Response:

{
  "user_id": "U10234",
  "recommendations": [
    {
      "content_id": "M543",
      "score": 0.982
    },
    {
      "content_id": "M128",
      "score": 0.961
    }
  ]
}

---

## Repository Structure

...
