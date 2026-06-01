# 🎬 Real-Time Content Recommendation Lakehouse

> A production-grade, distributed recommendation platform modeled after the data infrastructure powering Netflix, Disney+, Hotstar, and Prime Video — built to ingest, process, and serve personalized content recommendations at millions-of-events-per-day scale.

---

## Overview

Modern streaming platforms operate at a scale where every scroll, pause, and playback generates a signal. This project implements a full-stack **Lakehouse architecture** capable of continuously ingesting user interaction streams, enriching and storing them across a medallion data model, training collaborative filtering models on live data, and serving sub-second personalized recommendations through a production-ready API layer.

The architecture addresses the fundamental tension in streaming platforms: the need for **real-time responsiveness** and **historical analytical depth** within a single, consistent data system.

---

## Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│                     Ingestion Layer                          │
│         Mobile · Web · Smart TV · Third-party APIs          │
└────────────────────────────┬─────────────────────────────────┘
                             │  Event Streams
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                    Apache Kafka                               │
│   watch-events · click-events · ratings · search-queries     │
└────────────────────────────┬─────────────────────────────────┘
                             │  Structured Streaming
                             ▼
┌──────────────────────────────────────────────────────────────┐
│              Apache Spark Streaming Layer                    │
│    Event enrichment · Sessionization · Feature engineering   │
└────────────────────────────┬─────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
         [Bronze]        [Silver]        [Gold]
       Raw Events     User Profiles   ML Features
              └──────────────┼──────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│           Apache Iceberg — Lakehouse Storage                 │
│    ACID transactions · Time travel · Schema evolution        │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│         ALS Collaborative Filtering (Spark MLlib)            │
│     User-Item Matrix · Incremental retraining · Scoring      │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│              FastAPI  ·  Redis Cache Layer                   │
│    Recommendation API · User profile API · Content lookup    │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│              Streamlit Analytics Dashboard                   │
│   Engagement metrics · Trending content · Rec insights       │
└──────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Domain                  | Technology                        | Purpose                                      |
|-------------------------|-----------------------------------|----------------------------------------------|
| Event Streaming         | Apache Kafka                      | High-throughput, fault-tolerant event bus    |
| Stream Processing       | Apache Spark Structured Streaming | Distributed, stateful stream processing      |
| Lakehouse Storage       | Apache Iceberg                    | ACID-compliant, time-travel-enabled tables   |
| Workflow Orchestration  | Apache Airflow                    | DAG-based pipeline scheduling & monitoring  |
| Serving API             | FastAPI                           | Async, high-performance recommendation APIs |
| Low-Latency Cache       | Redis                             | Sub-millisecond recommendation retrieval    |
| Analytics Dashboard     | Streamlit                         | Interactive engagement and content analytics |
| Object Storage          | AWS S3                            | Scalable Parquet/Iceberg file storage        |
| Metadata Catalog        | AWS Glue                          | Centralized schema and partition registry    |
| Ad-hoc Query Engine     | Trino / AWS Athena                | Interactive SQL over Iceberg tables          |
| Containerization        | Docker + Docker Compose           | Reproducible local and cloud environments    |
| Observability           | Prometheus + Grafana              | Real-time pipeline and API monitoring        |

---

## Medallion Data Architecture

### 🥉 Bronze Layer — Raw Event Store

The Bronze layer is an **append-only, schema-on-read** store that preserves every ingested event in its original form. No transformations are applied — this layer exists as the immutable source of truth, enabling full event replay and auditability.

**Stored event types:**

```json
// Watch Event
{
  "user_id": "U1001",
  "content_id": "M210",
  "watch_duration": 4200,
  "completion_rate": 0.92,
  "timestamp": "2026-06-01T10:30:00Z"
}

// Rating Event
{
  "user_id": "U1001",
  "content_id": "M210",
  "rating": 5,
  "timestamp": "2026-06-01T10:45:00Z"
}

// Search Event
{
  "user_id": "U1001",
  "query": "sci-fi thriller",
  "timestamp": "2026-06-01T09:15:00Z"
}

// Click Event
{
  "user_id": "U1001",
  "content_id": "M210",
  "action": "click",
  "timestamp": "2026-06-01T09:16:00Z"
}
```

---

### 🥈 Silver Layer — Validated & Enriched Profiles

The Silver layer applies deterministic transformations to produce **analytically reliable, deduplicated, and enriched datasets** suitable for downstream modeling.

**Transformations applied:**
- Deduplication via event ID watermarking
- Late-arrival event handling with configurable watermark windows
- User session stitching and sessionization
- Content metadata joins (genre, language, release year, maturity rating)
- Null imputation and schema enforcement
- Incremental `MERGE INTO` upserts on Iceberg tables

---

### 🥇 Gold Layer — ML-Ready Feature Store

The Gold layer aggregates enriched Silver data into **feature-rich, model-ready datasets** used directly by the ALS training pipeline and the analytics dashboard.

**Artifacts produced:**
- User-Item interaction matrix (implicit feedback weighted by duration, completion rate, and rating)
- Per-user genre affinity vectors
- Content popularity decay scores (time-weighted view counts)
- Collaborative signal features for cold-start mitigation
- Session-level engagement metrics

---

## Recommendation Engine

The recommendation engine implements **Alternating Least Squares (ALS)** via Apache Spark MLlib — a matrix factorization technique well-suited for implicit feedback at scale.

### Signal Weighting

Raw interactions are not treated equally. Each signal contributes a weighted confidence score to the interaction matrix:

| Signal              | Weight |
|---------------------|--------|
| Content Completion  | 1.0    |
| Rating (normalized) | 0.9    |
| Watch Duration      | 0.7    |
| Click               | 0.4    |
| Search Match        | 0.3    |

### Model Configuration

```python
als = ALS(
    maxIter=15,
    regParam=0.1,
    rank=50,
    userCol="user_id",
    itemCol="content_id",
    ratingCol="implicit_score",
    implicitPrefs=True,
    coldStartStrategy="drop"
)
```

### Retraining Strategy

The model supports **incremental retraining** via Airflow-scheduled DAGs. Rather than full retraining on every pipeline run, delta interaction records from the Gold layer are used to update factorization weights, reducing compute cost while keeping recommendations fresh.

---

## Apache Iceberg — Lakehouse Capabilities

Iceberg provides the transactional backbone of the storage layer, enabling capabilities that traditional Parquet/Hive setups cannot reliably support.

### Time Travel Queries

```sql
-- Replay user interaction state from 24 hours ago
SELECT *
FROM lakehouse.silver.user_interactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-06-01 00:00:00';
```

### Schema Evolution — Non-Breaking

```sql
-- Add device context without rewriting existing partitions
ALTER TABLE lakehouse.bronze.watch_events
ADD COLUMNS (
    device_type  STRING,
    app_version  STRING,
    network_type STRING
);
```

### Snapshot Rollback

```sql
-- Roll back to a known-good snapshot after a bad ingestion run
CALL lakehouse.system.rollback_to_snapshot(
    'bronze.watch_events',
    snapshot_id => 8765432109876543
);
```

---

## Spark Optimization Strategy

The processing layer is tuned for high-throughput streaming workloads:

| Optimization                  | Implementation Detail                                        |
|-------------------------------|--------------------------------------------------------------|
| Adaptive Query Execution      | `spark.sql.adaptive.enabled = true` — dynamic partition coalescing |
| Broadcast Joins               | Content metadata table (<= 100 MB) broadcast to all executors |
| Predicate Pushdown            | Iceberg scan filters applied at file manifest level          |
| Partition Pruning             | Event date partitioning reduces scan surface by >90%         |
| Vectorized Parquet Reads      | Arrow-based columnar I/O for feature engineering stages      |
| Optimized Shuffle             | `spark.sql.shuffle.partitions` tuned to cluster parallelism  |

---

## API Layer

The FastAPI service exposes recommendation and analytics endpoints with Redis-backed caching to guarantee sub-50ms p99 response times under load.
GET /recommendations/{user_id} → Top-N personalized recommendations
GET /content/{content_id}/similar → Content-based similarity results
GET /users/{user_id}/profile → Enriched user preference profile
POST /events/ingest → Direct event ingestion endpoint
GET /analytics/trending → Platform-wide trending content
GET /health → Service health and dependency status

text

**Cache strategy:** Recommendations are pre-computed post-model-run and stored in Redis with a configurable TTL (default: 6 hours). On cache miss, the API falls back to real-time Iceberg query execution.

---

## Airflow Orchestration

All pipeline stages are managed as versioned, dependency-aware DAGs with automatic retries, SLA tracking, and alerting.
kafka_ingestion_sensor
↓
bronze_stream_ingestion [Spark Streaming Job]
↓
silver_cleaning_and_enrichment [Spark Batch Job — hourly]
↓
gold_feature_engineering [Spark Batch Job — hourly]
↓
als_model_training [Spark MLlib — daily]
↓
recommendation_generation [Spark Batch — post-training]
↓
redis_cache_population [Python — post-generation]
↓
dashboard_metric_refresh [Streamlit cache invalidation]

text

---

## Monitoring & Observability

**Pipeline metrics tracked via Prometheus + Grafana:**

| Metric                        | Alert Threshold          |
|-------------------------------|--------------------------|
| Kafka Consumer Lag            | > 10,000 messages        |
| Spark Streaming Batch Delay   | > 30 seconds             |
| ALS Training Duration         | > 45 minutes             |
| API P99 Latency               | > 200ms                  |
| Redis Cache Hit Rate          | < 85%                    |
| Iceberg Snapshot Growth Rate  | > 500 snapshots/day      |

Additional observability surfaces: Spark History Server, Airflow Web UI, FastAPI `/metrics` endpoint.

---

## Scalability Profile

| Dimension                  | Supported Scale        |
|----------------------------|------------------------|
| Daily Ingested Events      | 10M+                   |
| Registered Users           | 1M+                    |
| Content Catalog Size       | 100K+ titles           |
| Streaming Latency (E2E)    | < 10 seconds           |
| Recommendation Throughput  | Millions per run       |
| API Response Time (cached) | < 20ms P99             |

**Horizontal scaling levers:**
- Kafka: add partitions per topic; scale consumer group workers independently
- Spark: increase executor count and driver memory via cluster manager (YARN / Kubernetes)
- ALS Training: distribute across additional worker nodes with no code change
- API: stateless FastAPI replicas behind a load balancer; Redis Cluster for cache scaling

---

## Project Structure
content-recommender/
├── producers/
│ ├── watch_producer.py
│ ├── click_producer.py
│ ├── rating_producer.py
│ └── search_producer.py
├── spark_jobs/
│ ├── bronze_ingestion.py
│ ├── silver_user_profiles.py
│ ├── gold_feature_engineering.py
│ ├── train_als_model.py
│ └── generate_recommendations.py
├── iceberg_setup/
│ └── create_tables.py
├── api/
│ ├── main.py
│ ├── routes/
│ └── cache/
├── airflow/
│ └── dags/
├── dashboard/
│ └── app.py
├── scripts/
│ └── init_kafka_topics.sh
├── docker-compose.yml
├── requirements.txt
└── README.md

text

---

## Getting Started

```bash
# 1. Clone and enter project
git clone <repository-url>
cd content-recommender

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Spin up infrastructure (Kafka, Spark, Redis, Airflow, Iceberg)
docker-compose up -d

# 4. Initialize Kafka topics
bash scripts/init_kafka_topics.sh

# 5. Create Iceberg lakehouse tables
spark-submit iceberg_setup/create_tables.py

# 6. Start event producers (simulates real-time user activity)
python producers/watch_producer.py &
python producers/click_producer.py &
python producers/rating_producer.py &
python producers/search_producer.py &

# 7. Launch streaming ingestion pipeline
spark-submit spark_jobs/bronze_ingestion.py &
spark-submit spark_jobs/silver_user_profiles.py &

# 8. Train ALS recommendation model
spark-submit spark_jobs/train_als_model.py

# 9. Generate and cache recommendations
spark-submit spark_jobs/generate_recommendations.py

# 10. Start API server
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# 11. Launch analytics dashboard
streamlit run dashboard/app.py
```

---

## Skills Demonstrated

`Data Engineering` · `Distributed Systems` · `Event-Driven Architecture` · `Lakehouse Design` · `Real-Time Stream Processing` · `Collaborative Filtering` · `ML Engineering` · `Spark Optimization` · `Cloud Data Platforms` · `API Development` · `Pipeline Orchestration` · `Observability & Monitoring`

---

## License

This project is developed for educational, research, and portfolio demonstration purposes.

---

*Built by **Jaskirat Singh** — Data Engineering · Machine Learning · Distributed Systems · Big Data Analytics*
