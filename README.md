# Real-Time Content Recommendation Lakehouse

<p align="center">
  <strong>Enterprise-Scale Streaming Recommendation Platform</strong><br>
  Built using Apache Kafka, Apache Spark, Apache Iceberg, Airflow, FastAPI, Redis, and AWS
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka" />
  <img src="https://img.shields.io/badge/Apache-Spark-E25A1C?logo=apachespark" />
  <img src="https://img.shields.io/badge/Apache-Iceberg-1F70C1" />
  <img src="https://img.shields.io/badge/FastAPI-009688?logo=fastapi" />
  <img src="https://img.shields.io/badge/Redis-DC382D?logo=redis" />
  <img src="https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow" />
  <img src="https://img.shields.io/badge/AWS-S3-orange?logo=amazonaws" />
</p>

---

## Executive Summary

Modern streaming platforms generate billions of behavioral signals daily. Every click, search, watch event, rating, and content interaction contributes to a continuously evolving understanding of user preferences.

This project implements a production-style **Lakehouse-based Recommendation Platform** capable of:

- Real-time event ingestion
- Distributed stream processing
- Medallion architecture data modeling
- ACID-compliant data lake storage
- Collaborative filtering model training
- Low-latency recommendation serving
- Analytics and operational monitoring

The system demonstrates how large-scale platforms such as Netflix, Disney+, Prime Video, and Hotstar architect recommendation pipelines that combine real-time responsiveness with historical analytical depth.

---

# Architecture Overview

```text
┌───────────────────────────────────────────────────────────────┐
│                     Client Applications                       │
│          Web • Mobile • Smart TV • Partner APIs              │
└──────────────────────────────┬────────────────────────────────┘
                               │
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                         Apache Kafka                          │
│    Watch Events • Click Events • Ratings • Searches          │
└──────────────────────────────┬────────────────────────────────┘
                               │
                               ▼
┌───────────────────────────────────────────────────────────────┐
│              Apache Spark Structured Streaming               │
│     Event Validation • Enrichment • Sessionization           │
└──────────────────────────────┬────────────────────────────────┘
                               │
                 ┌─────────────┼─────────────┐
                 ▼             ▼             ▼
            Bronze         Silver         Gold
           Raw Data      Curated Data   ML Features
                 └─────────────┼─────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                    Apache Iceberg Lakehouse                   │
│        ACID Transactions • Time Travel • Snapshots           │
└──────────────────────────────┬────────────────────────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────┐
│             Spark MLlib Recommendation Engine                │
│        ALS Collaborative Filtering Model Training           │
└──────────────────────────────┬────────────────────────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                   FastAPI Recommendation API                 │
│                  Redis Recommendation Cache                  │
└──────────────────────────────┬────────────────────────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                 Streamlit Analytics Dashboard                │
└───────────────────────────────────────────────────────────────┘
```

---

# Business Problem

Traditional recommendation systems often struggle to balance:

- Real-time personalization
- Historical behavioral analysis
- Scalability
- Cost efficiency
- Data consistency

This platform addresses these challenges by implementing a modern Lakehouse architecture that enables both streaming and batch workloads on a unified storage layer.

---

# Core Features

### Real-Time Event Processing

Continuously ingests:

- Watch events
- Clickstream activity
- Ratings
- Search behavior
- Content interactions

using Apache Kafka and Spark Structured Streaming.

### Lakehouse Data Architecture

Implements a complete Medallion architecture:

- Bronze Layer → Raw immutable events
- Silver Layer → Cleaned and enriched datasets
- Gold Layer → ML-ready feature store

using Apache Iceberg tables.

### Recommendation Engine

Generates personalized recommendations using:

- Implicit feedback modeling
- Matrix factorization
- Collaborative filtering
- ALS (Alternating Least Squares)

via Spark MLlib.

### Recommendation Serving Layer

Provides:

- Personalized recommendations
- Similar-content retrieval
- User profile insights
- Trending content analytics

through FastAPI endpoints with Redis acceleration.

### Operational Monitoring

Tracks:

- Consumer lag
- Pipeline throughput
- Training duration
- API latency
- Cache performance
- Storage growth

using Prometheus and Grafana.

---

# Technology Stack

| Layer | Technologies |
|---------|-------------|
| Event Streaming | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Lakehouse Storage | Apache Iceberg |
| Workflow Orchestration | Apache Airflow |
| Machine Learning | Spark MLlib ALS |
| API Layer | FastAPI |
| Caching | Redis |
| Dashboard | Streamlit |
| Object Storage | AWS S3 |
| Metadata Catalog | AWS Glue |
| Query Engine | Trino / AWS Athena |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker & Docker Compose |

---

# Medallion Data Model

## Bronze Layer

Stores immutable raw events exactly as received from producers.

### Responsibilities

- Event persistence
- Auditability
- Replay capability
- Source-of-truth storage

### Example Events

```json
{
  "user_id": "U1001",
  "content_id": "M210",
  "watch_duration": 4200,
  "completion_rate": 0.92,
  "timestamp": "2026-06-01T10:30:00Z"
}
```

---

## Silver Layer

Applies deterministic transformations to create trusted datasets.

### Transformations

- Deduplication
- Data quality validation
- Schema enforcement
- Sessionization
- Content metadata enrichment
- Late-arriving event handling

### Outputs

- User interaction tables
- Session datasets
- Enriched content metadata
- User behavioral profiles

---

## Gold Layer

Creates ML-ready features for recommendation training.

### Feature Engineering

- User-item interaction matrix
- Genre affinity vectors
- Content popularity scores
- Engagement metrics
- Collaborative filtering signals
- Cold-start support features

---

# Recommendation Engine

The recommendation layer uses Apache Spark MLlib's ALS implementation to learn latent relationships between users and content.

## Interaction Weighting

| Signal | Weight |
|----------|---------|
| Completion Rate | 1.0 |
| Rating | 0.9 |
| Watch Duration | 0.7 |
| Click | 0.4 |
| Search Match | 0.3 |

---

## Model Configuration

```python
als = ALS(
    rank=50,
    maxIter=15,
    regParam=0.1,
    implicitPrefs=True,
    userCol="user_id",
    itemCol="content_id",
    ratingCol="implicit_score",
    coldStartStrategy="drop"
)
```

---

## Training Strategy

Instead of expensive full retraining cycles, the platform supports incremental retraining using newly generated interaction signals from the Gold layer.

Benefits:

- Reduced compute cost
- Faster refresh cycles
- More relevant recommendations
- Improved operational efficiency

---

# Apache Iceberg Capabilities

The Lakehouse layer leverages Apache Iceberg to provide enterprise-grade data management features.

## ACID Transactions

Guarantees consistency across concurrent reads and writes.

## Time Travel

```sql
SELECT *
FROM lakehouse.silver.user_interactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-06-01 00:00:00';
```

## Schema Evolution

```sql
ALTER TABLE bronze.watch_events
ADD COLUMNS (
    device_type STRING,
    app_version STRING
);
```

## Snapshot Rollback

```sql
CALL system.rollback_to_snapshot(
    'bronze.watch_events',
    snapshot_id => 8765432109876543
);
```

---

# API Layer

The recommendation service is exposed through FastAPI.

## Endpoints

### Get Recommendations

```http
GET /recommendations/{user_id}
```

Returns personalized Top-N recommendations.

### Similar Content

```http
GET /content/{content_id}/similar
```

Returns content similarity results.

### User Profile

```http
GET /users/{user_id}/profile
```

Returns enriched preference insights.

### Event Ingestion

```http
POST /events/ingest
```

Accepts user interaction events.

### Trending Analytics

```http
GET /analytics/trending
```

Returns platform-wide trending content.

---

# Redis Caching Strategy

Recommendations generated during model execution are pre-computed and cached.

Benefits:

- Sub-20ms response times
- Reduced database pressure
- Lower infrastructure costs
- Improved user experience

Cache invalidation occurs automatically after model refresh cycles.

---

# Workflow Orchestration

Pipeline execution is managed through Apache Airflow.

```text
Kafka Event Streams
          │
          ▼
Bronze Ingestion
          │
          ▼
Silver Processing
          │
          ▼
Gold Feature Engineering
          │
          ▼
ALS Model Training
          │
          ▼
Recommendation Generation
          │
          ▼
Redis Cache Population
          │
          ▼
Dashboard Refresh
```

---

# Monitoring & Observability

## Metrics Tracked

| Metric | Alert Threshold |
|----------|----------------|
| Kafka Consumer Lag | > 10,000 |
| Streaming Delay | > 30 sec |
| ALS Training Duration | > 45 min |
| API P99 Latency | > 200 ms |
| Redis Hit Rate | < 85% |
| Snapshot Growth | > 500/day |

## Monitoring Stack

- Prometheus
- Grafana
- Spark History Server
- Airflow UI
- FastAPI Metrics Endpoint

---

# Scalability Characteristics

| Dimension | Scale |
|------------|---------|
| Daily Events | 10M+ |
| Registered Users | 1M+ |
| Content Catalog | 100K+ |
| Streaming Latency | < 10 sec |
| Recommendation Generation | Millions per cycle |
| Cached API Latency | < 20ms P99 |

---

# Project Structure

```text
content-recommender/
│
├── producers/
│   ├── watch_producer.py
│   ├── click_producer.py
│   ├── rating_producer.py
│   └── search_producer.py
│
├── spark_jobs/
│   ├── bronze_ingestion.py
│   ├── silver_user_profiles.py
│   ├── gold_feature_engineering.py
│   ├── train_als_model.py
│   └── generate_recommendations.py
│
├── iceberg_setup/
│   └── create_tables.py
│
├── api/
│   ├── main.py
│   ├── routes/
│   └── cache/
│
├── airflow/
│   └── dags/
│
├── dashboard/
│   └── app.py
│
├── scripts/
│   └── init_kafka_topics.sh
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Getting Started

## Clone Repository

```bash
git clone <repository-url>
cd content-recommender
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Start Infrastructure

```bash
docker-compose up -d
```

## Initialize Kafka Topics

```bash
bash scripts/init_kafka_topics.sh
```

## Create Iceberg Tables

```bash
spark-submit iceberg_setup/create_tables.py
```

## Start Event Producers

```bash
python producers/watch_producer.py &
python producers/click_producer.py &
python producers/rating_producer.py &
python producers/search_producer.py &
```

## Launch Streaming Pipelines

```bash
spark-submit spark_jobs/bronze_ingestion.py
spark-submit spark_jobs/silver_user_profiles.py
```

## Train Recommendation Model

```bash
spark-submit spark_jobs/train_als_model.py
```

## Generate Recommendations

```bash
spark-submit spark_jobs/generate_recommendations.py
```

## Start API

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

## Launch Dashboard

```bash
streamlit run dashboard/app.py
```

---

# Engineering Concepts Demonstrated

- Data Engineering
- Distributed Systems
- Real-Time Streaming
- Event-Driven Architecture
- Apache Spark Optimization
- Lakehouse Architecture
- Apache Iceberg
- Machine Learning Engineering
- Recommendation Systems
- API Engineering
- Workflow Orchestration
- Observability & Monitoring
- Cloud Data Platforms
- Scalable System Design

---

# Future Enhancements

- Deep Learning Recommendation Models
- Hybrid Recommendation Engine
- Feature Store Integration
- Vector Search Retrieval
- Real-Time Feature Serving
- Kubernetes Deployment
- Multi-Region Lakehouse Replication
- A/B Testing Framework
- Real-Time Ranking Models

---

# License

This project is intended for educational, research, and portfolio demonstration purposes.

---

## Author

**Jaskirat Singh**

Data Engineering • Distributed Systems • Machine Learning • Big Data Analytics

LinkedIn: Add Your LinkedIn URL  
GitHub: Add Your GitHub URL
