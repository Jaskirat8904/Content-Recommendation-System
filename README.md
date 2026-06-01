# 🎬 Real-Time Content Recommendation Lakehouse

<p align="center">
  <h3 align="center">Enterprise-Scale Recommendation Platform</h3>
  <p align="center">
    A production-inspired Lakehouse architecture for real-time content recommendations powered by Apache Kafka, Apache Spark, Apache Iceberg, Airflow, FastAPI, Redis, and AWS.
  </p>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka">
  <img src="https://img.shields.io/badge/Apache-Spark-E25A1C?logo=apachespark">
  <img src="https://img.shields.io/badge/Apache-Iceberg-2F80ED">
  <img src="https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow">
  <img src="https://img.shields.io/badge/FastAPI-009688?logo=fastapi">
  <img src="https://img.shields.io/badge/Redis-DC382D?logo=redis">
  <img src="https://img.shields.io/badge/AWS-S3-orange?logo=amazonaws">
  <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker">
</p>

---

## 📌 Overview

Modern streaming platforms process millions of user interactions every day. Every click, search, watch event, rating, and session contributes to understanding user preferences and generating personalized recommendations.

This project implements a complete **Lakehouse-based Recommendation System** capable of:

- Real-time event ingestion
- Distributed stream processing
- Medallion data architecture
- ACID-compliant Lakehouse storage
- Collaborative filtering model training
- Low-latency recommendation serving
- Interactive analytics and monitoring

The platform demonstrates how organizations such as Netflix, Disney+, Prime Video, and Hotstar architect scalable recommendation systems that combine streaming analytics, machine learning, and operational data platforms.

---

## 🚀 Key Features

### Real-Time Streaming Pipeline

- High-throughput Kafka event ingestion
- Spark Structured Streaming processing
- Event enrichment and validation
- Sessionization and behavioral analytics
- Near real-time recommendation updates

### Modern Lakehouse Architecture

- Apache Iceberg table format
- ACID transactions
- Schema evolution
- Time-travel queries
- Snapshot rollback support

### Recommendation Engine

- Collaborative Filtering using ALS
- Implicit feedback modeling
- User-item interaction scoring
- Incremental retraining workflows
- Personalized Top-N recommendations

### Recommendation Serving Layer

- FastAPI microservices
- Redis caching layer
- Low-latency recommendation retrieval
- Content similarity search
- User profile APIs

### Production Observability

- Prometheus metrics
- Grafana dashboards
- Spark monitoring
- Airflow DAG visibility
- Pipeline health tracking

---

# 📊 System Capabilities

| Metric | Scale |
|----------|----------|
| Daily Events Processed | 10M+ |
| Registered Users | 1M+ |
| Content Catalog Size | 100K+ |
| Recommendation Throughput | Millions per run |
| End-to-End Streaming Latency | < 10 sec |
| Cached API Latency | < 20ms |
| Recommendation Refresh | Hourly |
| Recommendation API Availability | High Availability Design |

---

# 🏗️ Architecture

```text
┌──────────────────────────────────────────────────────┐
│                 Client Applications                  │
│       Mobile • Web • Smart TV • Partner APIs        │
└─────────────────────────┬────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────┐
│                   Apache Kafka                       │
│ Watch Events • Clicks • Ratings • Searches          │
└─────────────────────────┬────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────┐
│        Apache Spark Structured Streaming            │
│ Validation • Enrichment • Sessionization            │
└─────────────────────────┬────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
      Bronze          Silver          Gold
       Layer           Layer          Layer
          │               │               │
          └───────────────┼───────────────┘
                          ▼
┌──────────────────────────────────────────────────────┐
│              Apache Iceberg Lakehouse               │
│ ACID • Time Travel • Snapshots • Evolution          │
└─────────────────────────┬────────────────────────────┘
                          ▼
┌──────────────────────────────────────────────────────┐
│              Spark MLlib ALS Model                  │
│          Recommendation Generation                  │
└─────────────────────────┬────────────────────────────┘
                          ▼
┌──────────────────────────────────────────────────────┐
│             FastAPI + Redis Cache                   │
│         Recommendation Serving Layer               │
└─────────────────────────┬────────────────────────────┘
                          ▼
┌──────────────────────────────────────────────────────┐
│            Streamlit Analytics Dashboard            │
└──────────────────────────────────────────────────────┘
```

---

# 🎯 Business Problem

Recommendation systems must balance two competing requirements:

1. Real-time responsiveness to user activity
2. Historical analytical depth for accurate personalization

Traditional architectures often separate operational and analytical workloads, resulting in duplicated storage, increased costs, and complex maintenance.

This project demonstrates how a modern Lakehouse architecture unifies streaming, analytics, machine learning, and recommendation serving within a single scalable platform.

---

# 🥉 Bronze Layer

The Bronze Layer serves as the immutable source of truth.

### Responsibilities

- Raw event ingestion
- Historical replay capability
- Auditability
- Data lineage preservation

### Event Types

- Watch Events
- Click Events
- Search Events
- Rating Events

---

# 🥈 Silver Layer

The Silver Layer transforms raw events into trusted analytical datasets.

### Processing Logic

- Event deduplication
- Schema validation
- Null handling
- Sessionization
- Metadata enrichment
- Late-event management

### Outputs

- User interaction datasets
- Session analytics
- Enriched content metadata
- Behavioral profiles

---

# 🥇 Gold Layer

The Gold Layer creates machine-learning-ready features.

### Generated Features

- User-item interaction matrix
- Genre affinity vectors
- Content popularity scores
- Session engagement metrics
- Collaborative filtering signals
- Cold-start mitigation features

---

# 🤖 Recommendation Engine

The recommendation system leverages Spark MLlib's Alternating Least Squares (ALS) algorithm to discover latent relationships between users and content.

## Interaction Scoring

| Signal | Weight |
|----------|----------|
| Completion Rate | 1.0 |
| Rating | 0.9 |
| Watch Duration | 0.7 |
| Click | 0.4 |
| Search Match | 0.3 |

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

### Training Strategy

The platform supports incremental retraining using newly generated interaction signals, reducing compute costs while ensuring recommendations remain fresh and relevant.

---

# ❄️ Apache Iceberg Capabilities

Apache Iceberg serves as the transactional foundation of the Lakehouse.

### ACID Transactions

Reliable concurrent reads and writes.

### Time Travel

```sql
SELECT *
FROM lakehouse.silver.user_interactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-06-01 00:00:00';
```

### Schema Evolution

```sql
ALTER TABLE bronze.watch_events
ADD COLUMNS (
    device_type STRING,
    app_version STRING
);
```

### Snapshot Rollback

```sql
CALL system.rollback_to_snapshot(
    'bronze.watch_events',
    snapshot_id => 8765432109876543
);
```

---

# ⚡ Data Flow

```text
User Activity
      │
      ▼
Apache Kafka
      │
      ▼
Spark Streaming
      │
      ▼
Bronze Layer
      │
      ▼
Silver Layer
      │
      ▼
Gold Features
      │
      ▼
ALS Model Training
      │
      ▼
Recommendation Generation
      │
      ▼
Redis Cache
      │
      ▼
FastAPI APIs
      │
      ▼
Applications
```

---

# 🌐 API Layer

## Recommendation APIs

### Get Personalized Recommendations

```http
GET /recommendations/{user_id}
```

### Similar Content Lookup

```http
GET /content/{content_id}/similar
```

### User Preference Profile

```http
GET /users/{user_id}/profile
```

### Event Ingestion

```http
POST /events/ingest
```

### Trending Analytics

```http
GET /analytics/trending
```

---

# 🚀 Performance Optimizations

### Apache Spark

- Adaptive Query Execution
- Broadcast Joins
- Predicate Pushdown
- Partition Pruning
- Vectorized Parquet Reads
- Optimized Shuffle Operations

### Recommendation Serving

- Redis-backed caching
- Pre-computed recommendation sets
- Asynchronous FastAPI endpoints
- Cache invalidation after model refresh

---

# 🔄 Workflow Orchestration

All workflows are orchestrated using Apache Airflow.

```text
Kafka Streams
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

# 📈 Monitoring & Observability

## Metrics Tracked

| Metric | Threshold |
|----------|------------|
| Kafka Consumer Lag | >10,000 |
| Streaming Delay | >30 sec |
| Training Duration | >45 min |
| API P99 Latency | >200ms |
| Cache Hit Rate | <85% |
| Snapshot Growth | >500/day |

## Monitoring Stack

- Prometheus
- Grafana
- Spark History Server
- Airflow UI
- FastAPI Metrics Endpoint

---

# 🛡️ Production Readiness

- Fault-tolerant Kafka ingestion
- Spark checkpointing
- Iceberg snapshot recovery
- Schema evolution support
- Retryable Airflow DAGs
- Redis cache invalidation
- Distributed processing architecture
- Horizontal scalability
- Observability and alerting

---

# 📂 Repository Structure

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

# ⚙️ Getting Started

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

## Create Kafka Topics

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

## Launch Streaming Jobs

```bash
spark-submit spark_jobs/bronze_ingestion.py
spark-submit spark_jobs/silver_user_profiles.py
```

## Train Model

```bash
spark-submit spark_jobs/train_als_model.py
```

## Generate Recommendations

```bash
spark-submit spark_jobs/generate_recommendations.py
```

## Start API

```bash
uvicorn api.main:app --reload
```

## Launch Dashboard

```bash
streamlit run dashboard/app.py
```

---

# 💡 Engineering Concepts Demonstrated

- Data Engineering
- Distributed Systems
- Event-Driven Architecture
- Real-Time Stream Processing
- Apache Spark Optimization
- Apache Iceberg
- Lakehouse Architecture
- Recommendation Systems
- Machine Learning Engineering
- Workflow Orchestration
- API Development
- Observability & Monitoring
- Cloud Data Platforms
- Scalable System Design

---

# 🔮 Future Enhancements

- Hybrid Recommendation Models
- Deep Learning Ranking Systems
- Vector Search Retrieval
- Feature Store Integration
- Kubernetes Deployment
- Real-Time Feature Serving
- A/B Testing Framework
- Multi-Region Data Replication

---

# 📜 License

This project is intended for educational, research, and portfolio demonstration purposes.

---

# 👨‍💻 Author

**Jaskirat Singh**

Data Engineering • Distributed Systems • Machine Learning • Big Data Analytics

