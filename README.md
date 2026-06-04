# 🎬 Real-Time Content Recommendation Lakehouse

<p align="center">

![Python](https://img.shields.io/badge/Python-3.11+-blue)
![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-orange)
![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-blue)
![Airflow](https://img.shields.io/badge/Apache-Airflow-green)
![FastAPI](https://img.shields.io/badge/FastAPI-009688)
![Redis](https://img.shields.io/badge/Redis-red)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)
![Docker](https://img.shields.io/badge/Docker-blue)

</p>

<p align="center">
Enterprise-Scale Real-Time Content Recommendation Platform built using Apache Kafka, Spark Structured Streaming, Apache Iceberg, Airflow, FastAPI, Redis and AWS.
</p>

<p align="center">
Inspired by large-scale recommendation architectures used by Netflix, Disney+, Prime Video, YouTube and Hotstar.
</p>

---

## 📸 Project Preview

<p align="center">
  <img src="./Screenshots/1.png" width="1000" alt="Project Preview">
</p>

---

# 📑 Table of Contents

* Overview
* Business Problem
* Architecture
* Technology Stack
* Data Flow
* Medallion Architecture
* Recommendation Engine
* API Layer
* Monitoring & Observability
* Repository Structure
* Getting Started
* Future Enhancements
* Engineering Concepts
* Author

---

# 📌 Overview

Modern streaming platforms generate millions of user interactions every day. Every watch event, click, search, rating, and session contributes to understanding user preferences and delivering personalized recommendations.

This project demonstrates a complete Lakehouse-based recommendation platform capable of:

* Real-time event ingestion
* Stream processing at scale
* Medallion data architecture
* ACID-compliant data lakehouse
* Machine learning model training
* Low-latency recommendation serving
* Interactive analytics and monitoring

---

# 🎯 Business Problem

Recommendation systems must satisfy two critical requirements:

1. React to user behavior in near real time
2. Leverage historical behavioral data for personalization

Traditional architectures often separate analytics and operational systems, leading to duplicated data, increased costs, and maintenance complexity.

This project demonstrates how a modern Lakehouse architecture unifies:

* Streaming
* Analytics
* Machine Learning
* Recommendation Serving
* Monitoring

within a single scalable platform.

---

# 🛠 Technology Stack

| Layer             | Technology                        |
| ----------------- | --------------------------------- |
| Event Streaming   | Apache Kafka                      |
| Processing Engine | Apache Spark Structured Streaming |
| Lakehouse Format  | Apache Iceberg                    |
| Data Lake         | AWS S3                            |
| Orchestration     | Apache Airflow                    |
| Machine Learning  | Spark MLlib ALS                   |
| API Serving       | FastAPI                           |
| Cache Layer       | Redis                             |
| Monitoring        | Prometheus + Grafana              |
| Dashboard         | Streamlit                         |
| Containerization  | Docker                            |

---

# 🏗️ Architecture

```text
Client Applications
(Web • Mobile • Smart TV)
          │
          ▼
    Apache Kafka
          │
          ▼
Spark Structured Streaming
          │
 ┌────────┼────────┐
 ▼        ▼        ▼
Bronze  Silver   Gold
 Layer   Layer   Layer
          │
          ▼
 Apache Iceberg
          │
          ▼
 Spark MLlib ALS
          │
          ▼
 Recommendation Engine
          │
          ▼
 FastAPI + Redis
          │
          ▼
 Applications
```

---

# ⚡ End-to-End Data Flow

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
ALS Training
      │
      ▼
Recommendations
      │
      ▼
Redis Cache
      │
      ▼
FastAPI
      │
      ▼
Applications
```

---

# 🥉 Bronze Layer

Raw immutable ingestion layer.

### Responsibilities

* Event ingestion
* Historical replay
* Auditability
* Data lineage preservation

### Event Types

* Watch Events
* Click Events
* Search Events
* Rating Events

---

# 🥈 Silver Layer

Trusted analytical datasets.

### Processing

* Deduplication
* Validation
* Null handling
* Sessionization
* Metadata enrichment
* Late event processing

### Outputs

* User interactions
* Session analytics
* Behavioral profiles
* Content metadata

---

# 🥇 Gold Layer

Machine-learning-ready datasets.

### Generated Features

* User-item interaction matrix
* Genre affinity vectors
* Content popularity scores
* Session engagement metrics
* Collaborative filtering features
* Cold-start mitigation features

---

# 🤖 Recommendation Engine

The recommendation system uses Spark MLlib's ALS (Alternating Least Squares) algorithm.

### Signals Used

| Signal          | Weight |
| --------------- | ------ |
| Completion Rate | 1.0    |
| Rating          | 0.9    |
| Watch Duration  | 0.7    |
| Click           | 0.4    |
| Search Match    | 0.3    |

### ALS Configuration

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

### Capabilities

* Collaborative filtering
* Personalized recommendations
* Top-N ranking
* Implicit feedback learning
* Incremental retraining

---

# ❄️ Apache Iceberg Features

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

# 🌐 API Layer

### Personalized Recommendations

```http
GET /recommendations/{user_id}
```

### Similar Content

```http
GET /content/{content_id}/similar
```

### User Profile

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

## Apache Spark

* Adaptive Query Execution
* Broadcast Joins
* Predicate Pushdown
* Partition Pruning
* Vectorized Reads
* Optimized Shuffle Operations

## Serving Layer

* Redis Caching
* Pre-computed Recommendations
* Async FastAPI Endpoints
* Cache Invalidation Strategy

---

# 🔄 Workflow Orchestration

Managed through Apache Airflow.

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
Redis Population
      │
      ▼
Dashboard Refresh
```

---

# 📈 Monitoring & Observability

## Metrics

| Metric             | Threshold |
| ------------------ | --------- |
| Kafka Consumer Lag | >10,000   |
| Streaming Delay    | >30 sec   |
| Training Duration  | >45 min   |
| API P99 Latency    | >200 ms   |
| Cache Hit Rate     | <85%      |
| Snapshot Growth    | >500/day  |

## Stack

* Prometheus
* Grafana
* Spark History Server
* Airflow UI
* FastAPI Metrics

---

# 📊 System Capabilities

| Metric                    | Scale            |
| ------------------------- | ---------------- |
| Daily Events Processed    | 10M+             |
| Registered Users          | 1M+              |
| Content Catalog           | 100K+            |
| Recommendation Throughput | Millions per run |
| Streaming Latency         | <10 sec          |
| Cached API Latency        | <20 ms           |
| Recommendation Refresh    | Hourly           |

---

# 🛡️ Production Readiness

* Fault-tolerant Kafka ingestion
* Spark checkpointing
* Iceberg recovery support
* Schema evolution
* Retryable Airflow DAGs
* Redis cache invalidation
* Horizontal scalability
* Observability and alerting

---

# 📂 Repository Structure

```text
content-recommender/
│
├── producers/
├── spark_jobs/
├── iceberg_setup/
├── api/
├── airflow/
├── dashboard/
├── scripts/
├── Screenshots/
│   └── 1.png
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# ⚙️ Getting Started

## Clone Repository

```bash
git clone https://github.com/Jaskirat8904/Content-Recommendation-System.git
cd Content-Recommendation-System
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

# 🎯 Engineering Highlights

✅ End-to-End Lakehouse Architecture

✅ Real-Time Streaming Pipelines

✅ Apache Iceberg Implementation

✅ Distributed Spark Processing

✅ Collaborative Filtering Recommendation Engine

✅ FastAPI Recommendation Serving

✅ Redis Low-Latency Caching

✅ Production Monitoring Stack

✅ Event-Driven System Design

✅ Scalable Data Engineering Practices

---

# 🔮 Future Enhancements

* Hybrid Recommendation Models
* Deep Learning Ranking
* Vector Search Retrieval
* Feature Store Integration
* Kubernetes Deployment
* Real-Time Feature Serving
* A/B Testing Framework
* Multi-Region Replication
* MLOps Automation

---

# 📚 Engineering Concepts Demonstrated

* Data Engineering
* Distributed Systems
* Event-Driven Architecture
* Real-Time Stream Processing
* Apache Spark Optimization
* Apache Iceberg
* Lakehouse Architecture
* Recommendation Systems
* Machine Learning Engineering
* Workflow Orchestration
* API Development
* Observability
* Cloud Data Platforms
* Scalable System Design

---

# 📜 License

This project is intended for educational, research and portfolio demonstration purposes.

---

# 👨‍💻 Author

**Jaskirat Singh**

Data Engineering • Distributed Systems • Big Data • Machine Learning

### Connect

* GitHub: https://github.com/Jaskirat8904
* LinkedIn: https://linkedin.com/in/your-linkedin
* Email: [your-email@example.com](mailto:your-email@example.com)

---

⭐ If you found this project useful, consider starring the repository.
