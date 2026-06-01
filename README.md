# 🎬 Real-Time Content Recommendation Lakehouse

A production-grade real-time recommendation platform inspired by Netflix, Disney+, Hotstar, and Prime Video. The system ingests millions of user interactions in real time, processes them using distributed data pipelines, builds recommendation models, and serves personalized content recommendations through APIs and interactive dashboards.

Built using **Apache Spark, Apache Kafka, Apache Iceberg, Airflow, Redis, FastAPI, Streamlit, Docker, and AWS-inspired Lakehouse Architecture**.

---

# 🚀 Overview

Modern streaming platforms generate billions of user interaction events every day:

* Content Views
* Watch Events
* Search Queries
* Ratings
* Likes & Dislikes
* Watchlist Additions
* Content Completion Events

Traditional databases struggle to efficiently support:

* Continuous event ingestion
* Real-time personalization
* Historical analytics
* User behavior tracking
* Incremental model updates
* Large-scale recommendation pipelines

This project solves these challenges using a modern Lakehouse architecture capable of processing and analyzing streaming data at scale.

---

# ✨ Key Features

### Real-Time Event Streaming

* Kafka-based ingestion
* User watch events
* Search events
* Rating events
* Clickstream events

### Distributed Data Processing

* Apache Spark Structured Streaming
* Event enrichment
* User behavior profiling
* Feature engineering

### Lakehouse Architecture

* Apache Iceberg tables
* ACID transactions
* Snapshot-based storage
* Schema evolution
* Time-travel analytics

### Recommendation Engine

* Collaborative Filtering (ALS)
* User-Item Interaction Matrix
* Incremental model retraining
* Personalized recommendations

### Medallion Architecture

* Bronze Layer (Raw Events)
* Silver Layer (Cleaned Profiles)
* Gold Layer (Recommendation Features)

### Analytics Dashboard

* User engagement analytics
* Most watched content
* Genre popularity trends
* Recommendation insights

### API Layer

* Personalized recommendation APIs
* Content lookup APIs
* User profile APIs

---

# 🏗️ System Architecture

```text
Users
Mobile Apps
Smart TVs
Web Platform
        │
        ▼
┌─────────────────────────┐
│     Apache Kafka        │
│   Event Streaming Bus   │
└─────────────────────────┘
        │
        ▼
┌─────────────────────────┐
│ Apache Spark Streaming  │
│  Processing Layer       │
└─────────────────────────┘
        │
        ▼
┌─────────────────────────┐
│     Apache Iceberg      │
│   Lakehouse Storage     │
└─────────────────────────┘
        │
 ┌──────┼───────┐
 ▼      ▼       ▼
Bronze Silver  Gold
 Raw   Clean Features
        │
        ▼
┌─────────────────────────┐
│ ALS Recommendation Model│
└─────────────────────────┘
        │
        ▼
┌─────────────────────────┐
│ FastAPI + Redis Cache   │
└─────────────────────────┘
        │
        ▼
┌─────────────────────────┐
│ Streamlit Dashboard     │
└─────────────────────────┘
```

---

# 🛠️ Technology Stack

| Layer                  | Technology           |
| ---------------------- | -------------------- |
| Streaming              | Apache Kafka         |
| Processing             | Apache Spark         |
| Lakehouse              | Apache Iceberg       |
| Workflow Orchestration | Apache Airflow       |
| API Services           | FastAPI              |
| Caching                | Redis                |
| Dashboard              | Streamlit            |
| Storage                | AWS S3               |
| Metadata Catalog       | AWS Glue             |
| Query Engine           | Athena / Trino       |
| Containerization       | Docker               |
| Monitoring             | Prometheus & Grafana |

---

# 📂 Medallion Data Architecture

## Bronze Layer

Raw immutable event storage.

Stores:

* Watch Events
* Click Events
* Search Events
* Rating Events

Characteristics:

* Append-only
* Raw ingestion
* Replayable history
* Event preservation

---

## Silver Layer

Validated and enriched datasets.

Operations:

* Deduplication
* Data cleaning
* Sessionization
* User profile creation
* Content metadata enrichment

---

## Gold Layer

Analytics and ML-ready datasets.

Contains:

* User-Item Interaction Matrix
* Genre Preferences
* Content Popularity Scores
* Recommendation Features
* Engagement Metrics

---

# 📊 Event Types

## Watch Event

```json
{
  "user_id": "U1001",
  "content_id": "M210",
  "watch_duration": 4200,
  "completion_rate": 0.92,
  "timestamp": "2026-06-01T10:30:00"
}
```

## Rating Event

```json
{
  "user_id": "U1001",
  "content_id": "M210",
  "rating": 5,
  "timestamp": "2026-06-01T10:45:00"
}
```

## Search Event

```json
{
  "user_id": "U1001",
  "query": "sci fi thriller",
  "timestamp": "2026-06-01T09:15:00"
}
```

## Click Event

```json
{
  "user_id": "U1001",
  "content_id": "M210",
  "action": "click",
  "timestamp": "2026-06-01T09:16:00"
}
```

---

# 🤖 Recommendation Engine

The platform implements Collaborative Filtering using Apache Spark MLlib.

### Model

* Alternating Least Squares (ALS)

### Input Features

* Watch Duration
* Completion Rate
* Ratings
* Click Frequency
* Search Behavior

### Output

* Personalized Content Recommendations
* Similar User Discovery
* Content Affinity Scores

---

# 🔄 Machine Learning Pipeline

```text
User Events
      │
      ▼
Kafka Topics
      │
      ▼
Bronze Layer
      │
      ▼
Silver Layer
      │
      ▼
User Profiles
      │
      ▼
Interaction Matrix
      │
      ▼
ALS Training
      │
      ▼
Recommendation Generation
      │
      ▼
Redis Cache + API
```

---

# ❄️ Apache Iceberg Features

## ACID Transactions

Reliable concurrent reads and writes.

## Time Travel

Query historical states of user behavior.

```sql
SELECT *
FROM user_interactions
TIMESTAMP AS OF '2026-06-01 12:00:00'
```

## Schema Evolution

Add new interaction types without rewriting datasets.

```sql
ALTER TABLE interactions
ADD COLUMN device_type STRING;
```

## Snapshot Management

* Rollbacks
* Auditing
* Reproducibility
* Version Tracking

---

# ⚡ Spark Optimizations

Implemented advanced Spark optimizations:

* Adaptive Query Execution (AQE)
* Broadcast Joins
* Predicate Pushdown
* Partition Pruning
* Vectorized Parquet Reads
* Optimized Shuffle Operations

---

# 📈 Dashboard Features

### User Analytics

* Active Users
* User Retention
* Session Duration
* Watch Time

### Content Analytics

* Most Viewed Content
* Trending Shows
* Genre Popularity
* Completion Rates

### Recommendation Insights

* Top Recommendations
* Recommendation Confidence
* User Preference Clusters

### Search Analytics

* Popular Queries
* Search Conversion Rates
* Search Trends

---

# 🔄 Airflow Orchestration

Pipeline Workflow:

```text
Kafka Ingestion
        ↓
Bronze Processing
        ↓
Silver Cleaning
        ↓
Feature Engineering
        ↓
ALS Training
        ↓
Recommendation Generation
        ↓
Dashboard Refresh
```

Features:

* Scheduling
* Monitoring
* Retries
* Dependency Management

---

# 📊 Monitoring & Observability

Metrics Tracked:

* Kafka Consumer Lag
* Streaming Throughput
* Spark Job Runtime
* Model Training Time
* API Latency
* Recommendation Request Volume

Tools:

* Prometheus
* Grafana
* Spark UI
* Airflow UI

---

# 🔐 Security

Implemented concepts:

* IAM-based access controls
* Secure API access
* TLS communication
* Audit logging
* Encrypted storage

---

# 📏 Scalability

Designed for large-scale streaming platforms.

| Metric                    | Scale    |
| ------------------------- | -------- |
| Daily Events              | 10M+     |
| Users                     | 1M+      |
| Content Items             | 100K+    |
| Kafka Topics              | Multiple |
| Streaming Latency         | Seconds  |
| Recommendations Generated | Millions |

Scaling Strategies:

* More Kafka partitions
* Additional Spark executors
* Distributed model training
* Horizontal API scaling

---

# 🚀 Running the Project

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
python producers/watch_producer.py
python producers/click_producer.py
python producers/rating_producer.py
python producers/search_producer.py
```

## Start Streaming Pipeline

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

## Launch API

```bash
uvicorn api.main:app --reload
```

## Launch Dashboard

```bash
streamlit run dashboard/app.py
```

---

# 🎯 Skills Demonstrated

* Data Engineering
* Big Data Processing
* Distributed Systems
* Event-Driven Architecture
* Recommendation Systems
* Machine Learning Engineering
* Spark Optimization
* Lakehouse Architecture
* Cloud Data Platforms
* Real-Time Analytics

---

# 📜 License

This project is intended for educational, research, and portfolio purposes.

---

# 👨‍💻 Author

**Jaskirat Singh**

Data Engineering • Machine Learning • Distributed Systems • Big Data Analytics
