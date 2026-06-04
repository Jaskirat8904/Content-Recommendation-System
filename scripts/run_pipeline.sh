#!/bin/bash

SPARK_SUBMIT="spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-recommender"

echo "[1] Generating content catalog..."
python data/seed_content.py

echo "[2] Creating Iceberg tables..."
$SPARK_SUBMIT iceberg_setup/create_tables.py

echo "[3] Starting Bronze streaming (background)..."
$SPARK_SUBMIT spark_jobs/bronze_ingestion.py &

sleep 60

echo "[4] Building Silver profiles..."
$SPARK_SUBMIT spark_jobs/silver_user_profiles.py

echo "[5] Building Gold analytics..."
$SPARK_SUBMIT spark_jobs/gold_interaction_matrix.py

echo "[6] Training ALS model..."
$SPARK_SUBMIT spark_jobs/train_als_model.py

echo "[7] Generating recommendations..."
$SPARK_SUBMIT spark_jobs/generate_recommendations.py

echo "Pipeline complete!"