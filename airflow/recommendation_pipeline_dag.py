from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "jaskirat",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK = (
    "spark-submit "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.local.type=hadoop "
    "--conf spark.sql.catalog.local.warehouse=/tmp/iceberg-recommender"
)
JOBS = "/opt/spark_jobs"

with DAG(
    dag_id="content_recommendation_pipeline",
    default_args=default_args,
    description="Real-Time Content Recommendation Lakehouse Pipeline",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["recommendation", "iceberg", "als"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"{SPARK} {JOBS}/bronze_ingestion.py",
        execution_timeout=timedelta(minutes=10),
    )

    silver = BashOperator(
        task_id="silver_user_profiles",
        bash_command=f"{SPARK} {JOBS}/silver_user_profiles.py",
        execution_timeout=timedelta(minutes=10),
    )

    gold = BashOperator(
        task_id="gold_interaction_matrix",
        bash_command=f"{SPARK} {JOBS}/gold_interaction_matrix.py",
        execution_timeout=timedelta(minutes=10),
    )

    train = BashOperator(
        task_id="train_als_model",
        bash_command=f"{SPARK} {JOBS}/train_als_model.py",
        execution_timeout=timedelta(minutes=20),
    )

    recs = BashOperator(
        task_id="generate_recommendations",
        bash_command=f"{SPARK} {JOBS}/generate_recommendations.py",
        execution_timeout=timedelta(minutes=10),
    )

    compaction = BashOperator(
        task_id="iceberg_compaction",
        bash_command=(
            f"{SPARK} -e \"CALL local.system.rewrite_data_files("
            "table => 'silver.user_content_interactions', strategy => 'binpack')\""
        ),
        execution_timeout=timedelta(minutes=15),
    )

    bronze >> silver >> gold >> train >> recs >> compaction