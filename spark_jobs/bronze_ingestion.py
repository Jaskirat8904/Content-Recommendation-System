from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

KAFKA_BOOTSTRAP = "localhost:9092"
WAREHOUSE = "/tmp/iceberg-recommender"
CHECKPOINT = "/tmp/checkpoints/recommender"

def create_spark():
    return SparkSession.builder \
        .appName("Recommender_BronzeIngestion") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

WATCH_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("content_id", IntegerType()),
    StructField("watch_status", StringType()),
    StructField("watch_duration_mins", IntegerType()),
    StructField("total_duration_mins", IntegerType()),
    StructField("completion_pct", DoubleType()),
    StructField("device", StringType()),
    StructField("timestamp", StringType()),
])

CLICK_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("content_id", IntegerType()),
    StructField("action", StringType()),
    StructField("source_page", StringType()),
    StructField("session_id", StringType()),
    StructField("timestamp", StringType()),
])

RATING_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("content_id", IntegerType()),
    StructField("rating", DoubleType()),
    StructField("review_text", StringType()),
    StructField("timestamp", StringType()),
])

SEARCH_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("query", StringType()),
    StructField("results_count", IntegerType()),
    StructField("clicked_result_id", IntegerType()),
    StructField("timestamp", StringType()),
])

def kafka_stream(spark, topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_value")

def stream_to_iceberg(df, schema, checkpoint_name, table_name):
    parsed = df.select(
        from_json(col("json_value"), schema).alias("d")
    ).select("d.*").withColumn("ingestion_time", current_timestamp())

    return parsed.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT}/bronze/{checkpoint_name}") \
        .toTable(f"local.bronze.{table_name}")

if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("[Bronze] Starting all 4 ingestion streams...")

    q1 = stream_to_iceberg(kafka_stream(spark, "watch-events"), WATCH_SCHEMA, "watch", "watch_events")
    q2 = stream_to_iceberg(kafka_stream(spark, "click-events"), CLICK_SCHEMA, "click", "click_events")
    q3 = stream_to_iceberg(kafka_stream(spark, "rating-events"), RATING_SCHEMA, "rating", "rating_events")
    q4 = stream_to_iceberg(kafka_stream(spark, "search-events"), SEARCH_SCHEMA, "search", "search_events")

    spark.streams.awaitAnyTermination()