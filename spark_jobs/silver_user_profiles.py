from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, max as spark_max,
    to_timestamp, current_timestamp, when, round as spark_round
)

WAREHOUSE = "/tmp/iceberg-recommender"

def create_spark():
    return SparkSession.builder \
        .appName("Recommender_SilverProfiles") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def build_interaction_matrix(spark):
    watch = spark.read.format("iceberg").load("local.bronze.watch_events") \
        .filter(col("user_id").isNotNull() & col("content_id").isNotNull()) \
        .groupBy("user_id", "content_id") \
        .agg(
            count("event_id").alias("watch_count"),
            avg("completion_pct").alias("avg_completion_pct"),
        )

    clicks = spark.read.format("iceberg").load("local.bronze.click_events") \
        .filter(col("user_id").isNotNull() & col("content_id").isNotNull()) \
        .groupBy("user_id", "content_id") \
        .agg(count("event_id").alias("click_count"))

    ratings = spark.read.format("iceberg").load("local.bronze.rating_events") \
        .filter(col("user_id").isNotNull() & col("content_id").isNotNull()) \
        .groupBy("user_id", "content_id") \
        .agg(avg("rating").alias("explicit_rating"))

    interactions = watch \
        .join(clicks,  ["user_id", "content_id"], "outer") \
        .join(ratings, ["user_id", "content_id"], "outer")

    interactions = interactions.withColumn(
        "implicit_score",
        spark_round(
            (
                (col("watch_count").cast("double") * 2.0) +
                (col("avg_completion_pct") / 20.0) +
                (col("click_count").cast("double") * 0.5) +
                (when(col("explicit_rating").isNotNull(), col("explicit_rating")).otherwise(0.0))
            ), 3
        )
    ).withColumn("last_interaction", current_timestamp()) \
     .withColumn("processed_time", current_timestamp())

    interactions.writeTo("local.silver.user_content_interactions") \
        .option("merge-schema", "true") \
        .overwritePartitions()

    print(f"[Silver] Interaction matrix built. Rows: {interactions.count()}")

def build_user_profiles(spark):
    watch = spark.read.format("iceberg").load("local.bronze.watch_events")
    ratings = spark.read.format("iceberg").load("local.bronze.rating_events")

    watch_stats = watch.groupBy("user_id").agg(
        sum("watch_duration_mins").alias("total_watch_mins"),
        count("content_id").alias("total_content_seen"),
        spark_max(to_timestamp(col("timestamp"))).alias("last_active"),
    )

    rating_stats = ratings.groupBy("user_id").agg(
        avg("rating").alias("avg_rating_given"),
    )

    device_mode = watch.groupBy("user_id", "device") \
        .count() \
        .orderBy("count", ascending=False) \
        .dropDuplicates(["user_id"]) \
        .select("user_id", col("device").alias("favourite_device"))

    profiles = watch_stats \
        .join(rating_stats, "user_id", "left") \
        .join(device_mode, "user_id", "left") \
        .withColumn("favourite_genre", col("user_id").cast("string")) \
        .withColumn("activity_score",
            spark_round(col("total_watch_mins") / 60.0, 2)
        )

    profiles.writeTo("local.silver.user_profiles") \
        .option("merge-schema", "true") \
        .overwritePartitions()

    print(f"[Silver] User profiles built. Rows: {profiles.count()}")

if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    build_interaction_matrix(spark)
    build_user_profiles(spark)
    print("[Silver] Done.")
    spark.stop()