from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, rank, current_date,
    row_number, desc
)
from pyspark.sql.window import Window

WAREHOUSE = "/tmp/iceberg-recommender"

def create_spark():
    return SparkSession.builder \
        .appName("Recommender_GoldMatrix") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .getOrCreate()

def compute_content_popularity(spark):
    watches = spark.read.format("iceberg").load("local.bronze.watch_events")
    ratings = spark.read.format("iceberg").load("local.bronze.rating_events")
    clicks  = spark.read.format("iceberg").load("local.bronze.click_events")

    watch_agg = watches.groupBy("content_id").agg(
        count("event_id").alias("total_watches")
    )
    rating_agg = ratings.groupBy("content_id").agg(
        avg("rating").alias("avg_rating")
    )
    click_agg = clicks.groupBy("content_id").agg(
        count("event_id").alias("total_clicks")
    )

    popularity = watch_agg \
        .join(rating_agg, "content_id", "left") \
        .join(click_agg, "content_id", "left") \
        .fillna({"avg_rating": 0.0, "total_clicks": 0})

    window = Window.orderBy(
        desc("total_watches"), desc("avg_rating"), desc("total_clicks")
    )
    popularity = popularity \
        .withColumn("popularity_rank", row_number().over(window)) \
        .withColumn("trend_score",
            col("total_watches") * 0.5 +
            col("avg_rating") * 10.0 +
            col("total_clicks") * 0.3
        ) \
        .withColumn("computed_date", current_date())

    popularity.writeTo("local.gold.content_popularity") \
        .option("merge-schema", "true") \
        .overwritePartitions()

    print(f"[Gold] Content popularity computed. Rows: {popularity.count()}")

def compute_genre_trends(spark):
    import pandas as pd
    catalog = spark.read.csv("data/content_catalog.csv", header=True, inferSchema=True)
    watches = spark.read.format("iceberg").load("local.bronze.watch_events")
    ratings = spark.read.format("iceberg").load("local.bronze.rating_events")

    watch_genre = watches.join(catalog.select("content_id", "genre"), "content_id") \
        .groupBy("genre") \
        .agg(count("event_id").alias("total_watches"),
             count("user_id").alias("unique_users"))

    rating_genre = ratings.join(catalog.select("content_id", "genre"), "content_id") \
        .groupBy("genre") \
        .agg(avg("rating").alias("avg_rating"))

    genre_df = watch_genre.join(rating_genre, "genre", "left") \
        .fillna({"avg_rating": 0.0}) \
        .withColumn("computed_date", current_date())

    genre_df.writeTo("local.gold.genre_trends") \
        .option("merge-schema", "true") \
        .overwritePartitions()

    print(f"[Gold] Genre trends computed. Rows: {genre_df.count()}")

if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    compute_content_popularity(spark)
    compute_genre_trends(spark)
    print("[Gold] All gold jobs done.")
    spark.stop()