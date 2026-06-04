from pyspark.sql import SparkSession

WAREHOUSE = "/tmp/iceberg-recommender"

spark = SparkSession.builder \
    .appName("RecommenderTableSetup") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS local.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS local.silver")
spark.sql("CREATE DATABASE IF NOT EXISTS local.gold")

# ── Bronze Tables ──────────────────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS local.bronze.watch_events (
    event_id            STRING,
    user_id             INT,
    content_id          INT,
    watch_status        STRING,
    watch_duration_mins INT,
    total_duration_mins INT,
    completion_pct      DOUBLE,
    device              STRING,
    timestamp           STRING,
    ingestion_time      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_time))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.bronze.click_events (
    event_id     STRING,
    user_id      INT,
    content_id   INT,
    action       STRING,
    source_page  STRING,
    session_id   STRING,
    timestamp    STRING,
    ingestion_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_time))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.bronze.rating_events (
    event_id     STRING,
    user_id      INT,
    content_id   INT,
    rating       DOUBLE,
    review_text  STRING,
    timestamp    STRING,
    ingestion_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_time))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.bronze.search_events (
    event_id          STRING,
    user_id           INT,
    query             STRING,
    results_count     INT,
    clicked_result_id INT,
    timestamp         STRING,
    ingestion_time    TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_time))
""")

# ── Silver Tables ──────────────────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.user_content_interactions (
    user_id             INT,
    content_id          INT,
    implicit_score      DOUBLE,
    watch_count         BIGINT,
    avg_completion_pct  DOUBLE,
    click_count         BIGINT,
    explicit_rating     DOUBLE,
    last_interaction    TIMESTAMP,
    processed_time      TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(20, user_id))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.user_profiles (
    user_id             INT,
    total_watch_mins    DOUBLE,
    total_content_seen  BIGINT,
    avg_rating_given    DOUBLE,
    favourite_genre     STRING,
    favourite_device    STRING,
    activity_score      DOUBLE,
    last_active         TIMESTAMP
)
USING iceberg
""")

# ── Gold Tables ────────────────────────────────────────────────────────────────

spark.sql("""
CREATE TABLE IF NOT EXISTS local.gold.user_recommendations (
    user_id         INT,
    recommendations ARRAY<STRUCT<content_id: INT, score: DOUBLE>>,
    generated_at    TIMESTAMP
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.gold.content_popularity (
    content_id      INT,
    total_watches   BIGINT,
    avg_rating      DOUBLE,
    total_clicks    BIGINT,
    popularity_rank INT,
    trend_score     DOUBLE,
    computed_date   DATE
)
USING iceberg
PARTITIONED BY (computed_date)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.gold.genre_trends (
    genre           STRING,
    total_watches   BIGINT,
    avg_rating      DOUBLE,
    unique_users    BIGINT,
    computed_date   DATE
)
USING iceberg
PARTITIONED BY (computed_date)
""")

print("All Iceberg tables created successfully.")
spark.stop()