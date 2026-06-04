from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col, current_timestamp, struct, collect_list

WAREHOUSE = "/tmp/iceberg-recommender"
MODEL_PATH = "/tmp/als_model"

def create_spark():
    return SparkSession.builder \
        .appName("Recommender_GenerateRecs") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .getOrCreate()

def generate_recs(spark):
    model = ALSModel.load(MODEL_PATH)

    user_recs = model.recommendForAllUsers(10)

    recs_flat = user_recs.select(
        col("user_id"),
        col("recommendations").alias("recommendations"),
        current_timestamp().alias("generated_at"),
    )

    recs_flat.writeTo("local.gold.user_recommendations") \
        .option("merge-schema", "true") \
        .overwritePartitions()

    print(f"[Recs] Generated recommendations for {recs_flat.count()} users")

    print("\nSample recommendations:")
    recs_flat.limit(5).show(truncate=False)

def get_recommendations_for_user(spark, user_id: int, top_n: int = 10):
    recs = spark.read.format("iceberg").load("local.gold.user_recommendations") \
        .filter(col("user_id") == user_id)

    if recs.count() == 0:
        print(f"No recommendations found for user {user_id}")
        return []

    row = recs.first()
    rec_list = [(r.content_id, round(float(r.score), 4)) for r in row.recommendations[:top_n]]
    return rec_list

if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    generate_recs(spark)

    sample_recs = get_recommendations_for_user(spark, user_id=42)
    print(f"\nTop-10 recommendations for User 42: {sample_recs}")

    spark.stop()