from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col

WAREHOUSE = "/tmp/iceberg-recommender"
MODEL_PATH = "/tmp/als_model"

def create_spark():
    return SparkSession.builder \
        .appName("Recommender_TrainALS") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def train_als(spark):
    interactions = spark.read.format("iceberg") \
        .load("local.silver.user_content_interactions") \
        .select("user_id", "content_id", "implicit_score") \
        .filter(col("implicit_score") > 0) \
        .dropna()

    print(f"[ALS] Training on {interactions.count()} interaction rows")

    train, test = interactions.randomSplit([0.8, 0.2], seed=42)

    als = ALS(
        maxIter=15,
        regParam=0.1,
        rank=20,
        userCol="user_id",
        itemCol="content_id",
        ratingCol="implicit_score",
        coldStartStrategy="drop",
        implicitPrefs=True,
        alpha=1.0,
    )

    model = als.fit(train)

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="implicit_score",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"[ALS] Model RMSE on test set: {rmse:.4f}")

    model.save(MODEL_PATH)
    print(f"[ALS] Model saved to: {MODEL_PATH}")
    return model

if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    train_als(spark)
    print("[ALS] Training complete.")
    spark.stop()