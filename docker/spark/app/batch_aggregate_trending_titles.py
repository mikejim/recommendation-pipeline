from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, window

# Initialize SparkSession
spark = SparkSession.builder     .appName("BatchTrendingTitles")     .getOrCreate()

# Path where the stream job stores Parquet files
WATCH_EVENTS_PARQUET_PATH = "/opt/spark-app/data/parquet/watch_events/"

# Read Parquet files written by the streaming job
df = spark.read.parquet(WATCH_EVENTS_PARQUET_PATH)

# Example: Count views per title
title_counts = df.groupBy("title").agg(count("*").alias("views")).orderBy(col("views").desc())

# Show results
title_counts.show()

# Optional: Save to Redis or file
# title_counts.write.csv("/opt/spark-app/data/output/top_titles.csv")

# Stop Spark session
spark.stop()