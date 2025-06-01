from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import redis
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Redis connection settings
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", 6379))

# Define schema of Kafka message
schema = StructType() \
    .add("user_id", StringType()) \
    .add("show_id", StringType()) \
    .add("genre", StringType()) \
    .add("device_type", StringType()) \
    .add("duration_watched", IntegerType()) \
    .add("timestamp", StringType())

# Initialize Spark
spark = SparkSession.builder \
    .appName("WatchEventStreamer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "watch_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert value to JSON and parse fields
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Aggregate total watch time per user and genre over 1-minute window
agg = parsed.withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .groupBy(
        col("user_id"),
        col("genre"),
        window(col("event_time"), "1 minute")
    ).sum("duration_watched")

# Function to write each micro-batch to Redis
def write_to_redis(batch_df, batch_id):
    # Redis client
    r = redis.Redis(host=redis_host, port=redis_port)
    for row in batch_df.collect():
        key = f"user:{row['user_id']}:genre:{row['genre']}"
        value = row["sum(duration_watched)"]
        r.set(key, value)
        print(f"🔁 Wrote to Redis: {key} = {value}")

# Write stream with foreachBatch
query = agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_redis) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
