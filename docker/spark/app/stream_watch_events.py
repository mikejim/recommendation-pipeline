from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import redis
import json

# Redis config
REDIS_HOST = "localhost"
REDIS_PORT = 6379

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
    .option("kafka.bootstrap.servers", "localhost:9092") \
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
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    for row in batch_df.collect():
        key = f"user:{row['user_id']}:genre:{row['genre']}"
        value = row["sum(duration_watched)"]
        redis_client.set(key, value)
        print(f"🔁 Wrote to Redis: {key} = {value}")

# Write stream with foreachBatch
query = agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_redis) \
    .option("checkpointLocation", "./checkpoint") \
    .start()

query.awaitTermination()
