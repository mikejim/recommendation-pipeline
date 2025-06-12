import redis
import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.functions import sum as _sum

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
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("🚀 Spark session created successfully")

# Use /tmp paths - these ALWAYS have proper permissions
PARQUET_PATH = "/tmp/parquet/watch_events/"
CHECKPOINT_PATH = "/tmp/parquet/checkpoints/watch_events/"
REDIS_CHECKPOINT_PATH = "/tmp/checkpoint_redis"

# Create directories (these will definitely work)
os.makedirs(PARQUET_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)
os.makedirs(REDIS_CHECKPOINT_PATH, exist_ok=True)
print(f"✅ Created directories: {PARQUET_PATH}, {CHECKPOINT_PATH}")

# Read from Kafka topic
print("📡 Setting up Kafka stream...")
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

print("🔄 Setting up aggregation...")

# Aggregate total watch time per user and genre over 1-minute window
agg = parsed.withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .groupBy(
        col("user_id"),
        col("genre"),
        window(col("event_time"), "1 minute")
    ).agg(_sum("duration_watched").alias("total_watch_time"))

# Function to write each micro-batch to Redis
def write_to_redis(batch_df, batch_id):
    try:
        r = redis.Redis(host=redis_host, port=redis_port)
        count = 0
        for row in batch_df.collect():
            key = f"user:{row['user_id']}:genre:{row['genre']}"
            value = row["total_watch_time"] 
            r.set(key, value)
            count += 1
        print(f"🔁 Batch {batch_id}: Wrote {count} records to Redis")
    except Exception as e:
        print(f"❌ Redis error in batch {batch_id}: {e}")

print("🚀 Starting Redis stream...")
# Stream #1: Write stream with foreachBatch
redis_query = agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_redis) \
    .option("checkpointLocation", REDIS_CHECKPOINT_PATH) \
    .start()

print(f"💾 Starting Parquet stream to: {PARQUET_PATH}")
# Stream #2: Write raw data to Parquet files
parquet_query = parsed.writeStream \
    .format("parquet") \
    .option("path", PARQUET_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .option("maxRecordsPerFile", 1) \
    .outputMode("append") \
    .start()

#     .trigger(once=True) \


print("🖥️ Starting console stream...")
console_query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()


print("✅ All streams started successfully!")
print("⏳ Streams are running... Press Ctrl+C to stop")

# Wait for the streams to finish
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("🛑 Stopping streams...")
    for stream in spark.streams.active:
        stream.stop()
    spark.stop()
    print("✅ All streams stopped")
except Exception as e:
    print(f"❌ Stream error: {e}")
finally:
    spark.stop()