import redis
import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

# Parquet path used in process_batch
parquet_base = os.getenv("PARQUET_OUTPUT_PATH", "/tmp/watch_events_parquet")

USE_REDIS = True  # or False if you don't need Redis

# Load environment variables
load_dotenv()

# Redis connection
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# Schema
schema = StructType() \
    .add("user_id", StringType()) \
    .add("show_id", StringType()) \
    .add("genre", StringType()) \
    .add("device_type", StringType()) \
    .add("duration_watched", IntegerType()) \
    .add("timestamp", StringType())

# Initialize Spark with optimized settings and minimal logging
spark = SparkSession.builder \
    .appName("WatchEventsStream") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.streaming.checkpointLocation.deleteOnExit", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Set minimal logging
spark.sparkContext.setLogLevel("ERROR")
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

print("🚀 Starting Watch Events Pipeline...")
print("📊 Processing batches every 30 seconds...")
print("⏳ Running... Press Ctrl+C to stop\n")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "watch_events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

# Parse JSON and transform - FIX: Add better error handling and validation
parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .filter(col("json_string").isNotNull() & (col("json_string") != "")) \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .filter(col("data").isNotNull()) \
    .select("data.*") \
    .filter(col("user_id").isNotNull() & col("show_id").isNotNull()) \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withColumn("processed_at", current_timestamp())


def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"[spark] ℹ️ No records to process in batch {batch_id}")
        return

    print(f"[spark] ✅ Processing batch {batch_id} with {batch_df.count()} records")

    # Redis part stays
    if USE_REDIS:

        for row in batch_df.select("event_time", "user_id", "show_id").collect():
            redis_key = f"user:{row.user_id}:event"
            redis_value = f"{row.event_time}|{row.show_id}"
            redis_client.rpush(redis_key, redis_value)
        print(f"[spark] ✅ Redis updated with {batch_df.count()} events")
 
    # Spark-native Parquet writing 
    output_path = os.path.join(parquet_base, f"batch_{batch_id}")
    batch_df.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"[spark] 💾 Written to Parquet at {output_path}")


def use_file_sink_approach():
    """Alternative approach using built-in file sink"""
    print("🔄 Using file sink approach...")
    
    # Write to Parquet using built-in sink
    parquet_query = parsed_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "file:///app/shared_volume/checkpoints") \
        .option("path", "file:///app/shared_volume/parquet_output") \
        .trigger(processingTime="30 seconds") \
        .outputMode("append") \
        .start()
    
    # Write to console for debugging
    console_query = parsed_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="30 seconds") \
        .outputMode("append") \
        .start()
    
    return parquet_query, console_query

# Choose approach: comment/uncomment as needed
USE_FILE_SINK = True  # Set to True to try the alternative approach

if USE_FILE_SINK:
    parquet_query, console_query = use_file_sink_approach()
    
    try:
        # Keep both queries running
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping pipeline...")
        parquet_query.stop()
        console_query.stop()
        print("✅ Pipeline stopped")
    finally:
        spark.stop()

else:
    # Original foreachBatch approach with fixes
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/watch_events_checkpoint") \
        .trigger(processingTime="30 seconds") \
        .outputMode("append") \
        .start()

    try:
        # Keep the application running
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping pipeline...")
        query.stop()
        print("✅ Pipeline stopped")
    finally:
        spark.stop()