import redis
import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp
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

# Initialize Spark with better configuration for streaming
spark = SparkSession.builder \
    .appName("WatchEventStreamer") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "false") \
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
    .option("failOnDataLoss", "false") \
    .load()

# Convert value to JSON and parse fields
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

print("🔄 Setting up aggregation...")

# Aggregate total watch time per user and genre over 1-minute window
agg = parsed.groupBy(
        col("user_id"),
        col("genre"),
        window(col("event_time"), "1 minute")
    ).agg(_sum("duration_watched").alias("total_watch_time"))

# Function to write each micro-batch to Redis
def write_to_redis(batch_df, batch_id):
    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
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
    .trigger(processingTime="10 seconds") \
    .start()

print(f"💾 Starting Parquet stream to: {PARQUET_PATH}")

# Stream #2: Write raw data to Parquet files - VERSIÓN CORREGIDA CON DIAGNÓSTICO
def write_parquet_batch(batch_df, batch_id):
    try:
        count = batch_df.count()
        print(f"📁 Parquet Batch {batch_id}: Processing {count} records")
        
        if count > 0:
            # Show schema and sample data
            print(f"🔍 Schema for batch {batch_id}:")
            batch_df.printSchema()
            print(f"📊 Sample data for batch {batch_id}:")
            batch_df.show(3, truncate=False)
            
            # Check for null values that might cause issues
            print(f"🔎 Checking for nulls in batch {batch_id}:")
            for col_name in batch_df.columns:
                null_count = batch_df.filter(batch_df[col_name].isNull()).count()
                print(f"  {col_name}: {null_count} nulls")
            
            # First, let's write as JSON to confirm data exists
            json_path = f"/tmp/json_test/batch_{batch_id}"
            batch_df.coalesce(1).write \
                .mode("overwrite") \
                .json(json_path)
            print(f"📋 JSON test written to: {json_path}")
            
            # Check JSON files
            import os
            if os.path.exists(json_path):
                json_files = os.listdir(json_path)
                print(f"📋 JSON files created: {json_files}")
            
            # Now try parquet with different approaches
            output_path = f"{PARQUET_PATH}/batch_{batch_id}"
            print(f"📝 Writing parquet to: {output_path}")
            
            # Try approach 1: Simple write
            try:
                batch_df.coalesce(1).write \
                    .mode("overwrite") \
                    .parquet(output_path)
                print("✅ Simple parquet write succeeded")
            except Exception as e:
                print(f"❌ Simple parquet write failed: {e}")
                
                # Try approach 2: Without compression
                try:
                    batch_df.coalesce(1).write \
                        .mode("overwrite") \
                        .option("compression", "none") \
                        .parquet(output_path + "_no_compression")
                    print("✅ No compression parquet write succeeded")
                except Exception as e2:
                    print(f"❌ No compression parquet write failed: {e2}")
                    
                    # Try approach 3: Cast all columns to string
                    try:
                        string_df = batch_df.select(*[col(c).cast("string").alias(c) for c in batch_df.columns])
                        string_df.coalesce(1).write \
                            .mode("overwrite") \
                            .parquet(output_path + "_string_cast")
                        print("✅ String cast parquet write succeeded")
                    except Exception as e3:
                        print(f"❌ String cast parquet write failed: {e3}")
                
            print(f"✅ Parquet Batch {batch_id}: Successfully wrote {count} records")
            
            # Verify files were created
            import os
            if os.path.exists(output_path):
                files = os.listdir(output_path)
                print(f"📂 Files created in {output_path}: {files}")
                parquet_files = [f for f in files if f.endswith('.parquet')]
                print(f"🗂️ Parquet files: {parquet_files}")
            else:
                print(f"❌ Directory {output_path} was not created!")
                
        else:
            print(f"⚠️ Parquet Batch {batch_id}: No data to write")
            
    except Exception as e:
        print(f"❌ Parquet Batch {batch_id} error: {e}")
        import traceback
        traceback.print_exc()

parquet_query = parsed.writeStream \
    .foreachBatch(write_parquet_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print("📦 Parquet stream started:", parquet_query.isActive)

# Stream #3: Debug console output (opcional)
def debug_batch(batch_df, batch_id):
    count = batch_df.count()
    print(f"🧪 Debug Batch {batch_id}: Row count = {count}")
    if count > 0:
        print("Sample data:")
        batch_df.show(5, truncate=False)
    else:
        print("No data in this batch")

debug_query = parsed.writeStream \
    .foreachBatch(debug_batch) \
    .option("checkpointLocation", "/tmp/checkpoint_debug/") \
    .trigger(processingTime="15 seconds") \
    .start()

print("✅ All streams started successfully!")
print("🖥️ Active streams:")
print(f"  - Redis stream: {redis_query.isActive}")
print(f"  - Parquet stream: {parquet_query.isActive}")
print(f"  - Debug stream: {debug_query.isActive}")
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
    