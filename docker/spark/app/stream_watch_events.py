import redis
import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

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
    """Process each batch: write to Redis and Parquet"""
    # Cache the batch for multiple operations
    batch_df.cache()
    
    try:
        record_count = batch_df.count()
        
        if record_count == 0:
            print(f"⚡ Batch {batch_id}: No records to process")
            return
        
        print(f"⚡ Batch {batch_id}: {record_count} records", end=" → ")
        
        # DEBUG: Show actual data before writing
        print(f"\n[DEBUG] Sample data:")
        sample_rows = batch_df.limit(2).collect()
        for i, row in enumerate(sample_rows):
            print(f"[DEBUG] Row {i}: user_id={row.user_id}, show_id={row.show_id}, genre={row.genre}")
        
        # 1. Write to Redis
        redis_count = 0
        for row in batch_df.collect():
            try:
                redis_key = f"watch_event:{row.user_id}:{row.show_id}:{batch_id}"
                redis_value = {
                    "user_id": row.user_id,
                    "show_id": row.show_id,
                    "genre": row.genre,
                    "device_type": row.device_type,
                    "duration_watched": row.duration_watched,
                    "event_time": str(row.event_time),
                    "processed_at": str(row.processed_at)
                }
                redis_client.setex(redis_key, 3600, json.dumps(redis_value))
                redis_count += 1
            except Exception as e:
                print(f"Redis error for record: {e}")
        
        print(f"Redis: {redis_count}", end=" → ")
        
        # 2. Write to Parquet - MULTIPLE SOLUTION ATTEMPTS
        try:
            # Ensure the directory exists
            parquet_base = "/tmp/watch_events_parquet"
            os.makedirs(parquet_base, exist_ok=True)
            
            # Count files before writing
            files_before = set(os.listdir(parquet_base)) if os.path.exists(parquet_base) else set()
            
            # Debug: Check partitioning and actual data
            print(f"\n[DEBUG] DataFrame info before write:")
            print(f"[DEBUG] Total records: {record_count}")
            print(f"[DEBUG] Partitions: {batch_df.rdd.getNumPartitions()}")
            
            if record_count > 0:
                # SOLUTION 1: Try multiple write strategies
                success = False
                method = "unknown"
                
                try:
                    # Strategy A: Direct manual file write using pandas
                    print(f"[DEBUG] Trying Strategy A: Manual pandas write...")
                    
                    # Convert to pandas and write manually
                    collected_data = batch_df.collect()
                    print(f"[DEBUG] Materialized {len(collected_data)} rows")
                    
                    # Convert to pandas DataFrame
                    import pandas as pd
                    
                    rows_data = []
                    for row in collected_data:
                        row_dict = {
                            'user_id': row.user_id,
                            'show_id': row.show_id,
                            'genre': row.genre,
                            'device_type': row.device_type,
                            'duration_watched': row.duration_watched,
                            'event_time': row.event_time,
                            'processed_at': row.processed_at
                        }
                        rows_data.append(row_dict)
                    
                    pandas_df = pd.DataFrame(rows_data)
                    print(f"[DEBUG] Created pandas DataFrame with {len(pandas_df)} rows")
                    
                    # Write using pandas
                    output_file = os.path.join(parquet_base, f"batch_{batch_id}.parquet")
                    pandas_df.to_parquet(output_file, engine='pyarrow', index=False)
                    print(f"[DEBUG] Wrote to {output_file}")
                    
                    method = "pandas"
                    success = True
                    
                except Exception as e1:
                    print(f"[DEBUG] Strategy A failed: {e1}")
                    
                    try:
                        # Strategy B: Write to CSV first as test
                        print(f"[DEBUG] Trying Strategy B: CSV write test...")
                        
                        # Test with simple CSV write
                        csv_file = os.path.join(parquet_base, f"batch_{batch_id}.csv")
                        batch_df.coalesce(1).write \
                            .mode("append") \
                            .option("header", "true") \
                            .csv(csv_file)
                        
                        method = "csv-test"
                        success = True
                        
                    except Exception as e2:
                        print(f"[DEBUG] Strategy B failed: {e2}")
                        
                        try:
                            # Strategy C: Write with explicit partitioning
                            print(f"[DEBUG] Trying Strategy C: Repartition write...")
                            
                            batch_df.repartition(1).write \
                                .mode("append") \
                                .parquet(parquet_base)
                            
                            method = "repartitioned"
                            success = True
                            
                        except Exception as e3:
                            print(f"[DEBUG] Strategy C failed: {e3}")
                            
                            # Strategy D: Save as temporary file then move
                            print(f"[DEBUG] Trying Strategy D: Temp file approach...")
                            
                            temp_path = f"/tmp/temp_parquet_{batch_id}"
                            batch_df.coalesce(1).write \
                                .mode("overwrite") \
                                .parquet(temp_path)
                            
                            # Move files from temp to final location
                            import shutil
                            if os.path.exists(temp_path):
                                for f in os.listdir(temp_path):
                                    if f.endswith('.parquet'):
                                        shutil.move(
                                            os.path.join(temp_path, f),
                                            os.path.join(parquet_base, f"{batch_id}_{f}")
                                        )
                                # Clean up temp directory
                                shutil.rmtree(temp_path, ignore_errors=True)
                                
                            method = "temp-file"
                            success = True
                
                if not success:
                    print(f"[DEBUG] All write strategies failed!")
            else:
                print("[DEBUG] No records to write")
                method = "skipped"
            
            # Give Spark a moment to flush files
            import time
            time.sleep(2)  # Increased wait time
            
            # Count files after writing
            files_after = set(os.listdir(parquet_base)) if os.path.exists(parquet_base) else set()
            new_files = files_after - files_before
            
            # More comprehensive file detection
            data_files = []
            for f in new_files:
                full_path = os.path.join(parquet_base, f)
                if os.path.isfile(full_path):
                    file_size = os.path.getsize(full_path)
                    # Consider any .parquet file or part file with size > 0 as data
                    if (f.endswith('.parquet') or f.startswith('part-')) and file_size > 0:
                        data_files.append(f)
            
            success_files = [f for f in new_files if f == '_SUCCESS']
            other_files = [f for f in new_files if f not in data_files and f not in success_files]
            
            all_files = list(files_after)
            total_size = sum(os.path.getsize(os.path.join(parquet_base, f)) for f in all_files if not f.startswith('.') and os.path.isfile(os.path.join(parquet_base, f)))
            
            print(f"Parquet: ✅ ({method}) (new: {len(data_files)} data files, {len(success_files)} success, {len(other_files)} other, total size: {total_size} bytes)", end=" → ")
            
            # Show file details if still no data files
            if len(data_files) == 0 and record_count > 0:
                print(f"\n[DEBUG] Expected data but got no files. All files: {sorted(all_files)}")
                # Try to understand what happened by checking file sizes
                for f in all_files:
                    if not f.startswith('.'):
                        full_path = os.path.join(parquet_base, f)
                        if os.path.isfile(full_path):
                            size = os.path.getsize(full_path)
                            print(f"[DEBUG] File: {f}, Size: {size} bytes")
            elif len(data_files) > 0:
                print(f"\n[DEBUG] SUCCESS! Created data files: {data_files}")
                    
        except Exception as e:
            print(f"Parquet: ❌ ({str(e)[:50]}...)", end=" → ")
            import traceback
            print(f"\n[DEBUG] Full error: {traceback.format_exc()}")
        
        # 3. Log batch stats
        try:
            stats_key = f"batch_stats:{batch_id}"
            stats = {
                "batch_id": batch_id,
                "record_count": record_count,
                "processed_at": str(current_timestamp())
            }
            redis_client.setex(stats_key, 7200, json.dumps(stats, default=str))
            print("✅ DONE")
            
        except Exception as e:
            print(f"Stats: ❌ ({str(e)[:20]}...)")
        
    except Exception as e:
        print(f"❌ BATCH ERROR: {str(e)[:50]}...")
        import traceback
        print(f"[DEBUG] Full batch error: {traceback.format_exc()}")
    
    finally:
        batch_df.unpersist()

# ALTERNATIVE APPROACH: Use built-in file sink instead of foreachBatch for Parquet
# This is often more reliable for file writing
def use_file_sink_approach():
    """Alternative approach using built-in file sink"""
    print("🔄 Using file sink approach...")
    
    # Write to Parquet using built-in sink
    parquet_query = parsed_df.writeStream \
        .format("parquet") \
        .option("path", "/tmp/watch_events_parquet_alt") \
        .option("checkpointLocation", "/tmp/watch_events_parquet_checkpoint") \
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
USE_FILE_SINK = False  # Set to True to try the alternative approach

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