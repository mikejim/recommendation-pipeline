#!/bin/bash

echo "🛠️ Fixing permissions for shared_volume..."
mkdir -p /app/shared_volume/parquet_output /app/shared_volume/checkpoints
chmod -R 777 /app/shared_volume

echo "🚀 Starting Spark job..."
exec "$@"
