#!/bin/bash

echo "📊 Waiting for initial Parquet files..."
while [ ! "$(ls -A /app/shared_volume/parquet_output/*.parquet 2>/dev/null)" ]; do
    sleep 5
done

echo "🚀 Starting Dash..."
exec python /app/dash-app/dashboard.py
