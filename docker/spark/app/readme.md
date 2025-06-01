# 🔁 Batch vs Streaming Jobs in Netflix-Style Pipeline

This project uses both **Spark Structured Streaming** and **Spark Batch** processing to simulate a realistic, scalable content recommendation system — just like Netflix.

---

## ⚡ Streaming Job: `stream_watch_events.py`

**Real-Time Processing** of user behavior.

| Property              | Description |
|----------------------|-------------|
| **Purpose**          | Low-latency processing of user `watch_events` in real time |
| **Input**            | Kafka topic `watch_events` |
| **Execution**        | Runs continuously (Structured Streaming) |
| **Storage**          | Outputs to Redis (for real-time API access) |
| **Latency**          | Seconds (near real-time) |
| **Use Cases**        | Live dashboards, active user monitoring, real-time UX feedback |
| **Deployment**       | Via Docker container using `spark-submit` or Docker Compose |
| **Output**           | Transformed events, optionally stored in Redis or Parquet files |

---

## 📊 Batch Job: `batch_aggregate_trending_titles.py`

**Offline Aggregation** of historical data.

| Property              | Description |
|----------------------|-------------|
| **Purpose**          | Analyze long-term trends by aggregating historical events |
| **Input**            | Parquet files written by the streaming job |
| **Execution**        | Runs on demand (daily/weekly using cron or orchestration tool) |
| **Storage**          | Output can be Redis, CSV, PostgreSQL, BigQuery, etc. |
| **Latency**          | Minutes to hours |
| **Use Cases**        | Top shows of the week, cohort analysis, ML feature generation |
| **Deployment**       | Via `spark-submit` with Docker |
| **Output**           | Aggregates (e.g., top 10 most-watched titles) |

---

## 🏗️ Why Both Matter

Together, they form a **Lambda Architecture**:

- **Streaming Layer** → Serves low-latency needs for immediate feedback
- **Batch Layer** → Handles complex, large-scale computation over time
- **Serving Layer** (Redis + FastAPI) → Makes results accessible in real-time

---

## ✅ How to Run

### Streaming:
```bash
docker-compose up spark-submit
```

### Batch:
```bash
docker-compose run spark-submit spark-submit --master spark://spark-master:7077 /opt/spark-app/batch_aggregate_trending_titles.py
```

---

## 📂 Recommended Folder Structure

```
docker/spark/app/
├── stream_watch_events.py
├── simulate_watch_events.py
├── batch_aggregate_trending_titles.py  ← batch job
├── requirements.txt
└── .env
```

---

By showcasing both paradigms, this project simulates **production-ready data pipelines** used by companies like Netflix.