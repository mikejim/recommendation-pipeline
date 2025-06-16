# 🎬 Netflix-Style Content Recommendation Pipeline

This is a full-stack **Data Engineering project** that simulates a streaming service that generates and processes user watch behavior in **real-time** and **batch** using Kafka, Spark, Redis, and FastAPI. Designed to showcase **streaming data pipelines** and **real-time analytics**.

---

## 🚀 Architecture Overview

![alt text for screen readers](/kafka_arch.jpg "Streaming architecture")

```

On future modifications, we want to achieve this: (FastAPI to show a dashboard, parquet located in AWS or Azure).

[ User Events ] → Kafka → Spark (Structured Streaming) → Redis → FastAPI (Real-time API)
                          ↓
                       Parquet (HDFS/S3) → Spark Batch → Aggregates
```

---

## 🔧 Tech Stack

- **Kafka**: Real-time event ingestion
- **Zookeeper**: Kafka coordination
- **Spark**: Streaming and batch processing
- **Redis**: Real-time data store for low-latency queries
- **FastAPI**: Lightweight backend for accessing recommendation data
- **Docker Compose**: Service orchestration

---

## 📦 Project Structure

```
.
├── docker-compose.yml
├── docker/
│   ├── Dockerfile                 # Dockerfile for spark-submit job
│   └── spark/
│       └── app/
│           ├── stream_watch_events.py  # Spark Structured Streaming job
│           ├── requirements.txt        # Python dependencies
│           ├── .env                    # Environment variables (Kafka, Redis)
│           └── simulate_watch_events.py # Event simulator (Kafka producer)
├── api/
│   └── main.py                     # FastAPI app to query Redis
└── SPARK-CHECKLIST.md              # Debug checklist for Spark streaming job
```


---

## 🧪 Simulate Watch Events

To simulate watch behavior from users (sends data to Kafka topic):

```bash
python simulate_watch_events.py
```

---

## ⚡ Real-Time Pipeline (Spark Submit)

Launches the Spark streaming job to consume Kafka messages and update Redis:

```bash
docker-compose build spark-submit
docker-compose up spark-submit
```

Make sure topic `watch_events` is created and Redis is reachable.

---

## 🌍 FastAPI Real-time API

Runs a simple REST API to query Redis-stored insights:

```bash
uvicorn main:app --reload --port 8000
```

- `GET /recommendations/{user_id}` → Returns list of recommendations

---

## 📁 .env Configuration

Inside `spark/app/.env`:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=watch_events
REDIS_HOST=redis
REDIS_PORT=6379
```

---

## 📄 Sample Kafka Topic Creation (Optional)

Create topic manually:

```bash
docker exec -it kafka kafka-topics --create --topic watch_events   --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
```

---

## ✅ Health Checklist

See [SPARK-CHECKLIST.md](SPARK-CHECKLIST.md) for end-to-end testing, log tracing, and common errors.

---


## 📬 Future Improvements

- Add PostgreSQL or BigQuery as offline warehouse
- Real-time user profiling with Redis TTLs
- Stream processing metrics with Prometheus/Grafana

---

## 📘 License

MIT License.