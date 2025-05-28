# 🎬 Netflix-Style Content Recommendation Pipeline

This project simulates a **real-time and batch data pipeline** for personalized content recommendations, inspired by the kind of infrastructure used at Netflix. It demonstrates complete data engineering capabilities — from data ingestion and processing to serving personalized recommendations.

---

## 🚀 Project Goals

- Simulate user watch activity using synthetic event streams
- Ingest and process data in real time using **Kafka + Spark Streaming**
- Aggregate historical watch patterns via **batch ETL** using **PySpark** or **dbt**
- Generate user preferences and recommend shows based on genre affinity
- Serve personalized recommendations through a **FastAPI** REST API
- Deploy components in containers using **Docker**

---

## 🧱 System Architecture

**Data Flow Overview**:

1. **Data Simulation**: Python script generates user watch events.
2. **Ingestion Layer**: Events are pushed to a Kafka topic (`watch_events`).
3. **Streaming Pipeline**: Spark Streaming reads Kafka events and updates real-time user preferences stored in Redis.
4. **Batch Pipeline**: A daily job (PySpark/dbt) aggregates long-term preferences from event logs and stores results in PostgreSQL.
5. **Serving Layer**: FastAPI service fetches both real-time and batch data to provide show recommendations per user.

---

## ⚙️ Tech Stack

| Layer              | Tools & Technologies                       |
|-------------------|--------------------------------------------|
| Data Generation    | Python                                     |
| Ingestion          | Apache Kafka                               |
| Stream Processing  | Spark Structured Streaming                 |
| Batch Processing   | PySpark / dbt                              |
| Storage            | Redis (real-time), PostgreSQL (batch)      |
| API Service        | FastAPI                                    |
| Containerization   | Docker                                     |
| (Optional) Monitoring | Prometheus + Grafana                    |

---

## 📂 Repository Structure

netflix-recommendation-pipeline/
├── data_simulation/ # User & show event simulators
├── kafka/ # Kafka + Zookeeper Docker setup
├── streaming/ # Spark Streaming jobs
├── batch/ # Batch ETL scripts or dbt models
├── api/ # FastAPI app for recommendations
├── storage/ # PostgreSQL schema, Redis config
├── monitoring/ # Grafana dashboards (optional)
├── docker-compose.yml # Multi-service orchestration
├── architecture.png # System diagram (optional image)
└── README.md
