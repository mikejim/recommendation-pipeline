# 🎬 Netflix-Style Content Recommendation Pipeline

This project simulates a **real-time + batch data pipeline** for personalized content recommendations, inspired by Netflix's data infrastructure. It demonstrates end-to-end data engineering skills: event ingestion, streaming and batch processing, preference modeling, and API delivery.

---

## 🚀 Project Goals

- Simulate user watch behavior as streaming events
- Process and store data in real time using **Kafka + Spark Streaming**
- Aggregate historical preferences using **batch ETL** with **PySpark/dbt**
- Generate recommendations using real-time + batch layers
- Expose recommendations through a **FastAPI** service
- Orchestrate components using **Docker**

---

## 🧱 Architecture

```mermaid
graph TD
    A[Simulated Users] --> B[Kafka Topic: watch_events]
    B --> C1[Spark Streaming Job]
    B --> C2[Batch ETL (PySpark/dbt)]
    C1 --> D1[Redis (Real-time Store)]
    C2 --> D2[PostgreSQL / Data Lake]
    D1 --> E[FastAPI Recommendation API]
    D2 --> E
