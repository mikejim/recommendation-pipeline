# ✅ Spark Streaming Job: Health & Debug Checklist

Use this checklist to verify your Spark Structured Streaming job is working properly in your Dockerized pipeline (Kafka + Spark + Redis).

---

## ✅ 1. Spark Web UI
- [ ] Visit [http://localhost:8080](http://localhost:8080)
- [ ] Verify your Spark application appears under "Running Applications"
- [ ] Check status: `RUNNING`, not `FAILED`
- [ ] Confirm streaming batches are being processed

---

## ✅ 2. Docker Logs
- [ ] Run: `docker-compose logs -f spark-submit`
- [ ] Look for logs indicating:
  - Starting StreamingQuery
  - Reading from Kafka
  - Writing to Redis (or other sink)
  - Errors or stack traces

---

## ✅ 3. Kafka Events Ingested
- [ ] Is your Kafka topic receiving events?
- [ ] Run the event simulator (e.g., `simulate_watch_events.py`)
- [ ] Check if Spark receives those events in logs

---

## ✅ 4. Redis State Updates (if applicable)
- [ ] Run: `docker exec -it <redis-container> redis-cli`
- [ ] Run: `KEYS *`
- [ ] Run: `GET <your-key>`
- [ ] Confirm data written by Spark appears here

---

## ✅ 5. Data Flow Debugging Tips
- [ ] Use print/debug statements in your PySpark script
- [ ] Add logging at key stages: Kafka read, transformations, Redis write
- [ ] Ensure environment variables (e.g. `REDIS_HOST`, `KAFKA_BOOTSTRAP_SERVERS`) are correctly loaded

---

## 🧪 Optional Checks
- [ ] Confirm `checkpoint` directory is populated
- [ ] Use `query.status` and `query.lastProgress` in PySpark for diagnostics
- [ ] Validate error handling in Spark for Kafka connection retries

---

## 🛑 Common Errors & Fixes
| Symptom | Fix |
|--------|-----|
| `Missing application resource` | Check Dockerfile COPY path |
| `NoBrokersAvailable` | Kafka isn't ready or wrong host/port |
| `Connection refused (Redis)` | Wrong REDIS_HOST or Redis container not up |
| `UnknownTopicOrPartitionException` | Topic not created yet — create it or add delay |
| `ModuleNotFoundError` | Missing Python deps — confirm requirements.txt |

---

**Keep this file in your repo (`SPARK-CHECKLIST.md`) for quick debugging.**