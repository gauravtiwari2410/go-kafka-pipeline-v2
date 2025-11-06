# Go Kafka Event Pipeline (v2) - metrics enabled

## Overview
This project is an end-to-end Go Kafka pipeline:
- Producer publishes events to Kafka
- Consumer consumes events, upserts to MS SQL, pushes failed payloads to Redis DLQ
- API server exposes endpoints and Prometheus `/metrics`

## What's new in v2
- Prometheus metrics:
  - `messages_processed_total` (counter)
  - `dlq_count_total` (counter)
  - `db_latency_seconds` (summary with p95)
- DLQ pushes increment `dlq_count_total`
- DB functions track latency and observe `db_latency_seconds`
- Logs correlate by `eventId` for tracing

## Quick start
1. Ensure Docker & Docker Compose installed.
2. From project root:
```bash
docker-compose up --build
```
3. Wait ~30-60s for services to initialize.
4. Check API:
```bash
curl http://localhost:8080/users/U1
curl http://localhost:8080/orders/O1
```
5. Check metrics:
```bash
curl http://localhost:8080/metrics
```

## Inspect DLQ
```bash
docker exec -it $(docker ps -qf "ancestor=redis:6-alpine") redis-cli LRANGE dlq 0 -1
```
