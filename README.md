# Real-Time Log Streaming & Observability Platform

A containerized log streaming pipeline that generates mock application logs, streams them through Apache Kafka, stores them in Elasticsearch, and visualizes infrastructure metrics via Prometheus and Grafana — all orchestrated with Docker Compose.

## Architecture

```
[log-generator] → Kafka (app-logs topic) → [log-consumer] → Elasticsearch
                                                                    ↑
[kafka-exporter] ──┐                                               │
[node-exporter]  ──┼→ Prometheus → Grafana                  Kibana / API
[es-exporter]    ──┘
```

| Service | Role |
|---|---|
| `log-generator` | Produces mock JSON logs (auth, payment, cart, user services) every second |
| `kafka-broker` | Single-node Kafka in KRaft mode; topic `app-logs` |
| `kafka-ui` | Web dashboard for browsing Kafka topics and messages |
| `log-consumer` | Consumes from Kafka, normalizes timestamps, indexes to Elasticsearch |
| `elasticsearch` | Stores logs in the `application-logs` index |
| `kafka-exporter` | Exports Kafka broker/consumer-lag metrics for Prometheus |
| `node-exporter` | Exports host CPU, RAM, and disk metrics |
| `elasticsearch-exporter` | Exports Elasticsearch cluster metrics |
| `prometheus` | Scrapes all exporters every 5 s |
| `grafana` | Visualization layer backed by Prometheus |

## Prerequisites

- Docker and Docker Compose

## Quick Start

```bash
docker compose up --build
```

All services start in dependency order. The consumer waits for both Kafka and Elasticsearch to be healthy before launching.

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Elasticsearch | http://localhost:9200 | — |
| Kafka Exporter | http://localhost:9308/metrics | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |

## Log Schema

Each log entry produced to Kafka:

```json
{
  "timestamp": 1718300000,
  "service_name": "auth-service",
  "method": "POST",
  "status_code": 200,
  "response_time_ms": 142
}
```

The consumer enriches entries with an `@timestamp` field (UTC ISO 8601) before indexing to Elasticsearch.

## Prometheus Scrape Targets

| Job | Target | Metrics |
|---|---|---|
| `node-metrics` | `node-exporter:9100` | CPU, RAM, disk |
| `kafka-metrics` | `kafka-exporter:9308` | Consumer lag, message counts |
| `elasticsearch-metrics` | `elasticsearch-exporter:9114` | Cluster health, indexing rate |

## Project Structure

```
infra-demo/
├── docker-compose.yml   # All 10 services
├── Dockerfile           # Image for log-consumer
├── script.py            # Kafka producer (log generator)
├── consumer.py          # Kafka consumer → Elasticsearch
├── prometheus.yml       # Prometheus scrape config
└── requirements.txt     # kafka-python-ng, elasticsearch
```

## Stopping

```bash
docker compose down
```
