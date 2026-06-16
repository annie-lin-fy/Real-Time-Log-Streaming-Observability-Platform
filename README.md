# Real-Time Log Streaming & Observability Platform

A containerized log streaming pipeline that generates mock application logs, streams them through a **3-node Apache Kafka cluster (KRaft)**, stores them in Elasticsearch, and visualizes infrastructure metrics via Prometheus and Grafana — all orchestrated with Docker Compose.

## Architecture

```
                       ┌──────────────────────────────────────────┐
                       │         Kafka Cluster (KRaft)            │
[log-generator] ──→    │  kafka1 ── kafka2 ── kafka3              │  ──→  [log-consumer] ──→ Elasticsearch
  (script.py)          │        topic: app-logs                   │        (consumer.py)        │
                       └──────────────────────────────────────────┘                             │
                                        │                                                Kibana / API
                                    kafka-ui
                                  (topic browser)

[kafka-exporter]  ──┐
[node-exporter]   ──┼──→  Prometheus  ──→  Grafana
[es-exporter]     ──┘
```

## Services

| Service | Role |
|---|---|
| `kafka1`, `kafka2`, `kafka3` | 3-node Kafka cluster in KRaft mode (combined broker + controller); topic `app-logs` with replication factor 3 |
| `kafka-init` | One-shot job: creates `app-logs` topic (3 partitions, RF=3) after all brokers pass health checks; exits on completion |
| `kafka-ui` | Web dashboard for browsing Kafka topics and messages |
| `log-consumer` | Consumes from Kafka, normalizes timestamps, indexes documents to Elasticsearch; restarts automatically on failure |
| `elasticsearch` | Stores logs in the `application-logs` index (single-node, security disabled) |
| `kafka-exporter` | Exports Kafka broker and consumer-lag metrics for Prometheus |
| `node-exporter` | Exports host CPU, RAM, and disk metrics |
| `elasticsearch-exporter` | Exports Elasticsearch cluster health and indexing metrics |
| `prometheus` | Scrapes all exporters every 5 s |
| `grafana` | Visualization layer with a pre-built Kafka Observability Dashboard |

> **Note:** The log-generator (`script.py`) is **not** a Docker Compose service — run it separately (see [Running the Log Generator](#running-the-log-generator)).

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ (only needed to run the log generator outside Docker)

## Quick Start

```bash
# Start all 11 services
docker compose up --build
```

All services start in dependency order. The consumer container waits for both Kafka brokers and Elasticsearch to be reachable before launching `consumer.py`.

### Running the Log Generator

The producer script connects to the Kafka cluster and sends one mock log entry per second:

```bash
pip install kafka-python-ng
python script.py
```

> If running outside Docker, update `KAFKA_BOOTSTRAP_SERVERS` in `script.py` to use the host-mapped ports (e.g., `localhost:19092`).

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Kafka Broker 1 | `localhost:19092` | — |
| Kafka Broker 2 | `localhost:19093` | — |
| Kafka Broker 3 | `localhost:19094` | — |
| Elasticsearch | http://localhost:9200 | — |
| Node Exporter | http://localhost:9100/metrics | — |
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

## Grafana Dashboard

A pre-built dashboard definition is included in `generate_dashbord.json` with the following panels:

| Panel | PromQL Query | Source | Thresholds |
|---|---|---|---|
| Kafka Under-Replicated Partitions | `sum(kafka_topic_partition_under_replicated_partition) by (topic)` | `kafka-exporter` | 0 = green, >0 = red |
| Consumer Lag | `sum by (consumergroup) (kafka_consumergroup_lag)` | `kafka-exporter` | 0–99 green, 100 yellow, 1 000 orange, 5 000 red |
| Active Broker Count | `kafka_brokers` | `kafka-exporter` | 3 = green, 2 = yellow, 1 = orange, 0 = red |
| Elasticsearch Indexing Rate | `rate(elasticsearch_indices_indexing_index_total[1m])` | `elasticsearch-exporter` | — |

> **Note:** Per-process Kafka broker CPU (`process_cpu_seconds_total`) is not available in this setup — it requires a JMX exporter. **Active Broker Count** (`kafka_brokers`) is used instead as a more actionable cluster-health signal.

Import it via the Grafana UI (**Dashboards → Import → Upload JSON file**).

## Anomaly Simulation

To simulate a **consumer lag & processing bottleneck**:

```bash
# Freeze the consumer — Kafka keeps receiving logs, lag accumulates
docker pause log-consumer

# Observe in Grafana:
#   Consumer Lag:              climbs steadily → peaks at session timeout (~45 s)
#   ES Indexing Rate:          drops toward 0
#   Active Broker Count:       stays at 3  (Kafka is healthy)
#   Under-Replicated Partitions: stays at 0  (no broker issue)

# Restore — consumer drains backlog, ES rate bursts back to normal
docker unpause log-consumer
```

This scenario demonstrates that rising consumer lag + falling ES rate with healthy broker metrics points to a **consumer-side bottleneck**, not a broker or replication failure.

## Project Structure

```
infra-demo/
├── docker-compose.yml        # All 11 services (3 Kafka + UI + ES + consumer + 3 exporters + Prometheus + Grafana)
├── Dockerfile                # Image for log-consumer
├── script.py                 # Kafka producer — log generator (run separately)
├── consumer.py               # Kafka consumer → Elasticsearch indexer
├── prometheus.yml            # Prometheus scrape configuration
├── generate_dashbord.json    # Grafana dashboard definition (importable JSON)
└── requirements.txt          # kafka-python-ng, elasticsearch>=8,<9
```

## Stopping

```bash
docker compose down
```

Add `-v` to also remove Kafka data volumes:

```bash
docker compose down -v
```
