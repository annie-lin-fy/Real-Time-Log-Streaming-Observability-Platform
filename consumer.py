import os
import json
import time
import warnings
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime, timezone

# =========================
# CONFIG
# =========================
TOPIC_NAME = "app-logs"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092").split(",")
ELASTICSEARCH_SERVER = os.getenv("ELASTICSEARCH_SERVER", "http://elasticsearch:9200")

warnings.filterwarnings("ignore")


# =========================
# ES CONNECT (retry safe)
# =========================
print("🔌 Connecting to Elasticsearch...", flush=True)

es = None
while True:
    try:
        es = Elasticsearch(
            [ELASTICSEARCH_SERVER],
            request_timeout=10,
            retry_on_timeout=True,
            max_retries=3
        )
        info = es.info()
        print(f"✅ Elasticsearch connected: {info['cluster_name']}", flush=True)
        break
    except Exception as e:
        print(f"⏳ Waiting ES... {e}", flush=True)
        time.sleep(3)


# =========================
# KAFKA CONNECT (retry safe)
# =========================
print("🔌 Connecting to Kafka...", flush=True)

consumer = None
while True:
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="app-log-final-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,

            # stability (IMPORTANT for KRaft)
            session_timeout_ms=45000,
            heartbeat_interval_ms=10000,

            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        print("✅ Kafka connected & subscribed", flush=True)
        break

    except Exception as e:
        print(f"⏳ Waiting Kafka... {e}", flush=True)
        time.sleep(3)


# =========================
# MAIN LOOP
# =========================
print("🚀 Start consuming logs...", flush=True)

empty_poll_count = 0

try:
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        has_msg = False

        for tp, messages in msg_pack.items():
            for msg in messages:
                has_msg = True

                log_entry = msg.value

                # =========================
                # normalize timestamp
                # =========================
                log_entry["@timestamp"] = datetime.fromtimestamp(
                    log_entry["timestamp"],
                    tz=timezone.utc
                ).isoformat()

                print(
                    f"📥 Kafka msg: partition={msg.partition}, offset={msg.offset}",
                    flush=True
                )

                # =========================
                # index to ES — commit offset only on success
                # =========================
                try:
                    resp = es.index(
                        index="application-logs",
                        document=log_entry
                    )
                    print(f"✅ ES indexed: {resp['_id']}", flush=True)
                    consumer.commit()

                except Exception as e:
                    print(f"❌ ES error (offset NOT committed, will retry): {e}", flush=True)

        # =========================
        # idle detection
        # =========================
        if not has_msg:
            empty_poll_count += 1
            print(f"😴 idle ({empty_poll_count})", flush=True)
        else:
            empty_poll_count = 0


except KeyboardInterrupt:
    print("🛑 Stopping consumer...", flush=True)

finally:
    if consumer:
        consumer.close()