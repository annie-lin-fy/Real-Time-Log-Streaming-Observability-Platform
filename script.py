import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = ['kafka1:9092', 'kafka2:9092', 'kafka3:9092'] 
TOPIC_NAME = 'app-logs'

print("Initializing Kafka Producer...", flush=True)

producer = None
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000
        )
        print("Kafka Producer successfully initialized!", flush=True)
        break
    except KafkaError as e:
        print(f"Kafka not ready yet ({e}), retrying in 2 seconds...", flush=True)
        time.sleep(2)

services = ["auth-service", "payment-service", "cart-service", "user-service"]
methods = ["GET", "POST", "PUT", "DELETE"]
statuses = [200, 201, 400, 401, 404, 500]

while True:
    log_entry = {
        "timestamp": int(time.time()),
        "service_name": random.choice(services),
        "method": random.choice(methods),
        "status_code": random.choice(statuses),
        "response_time_ms": random.randint(10, 1500)
    }
    
    try:
        future = producer.send(TOPIC_NAME, value=log_entry)
        record_metadata = future.get(timeout=5)
        print(f"Sent successfully to topic: {record_metadata.topic}, partition: {record_metadata.partition}", flush=True)
    except Exception as e:
        print(f"Failed to send log entry: {e}", flush=True)
        
    time.sleep(1)