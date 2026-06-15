FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD sh -c "\
    echo '⏳ [Sentry] Waiting for Kafka Broker (9092)...' && \
    until python -c 'import socket; s = socket.socket(); s.settimeout(2); s.connect((\"kafka1\", 9092))' 2>/dev/null; do sleep 3; done; \
    echo '⏳ [Sentry] Waiting for Elasticsearch (9200)...' && \
    until python -c 'import socket; s = socket.socket(); s.settimeout(2); s.connect((\"elasticsearch\", 9200))' 2>/dev/null; do sleep 3; done; \
    echo '🚀 [Sentry] All services are up! Launching Python Consumer...'; \
    python -u consumer.py"