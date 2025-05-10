#!/bin/sh
echo "Waiting for Kafka at $BOOTSTRAP_SERVERS..."

# Retry until Kafka is reachable
until nc -z broker 29092; do
  echo "Kafka not ready, sleeping..."
  sleep 2
done

echo "Kafka is up! Starting producer..."
exec python producer.py
