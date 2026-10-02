#!/usr/bin/env bash
set -euo pipefail

# Local Kafka runs as a single broker, so replication factor is one.
# Production topic replication/min-ISR must be set from the cluster's AZ count and policy.
BROKER="${KAFKA_BROKERS_IN_CONTAINER:-kafka:9092}"
PARTITIONS="${KAFKA_PARTITIONS:-48}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
TOPICS=(
  webhook.raw.events
  reservation.canonical.events
  inventory.delta.events
  dead.letter.queue
)

for topic in "${TOPICS[@]}"; do
  echo "Creating topic=${topic} partitions=${PARTITIONS} replication=${REPLICATION_FACTOR}"
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BROKER}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"
done

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BROKER}" --list
