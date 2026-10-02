# ADR-001: Kafka as the event backbone

- Status: accepted.
- Decision: Apache Kafka with KafkaJS; key property-scoped events by `property_id`.
- Rationale: durable replayable event streams and per-key partition ordering for partner ingress, canonical events, inventory deltas, and DLQ workflows.
- Boundary: PostgreSQL transactions remain the source of truth; use a transactional outbox because Kafka and PostgreSQL do not share an atomic commit.
