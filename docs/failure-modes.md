# Failure modes

Reference the Part 1 architecture failure-mode table. The operating system must monitor Redis degradation, PostgreSQL failover, Kafka broker/consumer lag, outbox age, partner 429/5xx responses, DLQ growth, stale/duplicate events, timezone drift, PMS push failures, and property partition skew. Recovery uses bounded retry, idempotent replay, explicit DLQ handling, and reconciliation; confirmed OTA reservations are not silently discarded.
