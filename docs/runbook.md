# Runbook outline

1. Check API acceptance and HMAC failure rates.
2. Check Kafka broker health, partition availability, and consumer lag.
3. Check PostgreSQL availability, replica lag, lock waits, and deadlocks.
4. Check outbox oldest-pending age and relay retries.
5. Check DLQ volume and classify poison versus transient failures.
6. Check OTA rate limits, authentication expiry, and circuit-breaker state.
7. Check open oversell conflicts; close other channels and alert operations.
8. Check partition-maintenance horizon and create missing month partitions.
9. Check reconciliation drift by property timezone and date window.
10. Record incident actions in audit trail and verify recovery with replay/reconciliation.
