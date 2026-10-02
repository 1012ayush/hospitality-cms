# ADR-002: Redis is not an inventory correctness lock

- Status: accepted.
- Decision: PostgreSQL row locks and constraints enforce inventory correctness. Redis is an optional contention/idempotency fast path only; no Redis lease is required initially.
- Rationale: leases can expire or be lost during failover. Partition ordering reduces concurrency but does not replace database serialization.
