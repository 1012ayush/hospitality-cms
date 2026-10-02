# Production-oriented inbound path (Part 3)

## Flow

`ALB/API gateway (mTLS termination) -> Fastify gateway -> PCI proxy hook -> webhook.raw.events -> normalizer -> reservation.canonical.events / inventory.delta.events -> core state engine -> PostgreSQL transaction + outbox -> outbox relay -> Kafka`. The gateway does not open a PostgreSQL connection. It waits for Kafka acknowledgment before returning 202.

The HTTP route is the internal normalized contract `POST /v1/webhooks/:propertyId/:connectionId`; internal HMAC headers are `x-hcm-timestamp` and `x-hcm-signature`, over `<unix-seconds>.<exact raw request bytes>`. Partner signature/header mappings are **ASSUMPTION – verify against partner docs** and belong in the connection-specific adapter, not in the generic verifier. Replay protection fails closed if Redis is unavailable.

The gateway's `PciProxy` runs before Kafka. `MockPciProxy` is local-only and strips common card/VCC shaped fields; it is not a payment tokenization service. Production must inject an approved PCI proxy implementation. No PAN/CVV is part of the canonical schema or database schema.

## Reservation processing

The generic normalizer expects the partner adapter to place a validated canonical CloudEvent at `payload.canonicalEvent`. It does not guess Booking.com/Expedia/Airbnb wire formats. Malformed or unsupported canonical events publish a reason-coded `com.hospitality.event.dead_lettered` event, then commit the source offset only after Kafka confirms the output. Kafka transport failures are retried by leaving the source offset uncommitted.

`applyReservationEvent` uses `withTenant` to switch to `hcm_app` and set transaction-local `app.organization_id`. Kafka is keyed by property ID for in-partition ordering, but row locks and PostgreSQL constraints provide correctness across retries and direct concurrent calls. Redis is a best-effort exact-replay cache; no Redis inventory lock is used. Each inventory lock is acquired in `(room_type_id, stay_date)` order.

The inventory transaction stores nightly allocation in `reservation_nights.inventory_counted`. Existing allocated units are released on modify/cancel; oversold nights are not decremented later. A booking confirmed by a channel is retained, flagged oversold, written to `oversell_conflicts`, and produces a prioritized auto-close outbox event. The outbox relay leases one property head at a time, publishes outside the DB transaction, and does not allow a later property event to pass a pending or dead-lettered predecessor.

## Pull-based partners

`PullReservationAdapter` provides fetch and explicit acknowledgment methods. `processPollingBatch` only acknowledges after a committed result; if acknowledgment fails, a later poll can safely redeliver and the database idempotency ledger handles it. Booking.com, Expedia Partner Central/EQC (not Rapid), and Airbnb connectivity APIs are partner-gated. Reservations are mostly pulled with explicit acknowledgment. Concrete API, auth, cursor, field and rate-limit behavior is **ASSUMPTION – verify against partner docs**.

## Operations and verification

- `DATABASE_URL` must be a login principal allowed to `SET ROLE hcm_app`; `WORKER_DATABASE_URL` must be a distinct restricted principal allowed to `SET ROLE hcm_worker`.
- The worker role has `BYPASSRLS`; isolate its credentials and keep tenant predicates/context in worker queries.
- `pnpm test:integration` runs Testcontainers PostgreSQL 16 coverage for concurrency, duplicate, stale sequence, modify/cancel, commit-before-offset crash redelivery, and Redis failure. Docker must be available.
- Migration sequence continues through `0013_outbox_partition_order.sql`; refresh `atlas.sum` before deploy. Pending legacy outbox rows without a property ID intentionally stop that migration for operator repair.
