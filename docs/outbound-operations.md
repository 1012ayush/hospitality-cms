# Part 4A outbound and operations

## Reconciliation

`services/reconciler/src/index.ts` exports `reconcileProperty`, `runNightlyReconciliation`, `isOffPeakWindow`, and `KyselyReconciliationStore`. The worker reads PMS truth and OTA-reported availability across a configurable horizon (default 365 days), records per-connection drift in `sync_logs`, requests `fullSync` for drifted dates, and writes a corresponding `audit_log` row. Property IANA timezone determines the local date and default 02:00–04:00 execution window. A Redis per-property/day claim prevents duplicate concurrent runs; database audit and sync records remain durable authority. Concrete provider bindings and deployment CronJob remain deployment integration.

## Outbound

`@hcm/adapter-base` defines the adapter contract in `adapters/base/src/index.ts`. `@hcm/adapter-booking-com` maps canonical availability/rates/restrictions into internal commands; its injected transport must own partner wire serialization and network/auth specifics. Booking.com, Expedia Partner Central/EQC (not Rapid), and Airbnb connectivity are partner-gated. **ASSUMPTION – verify against partner docs** for all actual endpoints, request/response field names, authentication details, rate limits, and retry headers. Reservations are generally pulled with explicit acknowledgment; the polling worker ACKs only after database commit.

The outbound engine consumes `inventory.delta.events`, coalesces per-property/date deltas in Kafka offset order, debounces briefly, prioritizes reservation availability over bulk rates, applies atomic Redis token buckets and per-connection circuit breakers, and retries with bounded exponential jitter. Redis failure fails closed for delivery (Kafka offset remains uncommitted). Exhausted/permanent ARI delivery failures are durably recorded in `dlq_events` before a copy is published to `dead.letter.queue`; poison messages go directly to the Kafka DLQ. `pnpm --filter @hcm/outbound-engine replay-dlq <organizationId> <dlqEventId> [actorUserId]` republishes a validated ARI event and marks it replayed after Kafka acknowledges (at-least-once if the process crashes between send and mark). Delivery is at least once; adapters should make availability writes idempotent.

## Observability

Hot-path auto-instrumentation is bootstrapped by `packages/shared/src/telemetry.ts` before client creation. Export spans for gateway acceptance, raw-to-canonical normalization, reservation transaction, outbox relay, outbound OTA attempt, and reconciliation. Required metrics: webhook accepted/rejected and acceptance latency; reservation receive-to-propagation latency; inventory lock wait and transaction latency; oversell conflict count; outbox age/lag; Kafka consumer lag; per-connection OTA request, 429/5xx/auth failures, retries, circuit state, token wait; reconciliation drift count/age; DLQ depth/replay rate.

Alert rules (thresholds are initial SLO-aligned defaults):
1. Gateway durable 202 p99 >20ms for 5 minutes: page ingestion on-call.
2. Reservation received-to-propagated p99 >60s for 10 minutes, or >5m for any sustained constrained state: page channel operations.
3. Any internal invariant violation (booked units outside [0,total]): page database/on-call immediately; expected count is zero.
4. Open oversell conflict not detected/alerted within 2m, or unresolved conflict age >10m: page operations.
5. Outbox oldest pending >30s or DLQ growth >10/min: page platform/channel operations.

## Known deployment boundary

The design does not imply OTA freshness guarantees: external propagation latency can still create oversells. The accepted booking remains authoritative, is flagged for conflict handling, and operations mitigation closes other channels. Partner transports, their credentials and quotas, production metric exporter configuration, and AWS deployment resources require environment/partner-specific integration.
