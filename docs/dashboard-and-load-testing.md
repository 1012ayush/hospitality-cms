# Part 4B: dashboard and load testing

## Dashboard

`apps/testing-dashboard` is dev/test-only. It requires HTTP Basic auth by default (`DASHBOARD_AUTH_PASSWORD`). `DASHBOARD_AUTH_DISABLED=true` bypasses auth only when `NODE_ENV` is not `production`; a production build fails closed with 503 if the bypass is set, and the simulator route is disabled in production.

Run locally with `DASHBOARD_AUTH_DISABLED=true pnpm dashboard:dev`; set `DASHBOARD_AUTH_PASSWORD` instead to exercise HTTP Basic authentication. The Basic username is `hcm`. The dashboard polls every five seconds. `/api/dashboard/snapshot` and `/api/dashboard/sync-logs` proxy the internal Admin API; the browser never connects directly to PostgreSQL. Expected internal API contracts are `GET /v1/dev-dashboard/snapshot?propertyId=...` returning `DashboardSnapshot` including each property’s usable `connections` (`channelConnectionId`, `channel`, `label`) and `GET /v1/dev-dashboard/sync-logs?propertyId=...&channelConnectionId=...&limit=...&after=...` returning `{logs: SyncLog[]}`. `DASHBOARD_API_BASE_URL` and optional `DASHBOARD_API_TOKEN` configure this server-side connection. When no upstream exists, non-production mode presents labeled demo data. Production returns 503 instead of demo data.

`ARIMatrixView` renders 60/90/120 days and virtualizes date columns with a horizontally scrollable window; each cell displays units, nightly price, stop-sell/CTA/CTD, oversell status and sync state. `WebhookSimulator` sends canonical fixtures through the configured gateway and supports Booking.com new/modify/cancel, Airbnb fixture, PMS ARI rate-change fixture, same-key redelivery, and 1–50 parallel last-room requests. HMAC signing happens in the server route using `DASHBOARD_SIMULATOR_HMAC_SECRET`. A 202 only proves durable raw acceptance. Configure `DASHBOARD_INVARIANT_API_URL` to an authenticated test observer implementing `POST /v1/dev-dashboard/invariant-check` and returning `{availableUnitsNonnegative, oversoldBookingsRetained}` to show actual state pass/fail; without it, the UI correctly reports `not_verified`. `SyncInspector` filters by property/channel and displays lock wait, DB commit, outbound status, and HTTP response.

Partner labels in simulator scenarios identify the target fixture only. They do not encode partner API wire formats. Booking.com, Expedia Partner Central/EQC (not Rapid), and Airbnb connectivity are partner-gated; reservations are mostly pulled with explicit acknowledgment. Any concrete partner endpoint, field, auth, quota, polling cursor, or acknowledgment mapping is **ASSUMPTION – verify against partner docs**.

## Load scripts and exact pass/fail criteria

Scripts: `tests/load/steady.js` runs **166 iterations/s for 10 minutes**; `tests/load/burst.js` runs **500 iterations/s for 2 minutes**. Each iteration submits one canonical reservation webhook, so this is a reservation-heavy gateway/partition stress profile, not the Part 1 mixed workload. Both sign the exact raw body with the gateway's normalized HMAC convention. Environment: `GATEWAY_URL`, `WEBHOOK_HMAC_SECRET`, `PROPERTY_ID`, `CHANNEL_CONNECTION_ID`, `ORGANIZATION_ID`, `ROOM_TYPE_ID`; optional `PROPERTY_TIMEZONE`, `PROPAGATION_OBSERVER_URL`, `PROPAGATION_OBSERVER_TOKEN`.

Run with k6 installed: `k6 run tests/load/steady.js` and `k6 run tests/load/burst.js`. Pass criteria: achieved constant arrival rate (k6 reports no dropped iterations); HTTP 202 rate >99.9%; gateway acceptance p99 <20ms; failed gateway requests <0.1%. If `PROPAGATION_OBSERVER_URL` is configured, each accepted reservation is polled until propagation or 60s timeout and the p99 `reservation_propagation_ms` must be <60,000ms. This measures propagation only if the observer observes the outbound completion boundary; it must not merely report Kafka enqueue.

Watch PostgreSQL: CPU, active/pool connections, transaction commits/sec, `inventory_days` lock wait, deadlocks, statement/commit p99, WAL throughput, replica lag, rejected check constraints. Watch Kafka: produce request rate/latency/errors, broker request queue, under-replicated partitions, ISR changes, per-group consumer lag, rebalances, DLQ rate, outbox oldest age. For burst, confirm the consumer lag drains and outbox age returns below 30s after traffic stops. Run these only against isolated partner sandbox/test credentials and a non-production database.

## Operational runbook outline

1. Check gateway 202 rate, p99, HMAC failures, replay rejects and dropped k6 iterations.
2. Check Kafka broker health, ISR/under-replicated partitions, partition ownership and consumer lag.
3. Check PostgreSQL health, active pool sessions, `inventory_days` lock waits, deadlocks and commits.
4. Check outbox oldest-pending age and relay retry count; verify age returns below 30s after recovery.
5. Check DLQ count/reason; replay only after correcting the source cause and preserve audit context.
6. Check per-connection 429/5xx/auth failures, token wait and circuit state; honor contracted partner quotas.
7. Review open oversell conflicts; leave the confirmed reservation intact, close other channels, alert operations and record resolution.
8. Check partition maintenance horizon and DEFAULT partition growth for `inventory_days` and `sync_logs`.
9. Check reconciliation drift by property IANA timezone and local off-peak window; verify fullSync completion.
10. Record incident actions in `audit_log`, confirm monitoring recovered, then run a bounded replay/reconciliation.

## Go-live partner documentation checklist

- Obtain current partner approval and exact program/product for Booking.com connectivity, Expedia Partner Central/EQC (not Rapid), and Airbnb connectivity.
- Verify each partner's exact endpoint, HTTP method, payload field names, required headers, response fields and supported ARI/restriction capabilities.
- Verify auth grant/exchange, token lifetime, secret rotation, scopes, tenant/property binding and revocation process.
- Verify quotas, burst rules, 429 and 5xx retry guidance, `Retry-After` semantics, idempotency support and circuit-breaker recovery behavior.
- Verify reservation pull cursor/pagination, polling window, ordering/version guarantees, cancellation/modify/no-show semantics, explicit acknowledgment timing and redelivery behavior.
- Verify availability/rate full-sync limits, maximum date range, room/rate mapping identifiers, timezone and currency rules, and partner confirmation/readback semantics.
- Verify webhook signing algorithm, signing bytes, timestamp/replay tolerance and retry policy wherever callbacks are enabled; do not assume the internal `x-hcm-*` signature format is the partner format.
- Verify PCI/VCC handling contract and approved proxy/tokenization configuration; prove raw card/VCC data never reaches Kafka, logs, DLQ or dashboard snapshots.
- Validate sandbox concurrency, stale event, duplicate, timeout, 429, outage recovery and reconciliation scenarios before production credentials are enabled.
