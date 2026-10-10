# Deployment: live ARI data and sync status

This guide applies the dashboard/API changes in this checkout. The current live Render/Vercel deployments have not been inspected or redeployed from here.

## What the UI reads

- `apps/testing-dashboard` proxies server-side to `DASHBOARD_API_BASE_URL`; production returns HTTP 503 instead of demo fixtures when it is missing.
- `services/admin-api` reads the tenant's actual `properties`, `room_types`, `inventory_days`, `rate_days`, `oversell_conflicts`, `channel_connections`, and `sync_logs`. Set `DASHBOARD_ORGANIZATION_ID` to the real tenant UUID in the database. The route uses `withTenant` and requires `DASHBOARD_API_TOKEN`.
- Dates are selected/rendered in each property's IANA `timezone`. Sync times are formatted from the persisted `logged_at` timestamp in that same zone. Browser/server timezone is not the authority.
- The Reservation mutator now records inbound applied, stale, and database-detected duplicate outcomes in `sync_logs` in the same DB transaction. Lock wait, DB commit, and HTTP response metrics are not persisted by this path and correctly display as unavailable.

## Required deployment wiring

1. Deploy from the updated repository revision. Deploying only the Vercel frontend cannot add the Admin API routes or database query behavior.
2. Keep one Admin API service. Configure its `DATABASE_URL`, `DASHBOARD_ORGANIZATION_ID`, and a high-entropy `DASHBOARD_API_TOKEN`. Its DB principal must have the intended RLS policy access. Do not expose the token as a `NEXT_PUBLIC_*` variable.
3. Configure Vercel server-side variables `DASHBOARD_API_BASE_URL=https://<admin-api-service-host>` and `DASHBOARD_API_TOKEN` to the same token. Set `DASHBOARD_AUTH_PASSWORD`; do not set `DASHBOARD_AUTH_DISABLED=true` in production.
4. Run the database migrations against the same production database used by Admin API and state workers. Load/import real room types and `inventory_days`; load `rate_plans`/`rate_days` if rates should appear. The development seed is not OTA connectivity and its sample connection is disabled.
5. The event pipeline needs running worker deployments, not just HTTP services: `@hcm/normalizer` consumes `webhook.raw.events`; `@hcm/core-state-engine` consumes `reservation.canonical.events` and relays `outbox_events`. All must use the same Kafka cluster, Redis, and DB; configure `KAFKA_BROKERS`, `REDIS_URL`, `DATABASE_URL`, and the core relay's restricted `WORKER_DATABASE_URL`.
6. For real OTA delivery, deploy an outbound worker wired to a real `ChannelTargetProvider` and partner-approved adapter/credentials. `services/outbound-engine` exports the consumer/dispatcher but has no ready production bootstrap/provider composition in this repo. No API key or demo fixture can substitute for partner approval.
7. The current gateway's production `main()` deliberately fails closed because a real Secrets Manager resolver and PCI proxy are not integrated. The mock HMAC resolver and mock PCI proxy are development-only. Do not run this implementation in production until those integrations exist. A working simulator response from a deployed host may be coming from an older/different revision or another gateway; compare Render's deployed commit and start logs.

## Render / Vercel checks

- For each Render service, verify repository, branch, deployed commit SHA, root directory (monorepo root), build command, start command, runtime environment variables, and latest deploy logs. Do not leave both `hcm-ingestion-gateway` and `hcm-ingestion-gateway1` receiving dashboard traffic unless each is intentionally configured to the same pipeline.
- Render HTTP services: Admin API and gateway. Render background workers: normalizer and core-state-engine. The core-state-engine exposes `/health` while its Kafka/DB loops run; a healthy HTTP health check alone does not prove consumer progress.
- Vercel must point to the Admin API host, not an ingestion gateway or core worker. The browser should call `/api/dashboard/snapshot`; this Next route proxies to `/v1/dev-dashboard/snapshot`. Sync Inspector calls `/api/dashboard/sync-logs`, which proxies to `/v1/dev-dashboard/sync-logs`.
- After deployment, open those two Vercel proxy paths while authenticated. Expected: JSON with `demo:false` and real property IDs for snapshot; `{ "logs": [...] }` for sync logs. A 503 indicates missing configuration, 401 indicates token mismatch, 404 indicates wrong/stale upstream service or revision, and 500 means inspect Admin API logs for SQL/schema mismatch.
- Submit a simulator event and follow the same `event.id` through Kafka consumer logs, `processed_events`, `reservations`, `inventory_days`, `outbox_events`, and `sync_logs`. Gateway HTTP 202 is only raw payload acceptance; it does not assert state mutation or OTA delivery.
- Verify hotel-local time by comparing an actual `sync_logs.logged_at` instant against the property's timezone. Do not change `TZ` or manually add/subtract hours; keep timestamps as `timestamptz`/ISO UTC and apply IANA timezone only for display.

## Known limitations

`sync_logs` currently records reservation ingestion outcomes and reconciler activity, not successful outbound OTA acknowledgments. Add outbound-attempt persistence and a production target-provider bootstrap before presenting outbound sync as complete. The current seed has inventory rows but no rate-day prices. Real booking/availability data requires connected PMS/OTA feeds and partner-gated credentials; external endpoints and payload semantics remain **ASSUMPTION – verify against partner docs**.
