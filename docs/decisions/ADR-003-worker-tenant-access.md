# ADR-003: Cross-tenant worker role

- Status: accepted for trusted background workers only.
- Decision: `hcm_worker` is a `NOLOGIN` group role with PostgreSQL's `BYPASSRLS` attribute; `hcm_app` remains `NOBYPASSRLS` and subject to tenant RLS.
- Reason: outbox relay and nightly reconciliation must claim work across organizations without enumerating tenants through the request-serving application role.
- Access boundary: grant `hcm_worker` membership only to dedicated worker login principals. Never grant it to Fastify, dashboard, admin API, human users, or the migration principal. Do not use worker credentials in request-facing processes.
- Query rule: even though PostgreSQL bypasses row policies for the role, every worker query must include `organization_id` when work is scoped to one tenant. Workers set `SET LOCAL app.organization_id` from a trusted queue/control-plane item for traceability; this context is not an authorization boundary for the bypass role.
- Audit: record worker identity, tenant ID, job/run ID, and affected entity in `audit_log` or structured telemetry. Rotate secrets and alert on unusual cross-tenant access.

## Reservation source version compare rule

`channel_version` stays as opaque partner metadata and must never be lexically sorted. Compare incoming `source_sequence` with the stored value for identity `(organization_id, channel_connection_id, external_reservation_id)`: apply only if incoming sequence is strictly greater; equal means duplicate/replay; lower means stale/out of order and must not change reservation or inventory. `source_sequence` is nonnegative and backfilled to `0` for pre-migration rows. Partner adapters must normalize a true monotonic numeric revision. If a partner has no such revision, **ASSUMPTION – verify against partner docs**; do not pretend its text version is sortable. An adapter-specific serialized revision allocator or timestamp/receipt policy requires a separately reviewed decision.
