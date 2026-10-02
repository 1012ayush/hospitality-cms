# Hotel Channel Manager scaffold

Production-oriented monorepo scaffold. This Part 2B supplies repository structure, PostgreSQL migrations and role grants, local infrastructure, CI wiring, partition job specification, and pgTAP tests. Business logic is intentionally not implemented in this part.

External OTA connectivity is partner-gated. Booking.com, Expedia Partner Central/EQC (not Rapid), and Airbnb APIs must be contract-enabled. Reservations are mostly pulled by polling with explicit acknowledgment. **ASSUMPTION – verify against partner docs** for endpoints, payload fields, auth, rate limits, cursors, and acknowledgment contracts.

Migrations use Atlas versioned SQL because SQL is the deliverable and Atlas natively tracks SQL migration files/checksums. `CREATE INDEX CONCURRENTLY` is isolated in `0009_indexes_concurrently.sql` with Atlas transaction mode disabled. Apply migrations in numeric order. Workers use tenant-scoped RLS and do not have `BYPASSRLS`.
