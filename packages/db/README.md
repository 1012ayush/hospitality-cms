# Database package

Migrations use Atlas's versioned SQL directory. Apply each numbered migration in order; each file is its own migration step. On first setup, generate and commit the migration checksum manifest with `pnpm db:hash`; then run `pnpm db:migrate`. Do not regenerate the manifest to conceal edits to already-applied migrations. `0009_indexes_concurrently.sql` has `-- atlas:txmode none` because `CREATE INDEX CONCURRENTLY` cannot run inside a transaction. PostgreSQL does not support concurrent index creation directly on a partitioned parent in PostgreSQL 16, so those indexes are created transactionally in `0008` and inherited by partitions.

Set `DATABASE_URL` to the migration-role connection and run `pnpm db:migrate`. Never use the migration credential in services.

The custom tenant GUC is a trusted server-side context, not a cryptographic identity: derive it from authenticated claims, set it transaction-locally, and never accept it from an untrusted caller. Runtime login principals should explicitly `SET ROLE hcm_app` or `hcm_worker` because group roles are `NOINHERIT`.
