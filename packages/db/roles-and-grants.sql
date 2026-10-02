-- Run as the database owner/migration administrator after migrations.
-- Roles are NOLOGIN groups; provision separate login principals in AWS Secrets Manager
-- and grant membership. hcm_app is RLS-scoped; hcm_worker deliberately has cross-tenant
-- BYPASSRLS for trusted fleet workers and must never be granted to request-facing APIs.
CREATE ROLE hcm_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
CREATE ROLE hcm_worker NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT BYPASSRLS;

GRANT USAGE ON SCHEMA hcm TO hcm_app, hcm_worker;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA hcm TO hcm_app;
REVOKE UPDATE, DELETE ON hcm.audit_log FROM hcm_app;
REVOKE UPDATE, DELETE ON hcm.outbox_events FROM hcm_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA hcm TO hcm_app;

-- Worker access deliberately crosses tenant RLS for outbox relay/reconciliation.
-- Restrict membership to isolated worker deployments; the role is not a human or API role.
-- Workers should still SET LOCAL app.organization_id in each transaction for explicit
-- query scoping, observability, and audit correlation, despite the role-level RLS bypass.
GRANT SELECT ON hcm.organizations TO hcm_worker;
GRANT SELECT, INSERT, UPDATE ON hcm.outbox_events TO hcm_worker;
GRANT SELECT, INSERT ON hcm.processed_events, hcm.sync_logs, hcm.oversell_conflicts,
  hcm.audit_log TO hcm_worker;
GRANT SELECT, INSERT, UPDATE ON hcm.dlq_events TO hcm_worker;
GRANT SELECT, INSERT, UPDATE ON hcm.inventory_days, hcm.rate_days,
  hcm.reservations, hcm.reservation_rooms, hcm.reservation_nights TO hcm_worker;
GRANT SELECT ON hcm.properties, hcm.room_types, hcm.rate_plans, hcm.channel_connections,
  hcm.channel_mappings, hcm.allotments, hcm.allotment_channel_caps TO hcm_worker;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA hcm TO hcm_worker;

-- Table owner (the migration role) must own the parents/functions. The SECURITY DEFINER
-- wrapper has a fixed search_path and exposes only the bounded monthly partition action.
CREATE OR REPLACE FUNCTION hcm.worker_ensure_month_partitions(p_month date)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, hcm
AS $$ BEGIN
  IF p_month <> date_trunc('month', p_month)::date
     OR p_month < date_trunc('month', current_date)::date
     OR p_month > (date_trunc('month', current_date) + interval '24 months')::date THEN
    RAISE EXCEPTION 'partition month must be month-start within current to +24 months';
  END IF;
  PERFORM hcm.ensure_hcm_month_partitions(p_month);
END $$;
REVOKE ALL ON FUNCTION hcm.worker_ensure_month_partitions(date) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION hcm.worker_ensure_month_partitions(date) TO hcm_worker;
REVOKE ALL ON FUNCTION hcm.ensure_month_partition(regclass,date) FROM PUBLIC;
REVOKE ALL ON FUNCTION hcm.ensure_hcm_month_partitions(date) FROM PUBLIC;

-- Apply defaults for tables created by the migration owner in this schema.
ALTER DEFAULT PRIVILEGES IN SCHEMA hcm GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO hcm_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA hcm GRANT USAGE, SELECT ON SEQUENCES TO hcm_app;

-- Tenant context examples (the value must come from authenticated server-side claims,
-- never from an untrusted request parameter):
-- Login principals granted membership must SET ROLE hcm_app / hcm_worker at connection start
-- (or use an explicitly configured INHERIT membership); both group roles are NOLOGIN/NOINHERIT.
-- App:    BEGIN; SET LOCAL app.organization_id = '<authorized-org-uuid>'; ... COMMIT;
-- Worker: BEGIN; SET LOCAL app.organization_id = '<trusted-org-uuid>'; ... COMMIT;
-- BYPASSRLS is global to the database role: secure worker credentials, isolate deployments,
-- and retain organization_id predicates in every worker query as defense in depth.

-- Mutator replaces the child-night/room projection atomically when a reservation is modified.
GRANT DELETE ON hcm.reservation_rooms, hcm.reservation_nights TO hcm_app;
