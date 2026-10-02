-- Fix reservation identity/version semantics, add safe partition fallbacks, and
-- explicitly authorize trusted cross-tenant worker roles to bypass RLS.
SET search_path = hcm, public;

LOCK TABLE hcm.reservations IN ACCESS EXCLUSIVE MODE;

-- Existing installations may already contain one row per textual source version.
-- Do not silently delete/merge bookings: operators must resolve duplicates before retrying.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM hcm.reservations
    GROUP BY organization_id, channel_connection_id, external_reservation_id
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'duplicate reservation identities exist; reconcile them before applying 0010_schema_fixes.sql';
  END IF;
END $$;

ALTER TABLE hcm.reservations
  DROP CONSTRAINT uq_reservation_channel_version;
ALTER TABLE hcm.reservations
  ADD CONSTRAINT uq_reservation_channel_identity
  UNIQUE (organization_id, channel_connection_id, external_reservation_id);

-- Text channel_version is retained as opaque partner metadata. It is not sortable.
-- Existing rows receive 0; adapters must supply a monotonic source_sequence for new versions.
ALTER TABLE hcm.reservations
  ADD COLUMN source_sequence bigint NOT NULL DEFAULT 0,
  ADD CONSTRAINT ck_reservations_source_sequence_nonnegative CHECK (source_sequence >= 0);

-- Monthly range partitions remain the hot path; DEFAULT prevents out-of-horizon writes
-- from failing immediately. Maintenance must evacuate matching rows from DEFAULT before
-- adding a specific month partition for any range that has already received fallback data.
CREATE TABLE hcm.inventory_days_default PARTITION OF hcm.inventory_days DEFAULT;
CREATE TABLE hcm.sync_logs_default PARTITION OF hcm.sync_logs DEFAULT;

ALTER TABLE hcm.inventory_days_default ENABLE ROW LEVEL SECURITY;
ALTER TABLE hcm.inventory_days_default FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON hcm.inventory_days_default
  USING (organization_id = hcm.current_organization_id())
  WITH CHECK (organization_id = hcm.current_organization_id());
ALTER TABLE hcm.sync_logs_default ENABLE ROW LEVEL SECURITY;
ALTER TABLE hcm.sync_logs_default FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON hcm.sync_logs_default
  USING (organization_id = hcm.current_organization_id())
  WITH CHECK (organization_id = hcm.current_organization_id());

-- roles-and-grants.sql creates hcm_worker BYPASSRLS. Set it here as well when the role
-- already exists so upgraded databases receive the same explicit worker capability.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'hcm_worker') THEN
    EXECUTE 'ALTER ROLE hcm_worker BYPASSRLS';
  END IF;
END $$;
