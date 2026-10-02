-- Production-oriented PostgreSQL 16 schema for a multi-tenant hotel channel manager.
-- Apply in a dedicated schema/database with a migration role. Runtime connections must
-- set app.organization_id transaction-locally and must not own tables or bypass RLS.
-- No PAN/CVV fields are defined. Payment data is represented only by an external token reference.

BEGIN;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE SCHEMA IF NOT EXISTS hcm;
SET search_path = hcm, public;

CREATE TYPE channel_kind AS ENUM ('booking_com','expedia_eqc','airbnb','pms','rms','other');
CREATE TYPE reservation_status AS ENUM ('new','confirmed','modified','cancelled','no_show');
CREATE TYPE conflict_status AS ENUM ('open','mitigating','resolved','waived');
CREATE TYPE outbox_status AS ENUM ('pending','published','dead_letter');
CREATE TYPE sync_direction AS ENUM ('inbound','outbound','reconciliation');
CREATE TYPE sync_status AS ENUM ('started','succeeded','retrying','failed','acknowledged');
CREATE TYPE guest_role AS ENUM ('primary','additional');
CREATE TYPE user_status AS ENUM ('active','disabled');

-- Tenant context is set with SET LOCAL app.organization_id = '<uuid>' per transaction.
CREATE FUNCTION current_organization_id() RETURNS uuid
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT NULLIF(current_setting('app.organization_id', true), '')::uuid
$$;

CREATE TABLE organizations (
  organization_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  slug citext NOT NULL UNIQUE,
  data_region text NOT NULL DEFAULT 'unspecified',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE properties (
  organization_id uuid NOT NULL REFERENCES organizations(organization_id),
  property_id uuid NOT NULL DEFAULT gen_random_uuid(),
  name text NOT NULL,
  code text NOT NULL,
  timezone text NOT NULL, -- IANA identifier; validate against pg_timezone_names at provisioning.
  currency char(3) NOT NULL,
  country_code char(2),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, property_id),
  UNIQUE (organization_id, code)
);

CREATE TABLE users (
  organization_id uuid NOT NULL REFERENCES organizations(organization_id),
  user_id uuid NOT NULL DEFAULT gen_random_uuid(),
  email citext NOT NULL,
  display_name text NOT NULL,
  status user_status NOT NULL DEFAULT 'active',
  identity_subject text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, user_id),
  UNIQUE (organization_id, email),
  UNIQUE (organization_id, identity_subject)
);

CREATE TABLE roles (
  organization_id uuid NOT NULL REFERENCES organizations(organization_id),
  role_id uuid NOT NULL DEFAULT gen_random_uuid(),
  role_name text NOT NULL,
  permissions jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (organization_id, role_id),
  UNIQUE (organization_id, role_name)
);

CREATE TABLE user_roles (
  organization_id uuid NOT NULL,
  user_id uuid NOT NULL,
  role_id uuid NOT NULL,
  property_id uuid,
  PRIMARY KEY (organization_id, user_id, role_id, property_id),
  FOREIGN KEY (organization_id,user_id) REFERENCES users(organization_id,user_id),
  FOREIGN KEY (organization_id,role_id) REFERENCES roles(organization_id,role_id),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id)
);

CREATE TABLE room_types (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  room_type_id uuid NOT NULL DEFAULT gen_random_uuid(),
  code text NOT NULL,
  name text NOT NULL,
  physical_units integer NOT NULL CHECK (physical_units >= 0),
  active boolean NOT NULL DEFAULT true,
  PRIMARY KEY (organization_id,property_id,room_type_id),
  UNIQUE (organization_id,property_id,code),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id)
);

CREATE TABLE rate_plans (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  rate_plan_id uuid NOT NULL DEFAULT gen_random_uuid(),
  code text NOT NULL,
  name text NOT NULL,
  currency char(3) NOT NULL,
  meal_plan text,
  derived_from_rate_plan_id uuid,
  derivation_kind text CHECK (derivation_kind IN ('fixed_delta','percentage','none')),
  derivation_value numeric(12,4),
  refundable boolean,
  active boolean NOT NULL DEFAULT true,
  PRIMARY KEY (organization_id,property_id,rate_plan_id),
  UNIQUE (organization_id,property_id,code),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id),
  FOREIGN KEY (organization_id,property_id,derived_from_rate_plan_id)
    REFERENCES rate_plans(organization_id,property_id,rate_plan_id),
  CHECK ((derived_from_rate_plan_id IS NULL) = (derivation_kind IS NULL)),
  CHECK (derivation_value IS NULL OR derived_from_rate_plan_id IS NOT NULL)
);

CREATE TABLE occupancy_pricing (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  rate_plan_id uuid NOT NULL,
  occupancy integer NOT NULL CHECK (occupancy > 0),
  adjustment_kind text NOT NULL CHECK (adjustment_kind IN ('absolute','delta','percentage')),
  adjustment numeric(12,4) NOT NULL,
  PRIMARY KEY (organization_id,property_id,rate_plan_id,occupancy),
  FOREIGN KEY (organization_id,property_id,rate_plan_id)
    REFERENCES rate_plans(organization_id,property_id,rate_plan_id)
);

CREATE TABLE los_pricing (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  rate_plan_id uuid NOT NULL,
  min_nights integer NOT NULL CHECK (min_nights > 0),
  max_nights integer CHECK (max_nights IS NULL OR max_nights >= min_nights),
  adjustment_kind text NOT NULL CHECK (adjustment_kind IN ('absolute','delta','percentage')),
  adjustment numeric(12,4) NOT NULL,
  PRIMARY KEY (organization_id,property_id,rate_plan_id,min_nights),
  FOREIGN KEY (organization_id,property_id,rate_plan_id)
    REFERENCES rate_plans(organization_id,property_id,rate_plan_id)
);

CREATE TABLE taxes_fees (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  tax_fee_id uuid NOT NULL DEFAULT gen_random_uuid(),
  code text NOT NULL,
  name text NOT NULL,
  calculation_kind text NOT NULL CHECK (calculation_kind IN ('percentage','fixed_per_night','fixed_per_stay','per_person_per_night')),
  amount numeric(12,4) NOT NULL CHECK (amount >= 0),
  taxable boolean NOT NULL DEFAULT false,
  included_in_rate boolean NOT NULL DEFAULT false,
  valid_from date,
  valid_to date,
  PRIMARY KEY (organization_id,property_id,tax_fee_id),
  UNIQUE (organization_id,property_id,code),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from)
);

-- Monthly range partitioning bounds active index size and makes date-range pruning predictable.
-- Keep inventory for the full reservation/audit horizon; archive only after policy-approved expiry.
CREATE TABLE inventory_days (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  room_type_id uuid NOT NULL,
  stay_date date NOT NULL,
  total_physical_units integer NOT NULL CHECK (total_physical_units >= 0),
  booked_units integer NOT NULL DEFAULT 0,
  available_units integer GENERATED ALWAYS AS (total_physical_units - booked_units) STORED,
  stop_sell boolean NOT NULL DEFAULT false,
  cta boolean NOT NULL DEFAULT false,
  ctd boolean NOT NULL DEFAULT false,
  min_los integer CHECK (min_los IS NULL OR min_los > 0),
  max_los integer CHECK (max_los IS NULL OR max_los > 0),
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (room_type_id,stay_date),
  FOREIGN KEY (organization_id,property_id,room_type_id)
    REFERENCES room_types(organization_id,property_id,room_type_id),
  CHECK (booked_units >= 0 AND booked_units <= total_physical_units),
  CHECK (max_los IS NULL OR min_los IS NULL OR max_los >= min_los)
) PARTITION BY RANGE (stay_date);

CREATE TABLE rate_days (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  rate_plan_id uuid NOT NULL,
  stay_date date NOT NULL,
  amount numeric(12,2) NOT NULL CHECK (amount >= 0),
  currency char(3) NOT NULL,
  stop_sell boolean NOT NULL DEFAULT false,
  cta boolean NOT NULL DEFAULT false,
  ctd boolean NOT NULL DEFAULT false,
  min_los integer CHECK (min_los IS NULL OR min_los > 0),
  max_los integer CHECK (max_los IS NULL OR max_los > 0),
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (rate_plan_id,stay_date),
  FOREIGN KEY (organization_id,property_id,rate_plan_id)
    REFERENCES rate_plans(organization_id,property_id,rate_plan_id),
  CHECK (max_los IS NULL OR min_los IS NULL OR max_los >= min_los)
);

CREATE TABLE channels (
  organization_id uuid NOT NULL,
  channel_id uuid NOT NULL DEFAULT gen_random_uuid(),
  kind channel_kind NOT NULL,
  name text NOT NULL,
  active boolean NOT NULL DEFAULT true,
  PRIMARY KEY (organization_id,channel_id),
  UNIQUE (organization_id,kind,name)
);

CREATE TABLE channel_connections (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  channel_connection_id uuid NOT NULL DEFAULT gen_random_uuid(),
  channel_id uuid NOT NULL,
  external_account_id text,
  credential_secret_ref text NOT NULL, -- Secret ARN/reference only; never credential material.
  secret_version_ref text,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id,channel_connection_id),
  UNIQUE (organization_id,property_id,channel_id,external_account_id),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id),
  FOREIGN KEY (organization_id,channel_id) REFERENCES channels(organization_id,channel_id)
);

CREATE TABLE channel_mappings (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  channel_connection_id uuid NOT NULL,
  room_type_id uuid NOT NULL,
  rate_plan_id uuid NOT NULL,
  external_property_id text NOT NULL,
  external_room_type_id text NOT NULL,
  external_rate_plan_id text NOT NULL,
  active boolean NOT NULL DEFAULT true,
  PRIMARY KEY (organization_id,channel_connection_id,room_type_id,rate_plan_id),
  UNIQUE (organization_id,channel_connection_id,external_room_type_id,external_rate_plan_id),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id),
  FOREIGN KEY (organization_id,channel_connection_id) REFERENCES channel_connections(organization_id,channel_connection_id),
  FOREIGN KEY (organization_id,property_id,room_type_id) REFERENCES room_types(organization_id,property_id,room_type_id),
  FOREIGN KEY (organization_id,property_id,rate_plan_id) REFERENCES rate_plans(organization_id,property_id,rate_plan_id)
);

CREATE TABLE allotments (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  allotment_id uuid NOT NULL DEFAULT gen_random_uuid(),
  room_type_id uuid NOT NULL,
  name text NOT NULL,
  start_date date NOT NULL,
  end_date date NOT NULL,
  total_units integer NOT NULL CHECK (total_units >= 0),
  released_at timestamptz,
  PRIMARY KEY (organization_id,property_id,allotment_id),
  FOREIGN KEY (organization_id,property_id,room_type_id) REFERENCES room_types(organization_id,property_id,room_type_id),
  CHECK (end_date > start_date)
);

CREATE TABLE allotment_channel_caps (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  allotment_id uuid NOT NULL,
  channel_connection_id uuid NOT NULL,
  cap_units integer NOT NULL CHECK (cap_units >= 0),
  PRIMARY KEY (organization_id,allotment_id,channel_connection_id),
  FOREIGN KEY (organization_id,property_id,allotment_id) REFERENCES allotments(organization_id,property_id,allotment_id),
  FOREIGN KEY (organization_id,channel_connection_id) REFERENCES channel_connections(organization_id,channel_connection_id)
);

CREATE TABLE guests (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  guest_id uuid NOT NULL DEFAULT gen_random_uuid(),
  given_name text,
  family_name text,
  email citext,
  phone text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id,property_id,guest_id),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id)
);

CREATE TABLE reservations (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  reservation_id uuid NOT NULL DEFAULT gen_random_uuid(),
  channel_connection_id uuid NOT NULL,
  external_reservation_id text NOT NULL,
  channel_version text NOT NULL,
  source_updated_at timestamptz,
  status reservation_status NOT NULL,
  oversold boolean NOT NULL DEFAULT false,
  primary_guest_id uuid,
  check_in date NOT NULL,
  check_out date NOT NULL,
  property_timezone text NOT NULL,
  currency char(3),
  total_amount numeric(14,2) CHECK (total_amount IS NULL OR total_amount >= 0),
  tokenized_payment_reference text,
  received_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id,property_id,reservation_id),
  CONSTRAINT uq_reservation_channel_version UNIQUE (organization_id,channel_connection_id,external_reservation_id,channel_version),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id),
  FOREIGN KEY (organization_id,channel_connection_id) REFERENCES channel_connections(organization_id,channel_connection_id),
  FOREIGN KEY (organization_id,property_id,primary_guest_id) REFERENCES guests(organization_id,property_id,guest_id),
  CHECK (check_out > check_in)
);

CREATE TABLE reservation_rooms (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  reservation_id uuid NOT NULL,
  room_ordinal smallint NOT NULL CHECK (room_ordinal > 0),
  room_type_id uuid NOT NULL,
  rate_plan_id uuid,
  adults smallint NOT NULL CHECK (adults >= 0),
  children smallint NOT NULL DEFAULT 0 CHECK (children >= 0),
  PRIMARY KEY (organization_id,property_id,reservation_id,room_ordinal),
  FOREIGN KEY (organization_id,property_id,reservation_id) REFERENCES reservations(organization_id,property_id,reservation_id),
  FOREIGN KEY (organization_id,property_id,room_type_id) REFERENCES room_types(organization_id,property_id,room_type_id),
  FOREIGN KEY (organization_id,property_id,rate_plan_id) REFERENCES rate_plans(organization_id,property_id,rate_plan_id)
);

CREATE TABLE reservation_nights (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  reservation_id uuid NOT NULL,
  room_ordinal smallint NOT NULL,
  stay_date date NOT NULL,
  room_type_id uuid NOT NULL,
  rate_plan_id uuid,
  guest_id uuid,
  nightly_amount numeric(12,2) CHECK (nightly_amount IS NULL OR nightly_amount >= 0),
  currency char(3),
  PRIMARY KEY (organization_id,property_id,reservation_id,room_ordinal,stay_date),
  FOREIGN KEY (organization_id,property_id,reservation_id,room_ordinal) REFERENCES reservation_rooms(organization_id,property_id,reservation_id,room_ordinal),
  FOREIGN KEY (organization_id,property_id,room_type_id) REFERENCES room_types(organization_id,property_id,room_type_id),
  FOREIGN KEY (organization_id,property_id,rate_plan_id) REFERENCES rate_plans(organization_id,property_id,rate_plan_id),
  FOREIGN KEY (organization_id,property_id,guest_id) REFERENCES guests(organization_id,property_id,guest_id)
);

CREATE TABLE processed_events (
  organization_id uuid NOT NULL,
  event_id text NOT NULL,
  channel_connection_id uuid,
  idempotency_key char(64) NOT NULL,
  event_type text NOT NULL,
  source_version text,
  outcome text NOT NULL CHECK (outcome IN ('applied','duplicate','stale','rejected')),
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id,event_id),
  CONSTRAINT uq_processed_events_org_idempotency_key UNIQUE (organization_id,idempotency_key),
  FOREIGN KEY (organization_id,channel_connection_id) REFERENCES channel_connections(organization_id,channel_connection_id)
);

CREATE TABLE outbox_events (
  organization_id uuid NOT NULL,
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  aggregate_version bigint,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  status outbox_status NOT NULL DEFAULT 'pending',
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error text,
  PRIMARY KEY (organization_id,id),
  FOREIGN KEY (organization_id) REFERENCES organizations(organization_id),
  CHECK ((status = 'published') = (published_at IS NOT NULL))
);

CREATE TABLE sync_logs (
  organization_id uuid NOT NULL,
  logged_at timestamptz NOT NULL DEFAULT now(),
  sync_log_id uuid NOT NULL DEFAULT gen_random_uuid(),
  property_id uuid NOT NULL,
  channel_connection_id uuid,
  direction sync_direction NOT NULL,
  status sync_status NOT NULL,
  operation text NOT NULL,
  correlation_id text,
  external_reference text,
  http_status integer,
  attempt integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
  duration_ms integer CHECK (duration_ms IS NULL OR duration_ms >= 0),
  sanitized_error_code text,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (logged_at,sync_log_id),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id),
  FOREIGN KEY (organization_id,channel_connection_id) REFERENCES channel_connections(organization_id,channel_connection_id)
) PARTITION BY RANGE (logged_at);

CREATE TABLE oversell_conflicts (
  organization_id uuid NOT NULL,
  property_id uuid NOT NULL,
  conflict_id uuid NOT NULL DEFAULT gen_random_uuid(),
  reservation_id uuid NOT NULL,
  room_type_id uuid NOT NULL,
  stay_date date NOT NULL,
  excess_units integer NOT NULL DEFAULT 1 CHECK (excess_units > 0),
  status conflict_status NOT NULL DEFAULT 'open',
  detected_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  resolution text,
  PRIMARY KEY (organization_id,property_id,conflict_id),
  FOREIGN KEY (organization_id,property_id,reservation_id) REFERENCES reservations(organization_id,property_id,reservation_id),
  FOREIGN KEY (organization_id,property_id,room_type_id) REFERENCES room_types(organization_id,property_id,room_type_id),
  CHECK ((status IN ('resolved','waived')) = (resolved_at IS NOT NULL))
);

CREATE TABLE audit_log (
  organization_id uuid NOT NULL,
  audit_id uuid NOT NULL DEFAULT gen_random_uuid(),
  occurred_at timestamptz NOT NULL DEFAULT now(),
  actor_user_id uuid,
  property_id uuid,
  action text NOT NULL,
  entity_type text NOT NULL,
  entity_id text NOT NULL,
  correlation_id text,
  before_data jsonb,
  after_data jsonb,
  PRIMARY KEY (organization_id,audit_id),
  FOREIGN KEY (organization_id,actor_user_id) REFERENCES users(organization_id,user_id),
  FOREIGN KEY (organization_id,property_id) REFERENCES properties(organization_id,property_id)
);

CREATE TABLE dlq_events (
  organization_id uuid NOT NULL,
  dlq_event_id uuid NOT NULL DEFAULT gen_random_uuid(),
  source_topic text NOT NULL,
  source_partition integer,
  source_offset bigint,
  event_id text,
  event_type text,
  sanitized_payload jsonb NOT NULL,
  error_code text NOT NULL,
  error_detail text,
  created_at timestamptz NOT NULL DEFAULT now(),
  replayed_at timestamptz,
  replayed_by uuid,
  PRIMARY KEY (organization_id,dlq_event_id),
  FOREIGN KEY (organization_id,replayed_by) REFERENCES users(organization_id,user_id)
);

-- Indexes: each supports a named high-frequency lookup/claim path or FK child access.
CREATE INDEX ix_inventory_org_property_date ON inventory_days (organization_id,property_id,stay_date,room_type_id); -- tenant/property date availability scans
CREATE INDEX ix_rate_days_org_property_date ON rate_days (organization_id,property_id,stay_date,rate_plan_id); -- date-oriented rate publication
CREATE INDEX ix_mapping_connection_active ON channel_mappings (organization_id,channel_connection_id,active); -- outbound mapping resolution
CREATE INDEX ix_reservations_property_stay ON reservations (organization_id,property_id,check_in,check_out); -- property stay/reconciliation queries
CREATE INDEX ix_reservations_guest ON reservations (organization_id,property_id,primary_guest_id) WHERE primary_guest_id IS NOT NULL; -- guest history lookup
CREATE INDEX ix_reservation_nights_date_room ON reservation_nights (organization_id,property_id,room_type_id,stay_date); -- rebuild booked occupancy and reconciliation
CREATE INDEX ix_processed_events_connection ON processed_events (organization_id,channel_connection_id,processed_at DESC); -- event ledger inspection per connection
CREATE INDEX ix_outbox_pending ON outbox_events (created_at,organization_id,id) WHERE status = 'pending'; -- ordered SKIP LOCKED publisher claim
CREATE INDEX ix_sync_logs_property_time ON sync_logs (organization_id,property_id,logged_at DESC); -- operational history by property
CREATE INDEX ix_sync_logs_connection_time ON sync_logs (organization_id,channel_connection_id,logged_at DESC); -- channel connection support
CREATE INDEX ix_conflicts_open ON oversell_conflicts (organization_id,property_id,detected_at) WHERE status IN ('open','mitigating'); -- active conflict queue
CREATE INDEX ix_audit_entity_time ON audit_log (organization_id,entity_type,entity_id,occurred_at DESC); -- audit entity history
CREATE INDEX ix_dlq_unreplayed ON dlq_events (organization_id,created_at) WHERE replayed_at IS NULL; -- operations replay queue
CREATE INDEX ix_allotment_channel_caps_connection ON allotment_channel_caps (organization_id,channel_connection_id); -- cap lookup by channel

-- Partition maintenance: call monthly (ahead of the required horizon). Retain inventory indefinitely
-- through reservation/audit horizon; sync log partitions are detached/dropped only after approved retention.
CREATE FUNCTION ensure_month_partition(p_parent regclass, p_month date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  month_start date := date_trunc('month', p_month)::date;
  month_end date := (date_trunc('month', p_month) + interval '1 month')::date;
  child_name text;
  parent_schema text;
BEGIN
  SELECT n.nspname INTO parent_schema FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=p_parent;
  child_name := format('%s_%s', (p_parent::text)::text, to_char(month_start,'YYYYMM'));
  child_name := regexp_replace(child_name, '[^a-zA-Z0-9_]', '_', 'g');
  EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
                 parent_schema, child_name, p_parent, month_start, month_end);
  -- A child relation can be addressed directly, so enforce tenant isolation there too.
  EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', parent_schema, child_name);
  EXECUTE format('ALTER TABLE %I.%I FORCE ROW LEVEL SECURITY', parent_schema, child_name);
  EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I.%I', parent_schema, child_name);
  EXECUTE format('CREATE POLICY tenant_isolation ON %I.%I USING (organization_id = hcm.current_organization_id()) WITH CHECK (organization_id = hcm.current_organization_id())', parent_schema, child_name);
END $$;

CREATE FUNCTION ensure_hcm_month_partitions(p_month date) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM ensure_month_partition('hcm.inventory_days'::regclass, p_month);
  PERFORM ensure_month_partition('hcm.sync_logs'::regclass, p_month);
END $$;

-- Provision an initial rolling 18-month partition window; scheduler should extend it monthly.
DO $$ DECLARE d date; BEGIN
  FOR d IN SELECT generate_series(date_trunc('month', current_date)::date,
                                   (date_trunc('month', current_date) + interval '17 months')::date,
                                   interval '1 month')::date LOOP
    PERFORM ensure_hcm_month_partitions(d);
  END LOOP;
END $$;

-- Enable and force RLS for every tenant-owned table. Application role must not BYPASSRLS.
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY[
    'organizations','properties','users','roles','user_roles','room_types','rate_plans',
    'occupancy_pricing','los_pricing','taxes_fees','inventory_days','rate_days','channels',
    'channel_connections','channel_mappings','allotments','allotment_channel_caps','guests',
    'reservations','reservation_rooms','reservation_nights','processed_events','outbox_events',
    'sync_logs','oversell_conflicts','audit_log','dlq_events'
  ] LOOP
    EXECUTE format('ALTER TABLE hcm.%I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE hcm.%I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format('CREATE POLICY tenant_isolation ON hcm.%I USING (organization_id = hcm.current_organization_id()) WITH CHECK (organization_id = hcm.current_organization_id())', t);
  END LOOP;
END $$;

COMMIT;

/*
MIGRATION ORDER
1. Extensions/schema/helper function and enum types.
2. organizations, properties, users, roles, user_roles.
3. room_types, rate_plans, pricing tables, taxes_fees.
4. channels, channel_connections, channel_mappings, allotments/caps.
5. guests, reservations, reservation_rooms, reservation_nights.
6. Partitioned inventory_days and rate_days; create rolling inventory partitions.
7. processed_events, outbox_events, sync_logs partitions, conflicts, audit, DLQ.
8. Indexes, RLS policies, grants, seed channel catalogs, and tenant provisioning.

Six sample queries and expected plan shape
1. Availability for [check_in, check_out); expect partition pruning and PK/index range access.
EXPLAIN (ANALYZE, BUFFERS)
SELECT stay_date, available_units, stop_sell, cta, ctd, min_los, max_los
FROM hcm.inventory_days
WHERE organization_id=:org AND property_id=:property AND room_type_id=:room_type
  AND stay_date >= :check_in::date AND stay_date < :check_out::date
ORDER BY stay_date;

2. Deterministic row lock; expect partition pruning and PK index scan, with row locks.
BEGIN;
SELECT stay_date, available_units, version
FROM hcm.inventory_days
WHERE organization_id=:org AND property_id=:property AND room_type_id=:room_type
  AND stay_date >= :check_in::date AND stay_date < :check_out::date
ORDER BY stay_date FOR UPDATE;
-- Application verifies every expected night exists and availability before decrementing.
ROLLBACK;

3. Outbox poll; expect partial ix_outbox_pending scan and LockRows; small LIMIT avoids broad locking.
EXPLAIN (ANALYZE, BUFFERS)
SELECT organization_id,id,aggregate_type,aggregate_id,event_type,payload
FROM hcm.outbox_events WHERE status='pending'
ORDER BY created_at,organization_id,id LIMIT 100 FOR UPDATE SKIP LOCKED;

4. Reconciliation diff; expect reservation_nights date/room index and inventory PK access; aggregate only bounded dates.
EXPLAIN (ANALYZE, BUFFERS)
WITH expected AS (
 SELECT organization_id,property_id,room_type_id,stay_date,count(*)::int booked
 FROM hcm.reservation_nights rn JOIN hcm.reservations r USING (organization_id,property_id,reservation_id)
 WHERE organization_id=:org AND property_id=:property AND stay_date >= :from_date::date
   AND stay_date < :to_date::date AND r.status IN ('new','confirmed','modified')
 GROUP BY organization_id,property_id,room_type_id,stay_date
)
SELECT i.room_type_id,i.stay_date,i.booked_units,coalesce(e.booked,0) expected_booked
FROM hcm.inventory_days i LEFT JOIN expected e USING (organization_id,property_id,room_type_id,stay_date)
WHERE i.organization_id=:org AND i.property_id=:property AND i.stay_date >= :from_date::date
  AND i.stay_date < :to_date::date AND i.booked_units <> coalesce(e.booked,0);

5. Sync history by property; expect partition pruning and ix_sync_logs_property_time scan.
EXPLAIN (ANALYZE, BUFFERS)
SELECT logged_at,sync_log_id,direction,status,operation,correlation_id,sanitized_error_code
FROM hcm.sync_logs WHERE organization_id=:org AND property_id=:property
  AND logged_at >= :from_ts::timestamptz AND logged_at < :to_ts::timestamptz
ORDER BY logged_at DESC LIMIT 200;

6. Idempotency check; expect unique index uq_processed_events_org_idempotency_key.
EXPLAIN (ANALYZE, BUFFERS)
SELECT event_id,outcome,processed_at FROM hcm.processed_events
WHERE organization_id=:org AND idempotency_key=:sha256_key;

DATABASE-ENFORCED INVARIANTS
- Tenant ownership is present on tenant tables and enforced by RLS when app.organization_id is set.
- PK/unique/FK constraints enforce row identity, tenant-scoped relationships, reservation source-version uniqueness,
  and event idempotency uniqueness.
- Inventory available_units is generated; booked_units is bounded from zero through total_physical_units.
- Date windows, nonnegative amounts/counts, enumerated states, and basic rate/LOS constraints are checked.
- Partition keys are included in partitioned table primary keys; queries must constrain date for pruning.

APPLICATION-ENFORCED INVARIANTS
- Set transaction-local tenant context; authorize property access/RBAC; validate IANA timezone and currency codes.
- Lock every required room/date in sorted order, ensure complete night coverage, compare versions, then update all-or-nothing.
- Derive booked_units consistently with reservation_nights; transactionally update reservations, inventory, conflicts,
  processed_events and outbox. Handle stale source versions and idempotent cancellation/no-show semantics.
- Cross-row allotment caps, sum of channel caps, group-block release, pricing derivation acyclicity, and
  reservation-room/night/date consistency need transactional application logic or future triggers.
- External OTA oversell cannot be prevented by a local CHECK; accept confirmed booking, flag conflict, close other
  channels, alert, and resolve operationally. Never store raw card/VCC data.
- Outbox publication, retry, Kafka offset coordination, audit immutability/retention, and sync log expiry are operational.

KNOWN GAPS
- Partition maintenance requires a scheduled job and monitoring; inventory history retention is policy-dependent.
- PostgreSQL cannot validate an IANA zone with a simple CHECK; provisioning/application validation is required.
- Reservation idempotency version is text because partner version semantics differ; verify each partner contract.
- Cross-row caps, reservation-night completeness, pricing derivation cycles, and audit immutability require application enforcement or separately reviewed triggers.
- RLS is defense in depth, not a substitute for authorization. Runtime roles, grants, and tenant context plumbing must be configured per deployment.
- Partner-specific payload, endpoint, auth, polling, acknowledgment, and quota details: ASSUMPTION – verify against partner docs.

HANDOFF SUMMARY
- Decision: PostgreSQL 16, schema hcm; RLS uses hcm.current_organization_id() from transaction-local app.organization_id.
- Partitioning: inventory_days and sync_logs monthly RANGE partitions; ensure_month_partition(regclass,date), ensure_hcm_month_partitions(date); inventory retained through reservation/audit horizon, sync logs policy-retained.
- Enums: channel_kind(booking_com,expedia_eqc,airbnb,pms,rms,other); reservation_status(new,confirmed,modified,cancelled,no_show); conflict_status(open,mitigating,resolved,waived); outbox_status(pending,published,dead_letter); sync_direction(inbound,outbound,reconciliation); sync_status(started,succeeded,retrying,failed,acknowledged); guest_role(primary,additional); user_status(active,disabled).
- Tables: organizations, properties, users, roles, user_roles, room_types, rate_plans, occupancy_pricing, los_pricing, taxes_fees, inventory_days, rate_days, channels, channel_connections, channel_mappings, allotments, allotment_channel_caps, guests, reservations, reservation_rooms, reservation_nights, processed_events, outbox_events, sync_logs, oversell_conflicts, audit_log, dlq_events.
- Key columns: reservations(channel_connection_id,external_reservation_id,channel_version,status,oversold,tokenized_payment_reference); inventory_days(room_type_id,stay_date,total_physical_units,booked_units,available_units,version); outbox_events(id,aggregate_type,aggregate_id,payload,published_at); sync_logs(logged_at,property_id,status).
- Important indexes: uq_reservation_channel_version; ix_inventory_org_property_date; ix_rate_days_org_property_date; ix_mapping_connection_active; ix_reservations_property_stay; ix_reservations_guest; ix_reservation_nights_date_room; uq_processed_events_org_idempotency_key; ix_processed_events_connection; ix_outbox_pending; ix_sync_logs_property_time; ix_sync_logs_connection_time; ix_conflicts_open; ix_audit_entity_time; ix_dlq_unreplayed; ix_allotment_channel_caps_connection.
- Conflict policy: accept externally confirmed booking; persist oversold conflict and mitigate through outbound close/ops workflow; local inventory CHECK protects internal decrements only.
- File: outputs/hotel-channel-manager-schema.sql.
*/
