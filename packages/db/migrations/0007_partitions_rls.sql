SET search_path = hcm, public;
-- Monthly partitions and tenant RLS.
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
