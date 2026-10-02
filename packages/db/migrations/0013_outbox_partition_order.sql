-- Stable property partition key lets multiple relay workers claim different properties
-- while guaranteeing earlier outbox rows for one property publish before later rows.
ALTER TABLE hcm.outbox_events ADD COLUMN partition_key text;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM hcm.outbox_events WHERE status <> 'published'
    AND COALESCE(payload #>> '{data,propertyId}',payload #>> '{data,property_id}') IS NULL) THEN
    RAISE EXCEPTION 'pending outbox row has no property ID; assign partition_key before applying 0013_outbox_partition_order.sql';
  END IF;
END $$;
UPDATE hcm.outbox_events
SET partition_key = COALESCE(payload #>> '{data,propertyId}', payload #>> '{data,property_id}', organization_id::text)
WHERE partition_key IS NULL;
CREATE INDEX ix_outbox_pending_partition_order
  ON hcm.outbox_events (partition_key,created_at,id)
  WHERE status <> 'published';
