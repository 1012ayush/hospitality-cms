-- Short outbox leases keep database locks out of Kafka network waits.
ALTER TABLE hcm.outbox_events
  ADD COLUMN claimed_by text,
  ADD COLUMN claimed_until timestamptz;
CREATE INDEX ix_outbox_expired_leases
  ON hcm.outbox_events (claimed_until,created_at)
  WHERE status='pending' AND claimed_until IS NOT NULL;

ALTER TABLE hcm.outbox_events
  ADD CONSTRAINT ck_outbox_claim_pair CHECK ((claimed_by IS NULL) = (claimed_until IS NULL));
