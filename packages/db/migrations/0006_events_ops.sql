SET search_path = hcm, public;
-- Idempotency, outbox, sync logs, conflicts, audit and DLQ.
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

