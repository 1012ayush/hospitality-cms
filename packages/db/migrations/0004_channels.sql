SET search_path = hcm, public;
-- Channels, connections, mappings and allotments.
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

