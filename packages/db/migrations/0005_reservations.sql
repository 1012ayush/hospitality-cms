SET search_path = hcm, public;
-- Guest, reservation and nightly stay records.
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

