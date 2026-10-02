SET search_path = hcm, public;
-- Inventory and rate-day storage.
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

