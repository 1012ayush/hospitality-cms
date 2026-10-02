SET search_path = hcm, public;
-- Tenant properties, identity/RBAC, room/rate catalog and pricing.
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
  property_id uuid NOT NULL,
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

