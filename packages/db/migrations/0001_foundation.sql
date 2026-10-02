-- Foundation: extensions, schema, tenant helper, enums, organizations.
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

