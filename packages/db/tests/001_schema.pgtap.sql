-- Run via `pnpm db:test` (Docker Compose PostgreSQL 16 + pgTAP image).
-- The test database connection must be a superuser/migration principal so it can SET ROLE.
BEGIN;
CREATE EXTENSION IF NOT EXISTS pgtap;
SELECT plan(10);

INSERT INTO hcm.organizations (organization_id,name,slug)
VALUES ('10000000-0000-0000-0000-000000000001','Test A','test-a'),
       ('20000000-0000-0000-0000-000000000002','Test B','test-b');
INSERT INTO hcm.properties (organization_id,property_id,name,code,timezone,currency)
VALUES ('10000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000011','Property A','A','Asia/Kolkata','INR'),
       ('20000000-0000-0000-0000-000000000002','20000000-0000-0000-0000-000000000022','Property B','B','Asia/Kolkata','INR');
INSERT INTO hcm.room_types (organization_id,property_id,room_type_id,code,name,physical_units)
VALUES ('10000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000011','10000000-0000-0000-0000-000000000111','STD','Standard',1);
INSERT INTO hcm.channels (organization_id,channel_id,kind,name)
VALUES ('10000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000012','booking_com','Test OTA');
INSERT INTO hcm.channel_connections
  (organization_id,property_id,channel_connection_id,channel_id,credential_secret_ref)
VALUES ('10000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000011',
  '10000000-0000-0000-0000-000000000013','10000000-0000-0000-0000-000000000012','test://secret-reference-only');

SELECT throws_ok(
  $$INSERT INTO hcm.inventory_days
    (organization_id,property_id,room_type_id,stay_date,total_physical_units,booked_units)
    VALUES ('10000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000011',
      '10000000-0000-0000-0000-000000000111',date_trunc('month',current_date)::date,1,2)$$,
  '23514', NULL, 'booked units greater than total are rejected');

SET LOCAL ROLE hcm_app;
SET LOCAL app.organization_id = '20000000-0000-0000-0000-000000000002';
SELECT is((SELECT count(*) FROM hcm.properties), 1::bigint,
          'RLS only exposes the active organization property');
RESET ROLE;

INSERT INTO hcm.processed_events (organization_id,event_id,idempotency_key,event_type,outcome)
VALUES ('10000000-0000-0000-0000-000000000001','event-1',repeat('a',64),'com.hospitality.reservation.created','applied');
SELECT throws_ok(
  $$INSERT INTO hcm.processed_events (organization_id,event_id,idempotency_key,event_type,outcome)
    VALUES ('10000000-0000-0000-0000-000000000001','event-2',repeat('a',64),'com.hospitality.reservation.created','applied')$$,
  '23505', NULL, 'idempotency key is unique within a tenant');

INSERT INTO hcm.inventory_days
  (organization_id,property_id,room_type_id,stay_date,total_physical_units,booked_units)
VALUES ('10000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000011',
  '10000000-0000-0000-0000-000000000111',date_trunc('month',current_date)::date,1,0);
SELECT is((SELECT available_units FROM hcm.inventory_days
  WHERE room_type_id='10000000-0000-0000-0000-000000000111'
    AND stay_date=date_trunc('month',current_date)::date), 1,
  'available_units is generated as total minus booked');

SELECT ok((SELECT relispartition FROM pg_class WHERE oid='hcm.inventory_days_default'::regclass),
          'inventory default partition exists');
SELECT ok((SELECT relispartition FROM pg_class WHERE oid='hcm.sync_logs_default'::regclass),
          'sync log default partition exists');
SELECT is((SELECT rolbypassrls FROM pg_roles WHERE rolname='hcm_worker'), true,
          'cross-tenant worker role has explicit BYPASSRLS');

SELECT ok(
  position(to_char(date_trunc('month',current_date),'YYYYMM') in
    array_to_string(explain(format(
      'SELECT stay_date FROM hcm.inventory_days WHERE stay_date = %L::date',
      date_trunc('month',current_date)::date)), E'\n')) > 0,
  'date predicate prunes to the matching monthly inventory partition');

INSERT INTO hcm.reservations
  (organization_id,property_id,reservation_id,channel_connection_id,external_reservation_id,
   channel_version,source_sequence,status,check_in,check_out,property_timezone)
VALUES ('10000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000011',
  '10000000-0000-0000-0000-000000000014','10000000-0000-0000-0000-000000000013',
  'external-res-1','v1',1,'confirmed',current_date,current_date+1,'Asia/Kolkata');
SELECT throws_ok(
  $$INSERT INTO hcm.reservations
    (organization_id,property_id,reservation_id,channel_connection_id,external_reservation_id,
     channel_version,source_sequence,status,check_in,check_out,property_timezone)
    VALUES ('10000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000011',
      '10000000-0000-0000-0000-000000000015','10000000-0000-0000-0000-000000000013',
      'external-res-1','v2',2,'modified',current_date,current_date+1,'Asia/Kolkata')$$,
  '23505', NULL, 'reservation identity is unique across source versions');
SELECT is((SELECT source_sequence FROM hcm.reservations
  WHERE organization_id='10000000-0000-0000-0000-000000000001'
    AND reservation_id='10000000-0000-0000-0000-000000000014'), 1::bigint,
  'reservation source sequence is stored numerically');

SELECT * FROM finish();
ROLLBACK;
