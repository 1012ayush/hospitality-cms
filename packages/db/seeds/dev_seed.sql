-- Local-only idempotent seed. The secret value is a reference placeholder, never credential material.
BEGIN;
SET LOCAL search_path = hcm, public;
SET LOCAL app.organization_id = '30000000-0000-0000-0000-000000000001';

INSERT INTO organizations (organization_id,name,slug,data_region)
VALUES ('30000000-0000-0000-0000-000000000001','Development Hotel Group','dev-hotel-group','local')
ON CONFLICT (organization_id) DO NOTHING;

INSERT INTO properties (organization_id,property_id,name,code,timezone,currency,country_code)
VALUES ('30000000-0000-0000-0000-000000000001','30000000-0000-0000-0000-000000000002',
        'Development Hotel','DEV001','Asia/Kolkata','INR','IN')
ON CONFLICT (organization_id,property_id) DO NOTHING;

INSERT INTO room_types (organization_id,property_id,room_type_id,code,name,physical_units)
VALUES
  ('30000000-0000-0000-0000-000000000001','30000000-0000-0000-0000-000000000002','30000000-0000-0000-0000-000000000003','STD','Standard',20),
  ('30000000-0000-0000-0000-000000000001','30000000-0000-0000-0000-000000000002','30000000-0000-0000-0000-000000000004','DLX','Deluxe',8)
ON CONFLICT (organization_id,property_id,room_type_id) DO NOTHING;

INSERT INTO channels (organization_id,channel_id,kind,name)
VALUES ('30000000-0000-0000-0000-000000000001','30000000-0000-0000-0000-000000000005','booking_com','Development OTA')
ON CONFLICT (organization_id,channel_id) DO NOTHING;

INSERT INTO channel_connections
  (organization_id,property_id,channel_connection_id,channel_id,external_account_id,credential_secret_ref,enabled)
VALUES ('30000000-0000-0000-0000-000000000001','30000000-0000-0000-0000-000000000002',
        '30000000-0000-0000-0000-000000000006','30000000-0000-0000-0000-000000000005',
        'dev-account','local://secret-reference-only',false)
ON CONFLICT (organization_id,channel_connection_id) DO NOTHING;

INSERT INTO inventory_days
  (organization_id,property_id,room_type_id,stay_date,total_physical_units,booked_units)
SELECT '30000000-0000-0000-0000-000000000001',
       '30000000-0000-0000-0000-000000000002',
       rt.room_type_id,
       d::date,
       rt.physical_units,
       0
FROM room_types AS rt
CROSS JOIN generate_series(current_date, current_date + 59, interval '1 day') AS d
WHERE rt.organization_id = '30000000-0000-0000-0000-000000000001'
  AND rt.property_id = '30000000-0000-0000-0000-000000000002'
ON CONFLICT (room_type_id,stay_date) DO NOTHING;

COMMIT;
