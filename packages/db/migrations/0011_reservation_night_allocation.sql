-- Track whether each reservation night actually consumed physical inventory. Oversold nights do not.
ALTER TABLE hcm.reservation_nights
  ADD COLUMN inventory_counted boolean NOT NULL DEFAULT true;

-- Preserve released state for historical cancellations; active and no-show nights remain allocated.
UPDATE hcm.reservation_nights n SET inventory_counted=false
FROM hcm.reservations r
WHERE r.organization_id=n.organization_id AND r.property_id=n.property_id AND r.reservation_id=n.reservation_id
  AND r.status='cancelled';
