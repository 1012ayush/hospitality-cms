-- atlas:txmode none
SET search_path = hcm, public;
-- Concurrent builds for populated non-partitioned tables; must run outside a transaction.
CREATE INDEX ix_rate_days_org_property_date ON rate_days (organization_id,property_id,stay_date,rate_plan_id); -- date-oriented rate publication
CREATE INDEX ix_mapping_connection_active ON channel_mappings (organization_id,channel_connection_id,active); -- outbound mapping resolution
CREATE INDEX ix_reservations_property_stay ON reservations (organization_id,property_id,check_in,check_out); -- property stay/reconciliation queries
CREATE INDEX ix_reservations_guest ON reservations (organization_id,property_id,primary_guest_id) WHERE primary_guest_id IS NOT NULL; -- guest history lookup
CREATE INDEX ix_reservation_nights_date_room ON reservation_nights (organization_id,property_id,room_type_id,stay_date); -- rebuild booked occupancy and reconciliation
CREATE INDEX ix_processed_events_connection ON processed_events (organization_id,channel_connection_id,processed_at DESC); -- event ledger inspection per connection
CREATE INDEX ix_outbox_pending ON outbox_events (created_at,organization_id,id) WHERE status = 'pending'; -- ordered SKIP LOCKED publisher claim
CREATE INDEX ix_conflicts_open ON oversell_conflicts (organization_id,property_id,detected_at) WHERE status IN ('open','mitigating'); -- active conflict queue
CREATE INDEX ix_audit_entity_time ON audit_log (organization_id,entity_type,entity_id,occurred_at DESC); -- audit entity history
CREATE INDEX ix_dlq_unreplayed ON dlq_events (organization_id,created_at) WHERE replayed_at IS NULL; -- operations replay queue
CREATE INDEX ix_allotment_channel_caps_connection ON allotment_channel_caps (organization_id,channel_connection_id); -- cap lookup by channel