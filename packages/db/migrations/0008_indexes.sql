SET search_path = hcm, public;
-- Indexes on partitioned parents (built transactionally).
CREATE INDEX ix_inventory_org_property_date ON inventory_days (organization_id,property_id,stay_date,room_type_id); -- tenant/property date availability scans
CREATE INDEX ix_sync_logs_property_time ON sync_logs (organization_id,property_id,logged_at DESC); -- operational history by property
CREATE INDEX ix_sync_logs_connection_time ON sync_logs (organization_id,channel_connection_id,logged_at DESC); -- channel connection support
