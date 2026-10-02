import type { ColumnType, Generated } from "kysely";

export type UUID = string;
export type PgDate = string;
export type Timestamp = ColumnType<Date, Date | string, Date | string>;
export type BigIntText = ColumnType<string, number | string, number | string>;
export type JsonObject = ColumnType<
  Record<string, unknown>,
  Record<string, unknown>,
  Record<string, unknown>
>;

export type ChannelKind =
  "booking_com" | "expedia_eqc" | "airbnb" | "pms" | "rms" | "other";
export type ReservationStatus =
  "new" | "confirmed" | "modified" | "cancelled" | "no_show";
export type ConflictStatus = "open" | "mitigating" | "resolved" | "waived";
export type OutboxStatus = "pending" | "published" | "dead_letter";
export type SyncDirection = "inbound" | "outbound" | "reconciliation";
export type SyncStatus =
  "started" | "succeeded" | "retrying" | "failed" | "acknowledged";
export type UserStatus = "active" | "disabled";

export interface OrganizationsTable {
  organization_id: Generated<UUID>;
  name: string;
  slug: string;
  data_region: Generated<string>;
  created_at: Generated<Timestamp>;
  updated_at: Generated<Timestamp>;
}

export interface PropertiesTable {
  organization_id: UUID;
  property_id: Generated<UUID>;
  name: string;
  code: string;
  timezone: string;
  currency: string;
  country_code: string | null;
  active: Generated<boolean>;
  created_at: Generated<Timestamp>;
}

export interface UsersTable {
  organization_id: UUID;
  user_id: Generated<UUID>;
  email: string;
  display_name: string;
  status: Generated<UserStatus>;
  identity_subject: string;
  created_at: Generated<Timestamp>;
}

export interface RolesTable {
  organization_id: UUID;
  role_id: Generated<UUID>;
  role_name: string;
  permissions: Generated<JsonObject>;
}

export interface UserRolesTable {
  organization_id: UUID;
  user_id: UUID;
  role_id: UUID;
  property_id: UUID;
}

export interface RoomTypesTable {
  organization_id: UUID;
  property_id: UUID;
  room_type_id: Generated<UUID>;
  code: string;
  name: string;
  physical_units: number;
  active: Generated<boolean>;
}

export interface RatePlansTable {
  organization_id: UUID;
  property_id: UUID;
  rate_plan_id: Generated<UUID>;
  code: string;
  name: string;
  currency: string;
  meal_plan: string | null;
  derived_from_rate_plan_id: UUID | null;
  derivation_kind: string | null;
  derivation_value: number | null;
  refundable: boolean | null;
  active: Generated<boolean>;
}

export interface OccupancyPricingTable {
  organization_id: UUID;
  property_id: UUID;
  rate_plan_id: UUID;
  occupancy: number;
  adjustment_kind: string;
  adjustment: number;
}

export interface LosPricingTable {
  organization_id: UUID;
  property_id: UUID;
  rate_plan_id: UUID;
  min_nights: number;
  max_nights: number | null;
  adjustment_kind: string;
  adjustment: number;
}

export interface TaxesFeesTable {
  organization_id: UUID;
  property_id: UUID;
  tax_fee_id: Generated<UUID>;
  code: string;
  name: string;
  calculation_kind: string;
  amount: number;
  taxable: Generated<boolean>;
  included_in_rate: Generated<boolean>;
  valid_from: PgDate | null;
  valid_to: PgDate | null;
}

export interface InventoryDaysTable {
  organization_id: UUID;
  property_id: UUID;
  room_type_id: UUID;
  stay_date: PgDate;
  total_physical_units: number;
  booked_units: Generated<number>;
  available_units: Generated<number>;
  stop_sell: Generated<boolean>;
  cta: Generated<boolean>;
  ctd: Generated<boolean>;
  min_los: number | null;
  max_los: number | null;
  version: Generated<BigIntText>;
  updated_at: Generated<Timestamp>;
}

export interface RateDaysTable {
  organization_id: UUID;
  property_id: UUID;
  rate_plan_id: UUID;
  stay_date: PgDate;
  amount: number;
  currency: string;
  stop_sell: Generated<boolean>;
  cta: Generated<boolean>;
  ctd: Generated<boolean>;
  min_los: number | null;
  max_los: number | null;
  version: Generated<BigIntText>;
  updated_at: Generated<Timestamp>;
}

export interface ChannelsTable {
  organization_id: UUID;
  channel_id: Generated<UUID>;
  kind: ChannelKind;
  name: string;
  active: Generated<boolean>;
}

export interface ChannelConnectionsTable {
  organization_id: UUID;
  property_id: UUID;
  channel_connection_id: Generated<UUID>;
  channel_id: UUID;
  external_account_id: string | null;
  credential_secret_ref: string;
  secret_version_ref: string | null;
  enabled: Generated<boolean>;
  created_at: Generated<Timestamp>;
}

export interface ChannelMappingsTable {
  organization_id: UUID;
  property_id: UUID;
  channel_connection_id: UUID;
  room_type_id: UUID;
  rate_plan_id: UUID;
  external_property_id: string;
  external_room_type_id: string;
  external_rate_plan_id: string;
  active: Generated<boolean>;
}

export interface AllotmentsTable {
  organization_id: UUID;
  property_id: UUID;
  allotment_id: Generated<UUID>;
  room_type_id: UUID;
  name: string;
  start_date: PgDate;
  end_date: PgDate;
  total_units: number;
  released_at: Timestamp | null;
}

export interface AllotmentChannelCapsTable {
  organization_id: UUID;
  property_id: UUID;
  allotment_id: UUID;
  channel_connection_id: UUID;
  cap_units: number;
}

export interface GuestsTable {
  organization_id: UUID;
  property_id: UUID;
  guest_id: Generated<UUID>;
  given_name: string | null;
  family_name: string | null;
  email: string | null;
  phone: string | null;
  created_at: Generated<Timestamp>;
}

export interface ReservationsTable {
  organization_id: UUID;
  property_id: UUID;
  reservation_id: Generated<UUID>;
  channel_connection_id: UUID;
  external_reservation_id: string;
  channel_version: string;
  source_sequence: Generated<BigIntText>;
  source_updated_at: Timestamp | null;
  status: ReservationStatus;
  oversold: Generated<boolean>;
  primary_guest_id: UUID | null;
  check_in: PgDate;
  check_out: PgDate;
  property_timezone: string;
  currency: string | null;
  total_amount: number | null;
  tokenized_payment_reference: string | null;
  received_at: Generated<Timestamp>;
  updated_at: Generated<Timestamp>;
}

export interface ReservationRoomsTable {
  organization_id: UUID;
  property_id: UUID;
  reservation_id: UUID;
  room_ordinal: number;
  room_type_id: UUID;
  rate_plan_id: UUID | null;
  adults: number;
  children: Generated<number>;
}

export interface ReservationNightsTable {
  organization_id: UUID;
  property_id: UUID;
  reservation_id: UUID;
  room_ordinal: number;
  stay_date: PgDate;
  room_type_id: UUID;
  rate_plan_id: UUID | null;
  guest_id: UUID | null;
  nightly_amount: number | null;
  currency: string | null;
  inventory_counted: Generated<boolean>;
}

export interface ProcessedEventsTable {
  organization_id: UUID;
  event_id: string;
  channel_connection_id: UUID | null;
  idempotency_key: string;
  event_type: string;
  source_version: string | null;
  outcome: "applied" | "duplicate" | "stale" | "rejected";
  processed_at: Generated<Timestamp>;
}

export interface OutboxEventsTable {
  organization_id: UUID;
  id: Generated<UUID>;
  aggregate_type: string;
  aggregate_id: string;
  aggregate_version: BigIntText | null;
  event_type: string;
  payload: JsonObject;
  status: Generated<OutboxStatus>;
  created_at: Generated<Timestamp>;
  published_at: Timestamp | null;
  attempts: Generated<number>;
  last_error: string | null;
  claimed_by: string | null;
  claimed_until: Timestamp | null;
  partition_key: string | null;
}

export interface SyncLogsTable {
  organization_id: UUID;
  logged_at: Generated<Timestamp>;
  sync_log_id: Generated<UUID>;
  property_id: UUID;
  channel_connection_id: UUID | null;
  direction: SyncDirection;
  status: SyncStatus;
  operation: string;
  correlation_id: string | null;
  external_reference: string | null;
  http_status: number | null;
  attempt: Generated<number>;
  duration_ms: number | null;
  sanitized_error_code: string | null;
  details: Generated<JsonObject>;
}

export interface OversellConflictsTable {
  organization_id: UUID;
  property_id: UUID;
  conflict_id: Generated<UUID>;
  reservation_id: UUID;
  room_type_id: UUID;
  stay_date: PgDate;
  excess_units: Generated<number>;
  status: Generated<ConflictStatus>;
  detected_at: Generated<Timestamp>;
  resolved_at: Timestamp | null;
  resolution: string | null;
}

export interface AuditLogTable {
  organization_id: UUID;
  audit_id: Generated<UUID>;
  occurred_at: Generated<Timestamp>;
  actor_user_id: UUID | null;
  property_id: UUID | null;
  action: string;
  entity_type: string;
  entity_id: string;
  correlation_id: string | null;
  before_data: JsonObject | null;
  after_data: JsonObject | null;
}

export interface DlqEventsTable {
  organization_id: UUID;
  dlq_event_id: Generated<UUID>;
  source_topic: string;
  source_partition: number | null;
  source_offset: BigIntText | null;
  event_id: string | null;
  event_type: string | null;
  sanitized_payload: JsonObject;
  error_code: string;
  error_detail: string | null;
  created_at: Generated<Timestamp>;
  replayed_at: Timestamp | null;
  replayed_by: UUID | null;
}

export interface HcmDatabase {
  organizations: OrganizationsTable;
  properties: PropertiesTable;
  users: UsersTable;
  roles: RolesTable;
  user_roles: UserRolesTable;
  room_types: RoomTypesTable;
  rate_plans: RatePlansTable;
  occupancy_pricing: OccupancyPricingTable;
  los_pricing: LosPricingTable;
  taxes_fees: TaxesFeesTable;
  inventory_days: InventoryDaysTable;
  rate_days: RateDaysTable;
  channels: ChannelsTable;
  channel_connections: ChannelConnectionsTable;
  channel_mappings: ChannelMappingsTable;
  allotments: AllotmentsTable;
  allotment_channel_caps: AllotmentChannelCapsTable;
  guests: GuestsTable;
  reservations: ReservationsTable;
  reservation_rooms: ReservationRoomsTable;
  reservation_nights: ReservationNightsTable;
  processed_events: ProcessedEventsTable;
  outbox_events: OutboxEventsTable;
  sync_logs: SyncLogsTable;
  oversell_conflicts: OversellConflictsTable;
  audit_log: AuditLogTable;
  dlq_events: DlqEventsTable;
}
