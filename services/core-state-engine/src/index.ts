import { createHash } from "node:crypto";
import { sql, type Kysely, type Transaction } from "kysely";
import type { Redis } from "ioredis";
import { withTenant, type HcmDatabase, type UUID } from "@hcm/db";
import type { CloudEvent, Reservation } from "@hcm/hospitality-contracts";
import {
  createLogger,
  loadConfig,
  createKafkaClient,
  createKafkaProducer,
  createRedisClient,
  withSpan,
} from "@hcm/shared";
import { Pool } from "pg";
import { Kysely as KyselyConstructor, PostgresDialect } from "kysely";
import type { Consumer, EachMessagePayload, Producer } from "kafkajs";
import { createKafkaConsumer } from "@hcm/shared";
import {
  CloudEventSchema,
  ReservationSchema,
} from "@hcm/hospitality-contracts";

type ReservationEventType =
  | "com.hospitality.reservation.created"
  | "com.hospitality.reservation.modified"
  | "com.hospitality.reservation.cancelled"
  | "com.hospitality.reservation.no_show";
type ApplyOutcome = {
  committed: true;
  outcome: "applied" | "duplicate" | "stale";
  reservationId?: UUID;
  oversold?: boolean;
};
type InventoryKey = { roomTypeId: UUID; stayDate: string };
type InventoryRow = InventoryKey & {
  total_physical_units: number;
  booked_units: number;
  available_units: number;
};
type NightRow = InventoryKey & {
  room_ordinal: number;
  rate_plan_id: UUID | null;
  inventory_counted: boolean;
};
type RoomInput = {
  roomTypeId: UUID;
  ratePlanId?: UUID;
  adults: number;
  children?: number;
};

export class ReservationApplyError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
    this.name = "ReservationApplyError";
  }
}

/** Redis can accelerate duplicate detection but is deliberately not on the correctness path. */
export interface IdempotencyCache {
  get(key: string): Promise<string | null>;
  set(
    key: string,
    value: string,
    expiry: "EX",
    seconds: number,
    condition: "NX",
  ): Promise<string | null>;
}

/** Kafka property-key ordering reduces contention. PostgreSQL locks and constraints are the authority.
 * Redis distributed locking is intentionally omitted: it cannot improve correctness beyond the DB lock,
 * and a lease expiry during a paused process adds fencing complexity. */
export async function applyReservationEvent(
  db: Kysely<HcmDatabase>,
  cache: IdempotencyCache | undefined,
  event: CloudEvent<Reservation, ReservationEventType>,
  options: {
    lockTimeoutMs?: number;
    statementTimeoutMs?: number;
    retries?: number;
    logger?: ReturnType<typeof createLogger>;
  } = {},
): Promise<ApplyOutcome> {
  const reservation = event.data;
  const idemDigest = createHash("sha256")
    .update(
      `${reservation.channelConnectionId}:${reservation.reservationId}:${reservation.channelVersion}`,
    )
    .digest("hex");
  const cacheKey = `hcm:reservation:idem:${reservation.tenantId}:${idemDigest}`;
  try {
    if (await cache?.get(cacheKey))
      return { committed: true, outcome: "duplicate" };
  } catch (error) {
    options.logger?.warn(
      { err: error },
      "Redis idempotency cache unavailable; continuing to authoritative Postgres ledger",
    );
  }

  const maxAttempts = (options.retries ?? 3) + 1;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const result = await withTenant(db, reservation.tenantId, async (trx) => {
        await sql`SELECT set_config('lock_timeout', ${`${options.lockTimeoutMs ?? 5_000}ms`}, true)`.execute(
          trx,
        );
        await sql`SELECT set_config('statement_timeout', ${`${options.statementTimeoutMs ?? 8_000}ms`}, true)`.execute(
          trx,
        );
        return mutateInTransaction(trx, event, idemDigest);
      });
      // Write-through only after transaction commit; cache failure cannot undo committed truth.
      try {
        await cache?.set(cacheKey, "1", "EX", 86_400, "NX");
      } catch (error) {
        options.logger?.warn(
          { err: error },
          "Redis idempotency cache write failed after commit",
        );
      }
      return result;
    } catch (error) {
      const code = getSqlState(error);
      const retryable =
        code === "40001" ||
        code === "40P01" ||
        code === "55P03" ||
        code === "57014";
      if (!retryable || attempt === maxAttempts) {
        if (retryable)
          throw new ReservationApplyError(
            code ?? "DB_RETRY_EXHAUSTED",
            "Transient database contention exceeded retry budget",
            true,
          );
        throw error;
      }
      // Bounded exponential retry with jitter; a new transaction and fresh row locks on each attempt.
      await new Promise((resolve) =>
        setTimeout(
          resolve,
          Math.min(25 * 2 ** (attempt - 1) + Math.random() * 25, 250),
        ),
      );
    }
  }
  throw new ReservationApplyError(
    "UNREACHABLE",
    "Retry loop exited unexpectedly",
    false,
  );
}

async function mutateInTransaction(
  trx: Transaction<HcmDatabase>,
  event: CloudEvent<Reservation, ReservationEventType>,
  idemDigest: string,
): Promise<ApplyOutcome> {
  const reservation = event.data;
  const orgId = reservation.tenantId;
  const eventType = event.type;
  const eventInserted = await sql<{ event_id: string }>`
    INSERT INTO hcm.processed_events
      (organization_id,event_id,channel_connection_id,idempotency_key,event_type,source_version,outcome)
    VALUES (${orgId}::uuid,${event.id},${reservation.channelConnectionId}::uuid,${idemDigest},${eventType},${reservation.channelVersion},'applied')
    ON CONFLICT DO NOTHING RETURNING event_id
  `.execute(trx);
  if (eventInserted.rows.length === 0)
    return { committed: true, outcome: "duplicate" };

  const existingResult = await sql<{
    reservation_id: string;
    source_sequence: string;
    status: string;
    primary_guest_id: string | null;
  }>`
    SELECT reservation_id,source_sequence::text,status,primary_guest_id
    FROM hcm.reservations
    WHERE organization_id=${orgId}::uuid AND channel_connection_id=${reservation.channelConnectionId}::uuid
      AND external_reservation_id=${reservation.reservationId}
    FOR UPDATE
  `.execute(trx);
  const existing = existingResult.rows[0];
  if (
    existing &&
    BigInt(reservation.sourceSequence) <= BigInt(existing.source_sequence)
  ) {
    await sql`UPDATE hcm.processed_events SET outcome='stale' WHERE organization_id=${orgId}::uuid AND event_id=${event.id}`.execute(
      trx,
    );
    return {
      committed: true,
      outcome: "stale",
      reservationId: existing.reservation_id,
    };
  }

  const roomInputs: RoomInput[] =
    reservation.rooms?.map((room) => ({
      roomTypeId: room.roomTypeId,
      ...(room.ratePlanId ? { ratePlanId: room.ratePlanId } : {}),
      adults: room.adults,
      children: room.children,
    })) ??
    (reservation.roomTypeId && reservation.occupancy
      ? [
          {
            roomTypeId: reservation.roomTypeId,
            ...(reservation.ratePlanId
              ? { ratePlanId: reservation.ratePlanId }
              : {}),
            adults: reservation.occupancy.adults,
            children: reservation.occupancy.children,
          },
        ]
      : []);
  if (roomInputs.length === 0)
    throw new ReservationApplyError(
      "ROOMS_REQUIRED",
      "Reservation must contain at least one room line",
      false,
    );
  const wasCancelled =
    eventType === "com.hospitality.reservation.cancelled" ||
    reservation.lifecycle === "cancelled";
  const isNoShow =
    eventType === "com.hospitality.reservation.no_show" ||
    reservation.lifecycle === "no_show";
  const oldNights = existing
    ? (
        await sql<NightRow>`SELECT room_type_id AS "roomTypeId",stay_date::text AS "stayDate",room_ordinal,rate_plan_id,inventory_counted
        FROM hcm.reservation_nights WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND reservation_id=${existing.reservation_id}::uuid
        ORDER BY room_type_id,stay_date,room_ordinal`.execute(trx)
      ).rows
    : [];

  // Lock the union of old and new nights in the globally consistent room_type_id,date order.
  const proposedNights =
    wasCancelled || isNoShow
      ? []
      : roomInputs.flatMap((room) =>
          enumerateNights(
            reservation.stay.checkIn,
            reservation.stay.checkOut,
          ).map((stayDate) => ({ roomTypeId: room.roomTypeId, stayDate })),
        );
  const lockKeys = dedupeKeys([
    ...oldNights.map((night) => ({
      roomTypeId: night.roomTypeId,
      stayDate: night.stayDate,
    })),
    ...proposedNights,
  ]);
  const before = await lockInventoryRows(
    trx,
    orgId,
    reservation.propertyId,
    lockKeys,
  );

  let primaryGuestId: string | null = existing?.primary_guest_id ?? null;
  if (reservation.guest) {
    if (primaryGuestId) {
      await sql`UPDATE hcm.guests SET given_name=${reservation.guest.givenName ?? null},family_name=${reservation.guest.familyName ?? null},
        email=${reservation.guest.email ?? null},phone=${reservation.guest.phone ?? null}
        WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND guest_id=${primaryGuestId}::uuid`.execute(
        trx,
      );
    } else {
      const guest = await sql<{
        guest_id: string;
      }>`INSERT INTO hcm.guests (organization_id,property_id,given_name,family_name,email,phone)
        VALUES (${orgId}::uuid,${reservation.propertyId}::uuid,${reservation.guest.givenName ?? null},${reservation.guest.familyName ?? null},
          ${reservation.guest.email ?? null},${reservation.guest.phone ?? null}) RETURNING guest_id`.execute(
        trx,
      );
      primaryGuestId = guest.rows[0]!.guest_id;
    }
  }
  let reservationId = existing?.reservation_id;
  if (existing) {
    // Release only units this reservation actually consumed. Oversold nights never stole another guest's unit.
    if (!isNoShow)
      await releaseOldAllocation(
        trx,
        orgId,
        reservation.propertyId,
        reservationId!,
        oldNights,
      );
    if (!wasCancelled && !isNoShow) {
      await sql`DELETE FROM hcm.reservation_nights WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND reservation_id=${reservationId}::uuid`.execute(
        trx,
      );
      await sql`DELETE FROM hcm.reservation_rooms WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND reservation_id=${reservationId}::uuid`.execute(
        trx,
      );
    }
  } else {
    const inserted = await sql<{ reservation_id: string }>`
      INSERT INTO hcm.reservations
        (organization_id,property_id,channel_connection_id,external_reservation_id,channel_version,source_sequence,
         source_updated_at,status,oversold,primary_guest_id,check_in,check_out,property_timezone,currency,total_amount,tokenized_payment_reference,received_at,updated_at)
      VALUES (${orgId}::uuid,${reservation.propertyId}::uuid,${reservation.channelConnectionId}::uuid,${reservation.reservationId},
        ${reservation.channelVersion},${reservation.sourceSequence}::bigint,${reservation.sourceUpdatedAt ?? null}::timestamptz,
        ${wasCancelled ? "cancelled" : isNoShow ? "no_show" : reservation.status}::hcm.reservation_status,false,${primaryGuestId}::uuid,
        ${reservation.stay.checkIn}::date,${reservation.stay.checkOut}::date,${reservation.stay.propertyTimeZone},
        ${reservation.currency ?? null},${reservation.totalAmount ?? null},${reservation.paymentReference ?? null},${reservation.receivedAt}::timestamptz,now())
      RETURNING reservation_id
    `.execute(trx);
    reservationId = inserted.rows[0]!.reservation_id;
  }

  if (existing) {
    await sql`UPDATE hcm.reservations SET channel_version=${reservation.channelVersion},source_sequence=${reservation.sourceSequence}::bigint,
      source_updated_at=${reservation.sourceUpdatedAt ?? null}::timestamptz,
      status=${wasCancelled ? "cancelled" : isNoShow ? "no_show" : reservation.status}::hcm.reservation_status,
      oversold=false,primary_guest_id=${primaryGuestId}::uuid,check_in=${reservation.stay.checkIn}::date,check_out=${reservation.stay.checkOut}::date,
      property_timezone=${reservation.stay.propertyTimeZone},currency=${reservation.currency ?? null},total_amount=${reservation.totalAmount ?? null},
      tokenized_payment_reference=${reservation.paymentReference ?? null},updated_at=now()
      WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND reservation_id=${reservationId}::uuid`.execute(
      trx,
    );
  }

  const allocationState = await readInventoryRows(
    trx,
    orgId,
    reservation.propertyId,
    lockKeys,
  );
  const remainingUnits = new Map(
    [...allocationState].map(([key, row]) => [key, row.available_units]),
  );
  let oversold = false;
  if (!wasCancelled && !isNoShow) {
    for (let index = 0; index < roomInputs.length; index++) {
      const room = roomInputs[index]!;
      const ordinal = index + 1;
      await sql`INSERT INTO hcm.reservation_rooms (organization_id,property_id,reservation_id,room_ordinal,room_type_id,rate_plan_id,adults,children)
        VALUES (${orgId}::uuid,${reservation.propertyId}::uuid,${reservationId}::uuid,${ordinal},${room.roomTypeId}::uuid,${room.ratePlanId ?? null}::uuid,${room.adults},${room.children ?? 0})`.execute(
        trx,
      );
      for (const stayDate of enumerateNights(
        reservation.stay.checkIn,
        reservation.stay.checkOut,
      )) {
        const key = `${room.roomTypeId}|${stayDate}`;
        const inventory = before.get(key);
        if (!inventory)
          throw new ReservationApplyError(
            "INVENTORY_DAY_MISSING",
            `No inventory row for ${room.roomTypeId} ${stayDate}`,
            false,
          );
        let counted = false;
        if ((remainingUnits.get(key) ?? 0) > 0) {
          const changed =
            await sql`UPDATE hcm.inventory_days SET booked_units=booked_units+1,version=version+1,updated_at=now()
            WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND room_type_id=${room.roomTypeId}::uuid
              AND stay_date=${stayDate}::date AND booked_units < total_physical_units`.execute(
              trx,
            );
          counted = Number(changed.numAffectedRows) === 1;
          if (counted)
            remainingUnits.set(key, (remainingUnits.get(key) ?? 0) - 1);
        }
        if (!counted) {
          oversold = true;
          // OTA already confirmed the guest. Persist the booking but do not violate physical inventory checks.
          await sql`INSERT INTO hcm.oversell_conflicts (organization_id,property_id,reservation_id,room_type_id,stay_date,excess_units,status)
            VALUES (${orgId}::uuid,${reservation.propertyId}::uuid,${reservationId}::uuid,${room.roomTypeId}::uuid,${stayDate}::date,1,'open')`.execute(
            trx,
          );
        }
        await sql`INSERT INTO hcm.reservation_nights
          (organization_id,property_id,reservation_id,room_ordinal,stay_date,room_type_id,rate_plan_id,guest_id,nightly_amount,currency,inventory_counted)
          VALUES (${orgId}::uuid,${reservation.propertyId}::uuid,${reservationId}::uuid,${ordinal},${stayDate}::date,
            ${room.roomTypeId}::uuid,${room.ratePlanId ?? null}::uuid,${primaryGuestId}::uuid,NULL,${reservation.currency ?? null},${counted})`.execute(
          trx,
        );
      }
    }
    await sql`UPDATE hcm.reservations SET oversold=${oversold} WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND reservation_id=${reservationId}::uuid`.execute(
      trx,
    );
  } else if (wasCancelled && existing) {
    await sql`UPDATE hcm.reservation_nights SET inventory_counted=false WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND reservation_id=${reservationId}::uuid`.execute(
      trx,
    );
  }

  // Publish only dates whose final availability changed, plus a high-priority auto-close request on oversell.
  const after = await readInventoryRows(
    trx,
    orgId,
    reservation.propertyId,
    lockKeys,
  );
  const changedKeys = lockKeys.filter(
    (key) =>
      before.get(keyString(key))?.available_units !==
      after.get(keyString(key))?.available_units,
  );
  for (const key of changedKeys) {
    const row = after.get(keyString(key));
    if (!row) continue;
    const cloudEvent = {
      specversion: "1.0",
      id: `${event.id}:ari:${key.roomTypeId}:${key.stayDate}`,
      source: "core-state-engine",
      type: "com.hospitality.ari.delta",
      time: new Date().toISOString(),
      datacontenttype: "application/json",
      data: {
        tenantId: orgId,
        propertyId: reservation.propertyId,
        channelConnectionId: reservation.channelConnectionId,
        source: "channel_manager",
        roomTypeId: key.roomTypeId,
        stay: {
          startDate: key.stayDate,
          endDate: addDays(key.stayDate, 1),
          propertyTimeZone: reservation.stay.propertyTimeZone,
        },
        availability: row.available_units,
        sourceVersion: String(row.available_units),
        receivedAt: new Date().toISOString(),
      },
    };
    await insertOutbox(
      trx,
      orgId,
      reservation.propertyId,
      "room_type",
      key.roomTypeId,
      `com.hospitality.ari.delta`,
      cloudEvent,
    );
  }
  if (oversold) {
    const mappingRows = await sql<{
      channel_connection_id: string;
    }>`SELECT DISTINCT channel_connection_id FROM hcm.channel_mappings
      WHERE organization_id=${orgId}::uuid AND property_id=${reservation.propertyId}::uuid AND active=true
        AND room_type_id IN (${sql.join(roomInputs.map((room) => sql`${room.roomTypeId}::uuid`))})`.execute(
      trx,
    );
    const autoClose = {
      specversion: "1.0",
      id: `${event.id}:auto-close`,
      source: "core-state-engine",
      type: "com.hospitality.inventory.propagation.requested",
      time: new Date().toISOString(),
      datacontenttype: "application/json",
      data: {
        tenantId: orgId,
        propertyId: reservation.propertyId,
        operation: "auto_close_other_channels",
        priority: "reservation",
        excludeConnectionId: reservation.channelConnectionId,
        channelConnectionIds: mappingRows.rows.map(
          (row) => row.channel_connection_id,
        ),
        dates: proposedNights.filter(
          (key) =>
            !before.get(keyString(key)) ||
            before.get(keyString(key))!.available_units === 0,
        ),
        reservationId: reservationId,
        conflictStatus: "open",
      },
    };
    await insertOutbox(
      trx,
      orgId,
      reservation.propertyId,
      "reservation",
      reservationId!,
      "com.hospitality.inventory.propagation.requested",
      autoClose,
    );
  }
  return {
    committed: true,
    outcome: "applied",
    ...(reservationId ? { reservationId } : {}),
    oversold,
  };
}

async function lockInventoryRows(
  trx: Transaction<HcmDatabase>,
  orgId: UUID,
  propertyId: UUID,
  keys: InventoryKey[],
): Promise<Map<string, InventoryRow>> {
  if (keys.length === 0) return new Map();
  const values = sql.join(
    keys.map((key) => sql`(${key.roomTypeId}::uuid,${key.stayDate}::date)`),
  );
  const rows =
    await sql<InventoryRow>`SELECT room_type_id AS "roomTypeId",stay_date::text AS "stayDate",total_physical_units,booked_units,available_units
    FROM hcm.inventory_days WHERE organization_id=${orgId}::uuid AND property_id=${propertyId}::uuid
      AND (room_type_id,stay_date) IN (${values}) ORDER BY room_type_id,stay_date FOR UPDATE`.execute(
      trx,
    );
  const map = new Map(
    rows.rows.map((row) => [
      keyString(row),
      {
        ...row,
        total_physical_units: Number(row.total_physical_units),
        booked_units: Number(row.booked_units),
        available_units: Number(row.available_units),
      },
    ]),
  );
  if (map.size !== keys.length)
    throw new ReservationApplyError(
      "INVENTORY_DAY_MISSING",
      "At least one requested or existing reservation night has no inventory row",
      false,
    );
  return map;
}
async function readInventoryRows(
  trx: Transaction<HcmDatabase>,
  orgId: UUID,
  propertyId: UUID,
  keys: InventoryKey[],
): Promise<Map<string, InventoryRow>> {
  if (keys.length === 0) return new Map();
  const values = sql.join(
    keys.map((key) => sql`(${key.roomTypeId}::uuid,${key.stayDate}::date)`),
  );
  const result =
    await sql<InventoryRow>`SELECT room_type_id AS "roomTypeId",stay_date::text AS "stayDate",total_physical_units,booked_units,available_units
    FROM hcm.inventory_days WHERE organization_id=${orgId}::uuid AND property_id=${propertyId}::uuid AND (room_type_id,stay_date) IN (${values})`.execute(
      trx,
    );
  return new Map(
    result.rows.map((row) => [
      keyString(row),
      {
        ...row,
        total_physical_units: Number(row.total_physical_units),
        booked_units: Number(row.booked_units),
        available_units: Number(row.available_units),
      },
    ]),
  );
}
async function releaseOldAllocation(
  trx: Transaction<HcmDatabase>,
  orgId: UUID,
  propertyId: UUID,
  reservationId: UUID,
  nights: NightRow[],
): Promise<void> {
  for (const night of nights)
    if (night.inventory_counted) {
      await sql`UPDATE hcm.inventory_days SET booked_units=booked_units-1,version=version+1,updated_at=now()
      WHERE organization_id=${orgId}::uuid AND property_id=${propertyId}::uuid AND room_type_id=${night.roomTypeId}::uuid
        AND stay_date=${night.stayDate}::date AND booked_units > 0`.execute(
        trx,
      );
    }
}
async function insertOutbox(
  trx: Transaction<HcmDatabase>,
  orgId: UUID,
  propertyId: UUID,
  aggregateType: string,
  aggregateId: string,
  eventType: string,
  payload: unknown,
): Promise<void> {
  await sql`INSERT INTO hcm.outbox_events (organization_id,aggregate_type,aggregate_id,aggregate_version,event_type,payload,partition_key)
    VALUES (${orgId}::uuid,${aggregateType},${aggregateId},NULL,${eventType},${JSON.stringify(payload)}::jsonb,${propertyId})`.execute(
    trx,
  );
}
function enumerateNights(start: string, end: string): string[] {
  const dates: string[] = [];
  for (let date = start; date < end; date = addDays(date, 1)) dates.push(date);
  return dates;
}
function addDays(date: string, days: number): string {
  const value = new Date(`${date}T00:00:00.000Z`);
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}
function keyString(key: InventoryKey): string {
  return `${key.roomTypeId}|${key.stayDate}`;
}
function dedupeKeys(keys: InventoryKey[]): InventoryKey[] {
  const map = new Map(keys.map((key) => [keyString(key), key]));
  return [...map.values()].sort(
    (left, right) =>
      left.roomTypeId.localeCompare(right.roomTypeId) ||
      left.stayDate.localeCompare(right.stayDate),
  );
}
function isTransientDatabaseError(error: unknown): boolean {
  const code = getSqlState(error) ?? "";
  return (
    code.startsWith("08") ||
    [
      "40001",
      "40P01",
      "55P03",
      "57014",
      "53300",
      "57P01",
      "57P02",
      "57P03",
      "ECONNRESET",
      "ECONNREFUSED",
      "ETIMEDOUT",
      "EPIPE",
    ].includes(code)
  );
}
function getSqlState(error: unknown): string | undefined {
  if (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    typeof error.code === "string"
  )
    return error.code;
  return undefined;
}

export interface OutboxRelayOptions {
  leaseMs?: number;
  publishTimeoutMs?: number;
}
/** At-least-once relay: lease in a short DB transaction, publish outside it, then mark the row. */
export async function relayOutboxBatch(
  db: Kysely<HcmDatabase>,
  producer: Producer,
  options: OutboxRelayOptions = {},
): Promise<number> {
  const workerId = `outbox-${process.pid}-${Math.random().toString(16).slice(2)}`;
  const claimed = await db.transaction().execute(async (trx) => {
    await sql`SET LOCAL ROLE hcm_worker`.execute(trx);
    const tenant = await sql<{
      organization_id: string;
      partition_key: string;
    }>`
      SELECT o.organization_id,o.partition_key FROM hcm.outbox_events o
      WHERE o.status='pending' AND (o.claimed_until IS NULL OR o.claimed_until < now())
        AND o.partition_key IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM hcm.outbox_events prior WHERE prior.partition_key=o.partition_key
          AND prior.status <> 'published' AND (prior.created_at,prior.id)<(o.created_at,o.id))
      ORDER BY o.created_at,o.id LIMIT 1 FOR UPDATE OF o SKIP LOCKED
    `.execute(trx);
    const orgId = tenant.rows[0]?.organization_id;
    const partitionKey = tenant.rows[0]?.partition_key;
    if (!orgId || !partitionKey) return [];
    await sql`SELECT set_config('app.organization_id',${orgId},true)`.execute(
      trx,
    );
    return (
      await sql<{
        organization_id: string;
        id: string;
        payload: Record<string, unknown>;
        event_type: string;
        attempts: number;
        created_at: Date;
      }>`
      WITH candidates AS (
        SELECT current_event.organization_id,current_event.id FROM hcm.outbox_events AS current_event
        WHERE current_event.organization_id=${orgId}::uuid AND current_event.partition_key=${partitionKey} AND current_event.status='pending' AND (current_event.claimed_until IS NULL OR current_event.claimed_until < now())
          AND NOT EXISTS (SELECT 1 FROM hcm.outbox_events prior WHERE prior.partition_key=current_event.partition_key
            AND prior.status <> 'published' AND (prior.created_at,prior.id)<(current_event.created_at,current_event.id))
        ORDER BY current_event.created_at,current_event.id LIMIT 1 FOR UPDATE OF current_event SKIP LOCKED
      )
      UPDATE hcm.outbox_events AS o SET claimed_by=${workerId},claimed_until=now()+(${options.leaseMs ?? 30_000}::text || ' milliseconds')::interval
      FROM candidates c WHERE o.organization_id=c.organization_id AND o.id=c.id
      RETURNING o.organization_id,o.id,o.payload,o.event_type,o.attempts,o.created_at
    `.execute(trx)
    ).rows;
  });

  claimed.sort(
    (a, b) =>
      a.created_at.getTime() - b.created_at.getTime() ||
      a.id.localeCompare(b.id),
  );
  for (const row of claimed) {
    const event = row.payload as {
      data?: { propertyId?: string; property_id?: string };
    };
    const propertyId =
      event.data?.propertyId ?? event.data?.property_id ?? row.organization_id;
    const topic = row.event_type.startsWith("com.hospitality.reservation.")
      ? "reservation.canonical.events"
      : "inventory.delta.events";
    try {
      await producer.send({
        topic,
        acks: -1,
        timeout: options.publishTimeoutMs ?? 2_000,
        messages: [
          {
            key: propertyId,
            value: JSON.stringify(row.payload),
            headers: {
              "outbox-id": row.id,
              "organization-id": row.organization_id,
            },
          },
        ],
      });
      await updateOutboxRow(db, row.organization_id, (trx) =>
        sql`UPDATE hcm.outbox_events SET status='published',published_at=now(),attempts=attempts+1,last_error=NULL,claimed_by=NULL,claimed_until=NULL
        WHERE organization_id=${row.organization_id}::uuid AND id=${row.id}::uuid AND claimed_by=${workerId}`.execute(
          trx,
        ),
      );
    } catch (error) {
      const reason = error instanceof Error ? error.name : "publish_error";
      if (Number(row.attempts) + 1 >= 15) {
        try {
          await producer.send({
            topic: "dead.letter.queue",
            acks: -1,
            messages: [
              {
                key: propertyId,
                value: JSON.stringify({
                  specversion: "1.0",
                  id: row.id,
                  source: "outbox-relay",
                  type: "com.hospitality.inventory.propagation.failed",
                  time: new Date().toISOString(),
                  data: {
                    organizationId: row.organization_id,
                    outboxId: row.id,
                    eventType: row.event_type,
                    error: reason,
                    payload: row.payload,
                  },
                }),
              },
            ],
          });
          await updateOutboxRow(db, row.organization_id, (trx) =>
            sql`UPDATE hcm.outbox_events SET status='dead_letter',attempts=attempts+1,last_error=${reason},claimed_by=NULL,claimed_until=NULL
            WHERE organization_id=${row.organization_id}::uuid AND id=${row.id}::uuid AND claimed_by=${workerId}`.execute(
              trx,
            ),
          );
        } catch {
          await updateOutboxRow(db, row.organization_id, (trx) =>
            sql`UPDATE hcm.outbox_events SET attempts=attempts+1,last_error='dead_letter_publish_failed',claimed_by=NULL,claimed_until=NULL
            WHERE organization_id=${row.organization_id}::uuid AND id=${row.id}::uuid AND claimed_by=${workerId}`.execute(
              trx,
            ),
          );
        }
      } else {
        await updateOutboxRow(db, row.organization_id, (trx) =>
          sql`UPDATE hcm.outbox_events SET attempts=attempts+1,last_error=${reason},claimed_by=NULL,claimed_until=NULL
          WHERE organization_id=${row.organization_id}::uuid AND id=${row.id}::uuid AND claimed_by=${workerId}`.execute(
            trx,
          ),
        );
      }
    }
  }
  return claimed.length;
}

async function updateOutboxRow<T>(
  db: Kysely<HcmDatabase>,
  organizationId: string,
  fn: (trx: Transaction<HcmDatabase>) => Promise<T>,
): Promise<T> {
  return db.transaction().execute(async (trx) => {
    await sql`SET LOCAL ROLE hcm_worker`.execute(trx);
    await sql`SELECT set_config('app.organization_id',${organizationId},true)`.execute(
      trx,
    );
    return fn(trx);
  });
}

export async function runOutboxRelay(
  db: Kysely<HcmDatabase>,
  producer: Producer,
  signal?: AbortSignal,
): Promise<void> {
  while (!signal?.aborted) {
    try {
      const count = await relayOutboxBatch(db, producer);
      if (count === 0) await new Promise((resolve) => setTimeout(resolve, 250));
    } catch (error) {
      console.error("Outbox relay iteration failed", error);
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
  }
}

export async function processCanonicalReservationMessage(
  payload: EachMessagePayload,
  db: Kysely<HcmDatabase>,
  cache: IdempotencyCache | undefined,
  producer: Producer,
): Promise<void> {
  const raw = payload.message.value?.toString("utf8") ?? "";
  let decoded: unknown;
  try {
    decoded = JSON.parse(raw);
  } catch {
    decoded = undefined;
  }
  const envelope = CloudEventSchema.safeParse(decoded);
  const key = payload.message.key?.toString() ?? "unknown-property";
  if (
    !envelope.success ||
    ![
      "com.hospitality.reservation.created",
      "com.hospitality.reservation.modified",
      "com.hospitality.reservation.cancelled",
      "com.hospitality.reservation.no_show",
    ].includes(envelope.data.type)
  ) {
    await publishStateDlq(
      producer,
      key,
      payload,
      "INVALID_RESERVATION_EVENT",
      envelope.success
        ? "Unsupported event type"
        : "Invalid CloudEvent envelope",
      decoded,
    );
    return;
  }
  const reservationResult = ReservationSchema.safeParse(envelope.data.data);
  if (!reservationResult.success) {
    await publishStateDlq(
      producer,
      key,
      payload,
      "INVALID_RESERVATION",
      reservationResult.error.issues
        .map((issue) => `${issue.path.join(".")}:${issue.code}`)
        .join(","),
      decoded,
    );
    return;
  }
  try {
    const event = {
      ...envelope.data,
      data: reservationResult.data,
    } as CloudEvent<Reservation, ReservationEventType>;
    await withSpan("reservation.apply", { "hcm.organization_id": event.data.tenantId, "hcm.property_id": event.data.propertyId, "messaging.kafka.message_id": event.id }, () => applyReservationEvent(db, cache, event));
  } catch (error) {
    const retryable =
      error instanceof ReservationApplyError
        ? error.retryable
        : isTransientDatabaseError(error);
    if (retryable) throw error; // Leave offset uncommitted for Kafka redelivery.
    await publishStateDlq(
      producer,
      key,
      payload,
      error instanceof ReservationApplyError
        ? error.code
        : "RESERVATION_APPLY_FAILED",
      "Permanent reservation state failure",
      decoded,
    );
  }
}

async function publishStateDlq(
  producer: Producer,
  key: string,
  payload: EachMessagePayload,
  code: string,
  detail: string,
  raw: unknown,
): Promise<void> {
  const event = {
    specversion: "1.0",
    id: `${payload.topic}:${payload.partition}:${payload.message.offset}`,
    source: "core-state-engine",
    type: "com.hospitality.webhook.received",
    time: new Date().toISOString(),
    data: {
      sourceTopic: payload.topic,
      sourcePartition: payload.partition,
      sourceOffset: payload.message.offset,
      errorCode: code,
      errorDetail: detail,
      raw,
    },
  };
  await producer.send({
    topic: "dead.letter.queue",
    acks: -1,
    messages: [{ key, value: JSON.stringify(event) }],
  });
}

export async function runReservationConsumer(
  consumer: Consumer,
  db: Kysely<HcmDatabase>,
  cache: IdempotencyCache | undefined,
  producer: Producer,
): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({
    topic: "reservation.canonical.events",
    fromBeginning: false,
  });
  await consumer.run({
    autoCommit: false,
    eachMessage: async (payload) => {
      await processCanonicalReservationMessage(payload, db, cache, producer);
      await consumer.commitOffsets([
        {
          topic: payload.topic,
          partition: payload.partition,
          offset: (BigInt(payload.message.offset) + 1n).toString(),
        },
      ]);
    },
  });
}

async function main(): Promise<void> {
  const config = loadConfig();
  if (!config.databaseUrl) throw new Error("DATABASE_URL is required");
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: 20,
    application_name: "core-state-engine",
  });
  const db = new KyselyConstructor<HcmDatabase>({
    dialect: new PostgresDialect({ pool }),
  });
  const workerUrl = process.env.WORKER_DATABASE_URL;
  if (!workerUrl)
    throw new Error(
      "WORKER_DATABASE_URL is required for the cross-tenant outbox relay role",
    );
  const workerPool = new Pool({
    connectionString: workerUrl,
    max: 5,
    application_name: "core-outbox-worker",
  });
  const workerDb = new KyselyConstructor<HcmDatabase>({
    dialect: new PostgresDialect({ pool: workerPool }),
  });
  const logger = createLogger({ ...config, serviceName: "core-state-engine" });
  const redis = createRedisClient(config);
  const kafka = createKafkaClient({
    ...config,
    serviceName: "core-state-engine",
  });
  const producer = createKafkaProducer(kafka);
  const consumer = createKafkaConsumer(
    kafka,
    process.env.KAFKA_GROUP_ID ?? "hcm-core-state-v1",
  );
  await producer.connect();
  logger.info("Core state engine dependencies connected");
  const abort = new AbortController();
  process.once("SIGTERM", () => abort.abort());
  process.once("SIGINT", () => abort.abort());
  await Promise.all([
    runReservationConsumer(consumer, db, redis, producer),
    runOutboxRelay(workerDb, producer, abort.signal),
  ]);
  await Promise.all([
    consumer.disconnect(),
    db.destroy(),
    workerDb.destroy(),
    redis.quit(),
    producer.disconnect(),
    pool.end(),
    workerPool.end(),
  ]);
}
if (
  process.argv[1] &&
  import.meta.url ===
    new URL(`file://${process.argv[1].replaceAll("\\", "/")}`).href
) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
