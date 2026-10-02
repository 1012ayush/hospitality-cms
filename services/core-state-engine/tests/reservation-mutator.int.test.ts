import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { Kysely, PostgresDialect } from "kysely";
import { Pool } from "pg";
import type { HcmDatabase, UUID } from "@hcm/db";
import type { CloudEvent, Reservation } from "@hcm/hospitality-contracts";
import { applyReservationEvent, type IdempotencyCache } from "../src/index.js";
let container: StartedTestContainer;
let pool: Pool;
let db: Kysely<HcmDatabase>;
let orgId: UUID;
let propertyId: UUID;
let roomTypeId: UUID;
let connectionId: UUID;
const dates = ["2030-01-01", "2030-01-02", "2030-01-03", "2030-01-04"];
beforeAll(async () => {
  container = await new GenericContainer("postgres:16-bookworm")
    .withEnvironment({
      POSTGRES_DB: "hcm_test",
      POSTGRES_USER: "hcm_test",
      POSTGRES_PASSWORD: "hcm_test",
    })
    .withExposedPorts(5432)
    .start();
  pool = new Pool({
    connectionString: `postgres://hcm_test:hcm_test@${container.getHost()}:${container.getMappedPort(5432)}/hcm_test`,
    max: 60,
  });
  const client = await pool.connect();
  try {
    const names = [
      "0001_foundation.sql",
      "0002_catalog.sql",
      "0003_inventory.sql",
      "0004_channels.sql",
      "0005_reservations.sql",
      "0006_events_ops.sql",
      "0007_partitions_rls.sql",
      "0008_indexes.sql",
      "0009_indexes_concurrently.sql",
      "0010_schema_fixes.sql",
      "0011_reservation_night_allocation.sql",
    ];
    for (const name of names)
      await client.query(
        await readFile(
          resolve(process.cwd(), "../../packages/db/migrations", name),
          "utf8",
        ),
      );
    await client.query(
      await readFile(
        resolve(process.cwd(), "../../packages/db/roles-and-grants.sql"),
        "utf8",
      ),
    );
  } finally {
    client.release();
  }
  db = new Kysely<HcmDatabase>({ dialect: new PostgresDialect({ pool }) });
}, 120_000);
afterAll(async () => {
  await db?.destroy();
  await pool?.end();
  await container?.stop();
}, 30_000);
beforeEach(async () => {
  orgId = randomUUID();
  propertyId = randomUUID();
  roomTypeId = randomUUID();
  connectionId = randomUUID();
  await pool.query(
    "INSERT INTO hcm.organizations (organization_id,name,slug) VALUES ($1,'Integration',$2)",
    [orgId, `org-${orgId}`],
  );
  await pool.query(
    "INSERT INTO hcm.properties (organization_id,property_id,name,code,timezone,currency) VALUES ($1,$2,'Test',$3,'America/New_York','USD')",
    [orgId, propertyId, `P-${propertyId}`],
  );
  const channel = randomUUID();
  await pool.query(
    "INSERT INTO hcm.channels (organization_id,channel_id,kind,name) VALUES ($1,$2,'booking_com','Test')",
    [orgId, channel],
  );
  await pool.query(
    "INSERT INTO hcm.channel_connections (organization_id,property_id,channel_connection_id,channel_id,credential_secret_ref) VALUES ($1,$2,$3,$4,'secret://test')",
    [orgId, propertyId, connectionId, channel],
  );
  await pool.query(
    "INSERT INTO hcm.room_types (organization_id,property_id,room_type_id,code,name,physical_units) VALUES ($1,$2,$3,'KING','King',1)",
    [orgId, propertyId, roomTypeId],
  );
  for (const date of dates)
    await pool.query(
      "INSERT INTO hcm.inventory_days (organization_id,property_id,room_type_id,stay_date,total_physical_units) VALUES ($1,$2,$3,$4::date,1)",
      [orgId, propertyId, roomTypeId, date],
    );
});
function evt(
  id: string,
  seq: number,
  start = dates[0]!,
  end = dates[1]!,
  kind: "created" | "modified" | "cancelled" | "no_show" = "created",
) {
  const data: Reservation = {
    tenantId: orgId,
    propertyId,
    channelConnectionId: connectionId,
    channel: "booking_com",
    reservationId: id,
    channelVersion: `opaque-${seq}`,
    sourceSequence: seq,
    lifecycle: kind === "created" ? "new" : kind,
    status:
      kind === "cancelled"
        ? "cancelled"
        : kind === "no_show"
          ? "no_show"
          : "confirmed",
    roomTypeId,
    rooms: [{ roomTypeId, adults: 2, children: 0 }],
    stay: {
      checkIn: start,
      checkOut: end,
      propertyTimeZone: "America/New_York",
    },
    occupancy: { adults: 2, children: 0 },
    receivedAt: new Date().toISOString(),
    oversold: false,
  };
  return {
    specversion: "1.0" as const,
    id: randomUUID(),
    source: "test",
    type: `com.hospitality.reservation.${kind}` as const,
    time: new Date().toISOString(),
    data,
  } as CloudEvent<
    Reservation,
    `com.hospitality.reservation.${"created" | "modified" | "cancelled" | "no_show"}`
  >;
}
const redisDown: IdempotencyCache = {
  async get() {
    throw Error("redis down");
  },
  async set() {
    throw Error("redis down");
  },
};
describe("transactional reservation mutator", () => {
  it("serializes 50 last-room bookings, accepts confirmed oversells, and never makes inventory negative", async () => {
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        applyReservationEvent(db, undefined, evt(`booking-${i}`, 1)),
      ),
    );
    expect(
      results.filter((x) => x.outcome === "applied" && !x.oversold),
    ).toHaveLength(1);
    expect(
      results.filter((x) => x.outcome === "applied" && x.oversold),
    ).toHaveLength(49);
    const inv = await pool.query(
      "SELECT booked_units,available_units FROM hcm.inventory_days WHERE organization_id=$1 AND room_type_id=$2 AND stay_date=$3::date",
      [orgId, roomTypeId, dates[0]],
    );
    expect(Number(inv.rows[0].booked_units)).toBe(1);
    expect(Number(inv.rows[0].available_units)).toBe(0);
  });
  it("deduplicates a repeated event through the Postgres idempotency authority", async () => {
    const e = evt("dup", 1);
    expect((await applyReservationEvent(db, undefined, e)).outcome).toBe(
      "applied",
    );
    expect((await applyReservationEvent(db, undefined, e)).outcome).toBe(
      "duplicate",
    );
  });
  it("ignores stale source_sequence after a newer version", async () => {
    await applyReservationEvent(db, undefined, evt("seq", 8));
    expect(
      (
        await applyReservationEvent(
          db,
          undefined,
          evt("seq", 7, dates[1]!, dates[2]!, "modified"),
        )
      ).outcome,
    ).toBe("stale");
    const r = await pool.query(
      "SELECT source_sequence::text,check_in::text FROM hcm.reservations WHERE organization_id=$1",
      [orgId],
    );
    expect(r.rows[0]).toMatchObject({
      source_sequence: "8",
      check_in: dates[0],
    });
  });
  it("releases old inventory on modify then cancel", async () => {
    await applyReservationEvent(db, undefined, evt("move", 1));
    await applyReservationEvent(
      db,
      undefined,
      evt("move", 2, dates[2]!, dates[3]!, "modified"),
    );
    const a = await pool.query(
      "SELECT stay_date::text FROM hcm.inventory_days WHERE organization_id=$1 AND room_type_id=$2 AND booked_units=1",
      [orgId, roomTypeId],
    );
    expect(a.rows.map((x) => x.stay_date)).toEqual([dates[2]]);
    await applyReservationEvent(
      db,
      undefined,
      evt("move", 3, dates[2]!, dates[3]!, "cancelled"),
    );
    const b = await pool.query(
      "SELECT sum(booked_units)::int AS n FROM hcm.inventory_days WHERE organization_id=$1",
      [orgId],
    );
    expect(b.rows[0].n).toBe(0);
  });
  it("deduplicates a redelivery after simulated commit-before-offset-commit crash", async () => {
    const e = evt("crash", 1);
    await applyReservationEvent(db, undefined, e);
    expect((await applyReservationEvent(db, undefined, e)).outcome).toBe(
      "duplicate",
    );
  });
  it("continues through Redis failure because Postgres is authoritative", async () => {
    expect(
      (await applyReservationEvent(db, redisDown, evt("redis-down", 1)))
        .outcome,
    ).toBe("applied");
  });
});
