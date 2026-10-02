import { sql, type Kysely } from "kysely";
import type { HcmDatabase, UUID } from "@hcm/db";
import type { BaseChannelAdapter } from "@hcm/adapter-base";
import type { Redis } from "ioredis";
import { createLogger, withSpan } from "@hcm/shared";

const logger = createLogger({ serviceName: "reconciler", logLevel: "info", nodeEnv: "production" });

export interface ReconciliationProperty { organizationId: UUID; propertyId: UUID; timezone: string; }
export interface AvailabilityPoint { roomTypeId: UUID; stayDate: string; units: number; }
export interface PmsInventoryTruthReader {
  readTruth(property: ReconciliationProperty, fromDate: string, toDate: string): Promise<AvailabilityPoint[]>;
}
export interface ReconciliationTarget {
  connectionId: UUID;
  adapter: BaseChannelAdapter;
  readReportedAvailability(fromDate: string, toDate: string): Promise<AvailabilityPoint[]>;
}
export interface ReconciliationTargetProvider {
  listTargets(property: ReconciliationProperty): Promise<ReconciliationTarget[]>;
}
export interface ReconciliationPropertyProvider { listProperties(): Promise<ReconciliationProperty[]>; }
export interface DriftItem { roomTypeId: UUID; stayDate: string; expectedUnits: number; reportedUnits: number | null; channelConnectionId: UUID; }
export interface ReconciliationStore {
  record(input: { property: ReconciliationProperty; channelConnectionId: UUID; drift: DriftItem[]; corrected: boolean; startedAt: Date }): Promise<void>;
}

/** Worker transactions explicitly set organization context, and hcm_worker predicates remain tenant-scoped despite BYPASSRLS. */
export class KyselyReconciliationStore implements ReconciliationStore {
  constructor(private readonly db: Kysely<HcmDatabase>) {}
  async record(input: { property: ReconciliationProperty; channelConnectionId: UUID; drift: DriftItem[]; corrected: boolean; startedAt: Date }): Promise<void> {
    await this.db.transaction().execute(async (trx) => {
      await sql`SET LOCAL ROLE hcm_worker`.execute(trx);
      await sql`SELECT set_config('app.organization_id', ${input.property.organizationId}, true)`.execute(trx);
      const finished = new Date();
      await trx.insertInto("sync_logs").values({
        organization_id: input.property.organizationId,
        property_id: input.property.propertyId,
        channel_connection_id: input.channelConnectionId,
        direction: "reconciliation",
        status: input.drift.length === 0 || input.corrected ? "succeeded" : "failed",
        operation: "nightly_availability_reconciliation",
        correlation_id: null,
        external_reference: null,
        http_status: null,
        duration_ms: finished.getTime() - input.startedAt.getTime(),
        sanitized_error_code: input.drift.length && !input.corrected ? "DRIFT_UNCORRECTED" : null,
        details: { driftCount: input.drift.length, corrected: input.corrected, drift: input.drift } as never,
      }).execute();
      await trx.insertInto("audit_log").values({
        organization_id: input.property.organizationId,
        property_id: input.property.propertyId,
        actor_user_id: null,
        action: input.corrected ? "reconciliation.full_sync_requested" : "reconciliation.checked",
        entity_type: "channel_connection",
        entity_id: input.channelConnectionId,
        correlation_id: null,
        before_data: null,
        after_data: { driftCount: input.drift.length, corrected: input.corrected } as never,
      }).execute();
    });
  }
}

function dayInZone(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit" }).format(date);
}
function addDays(isoDate: string, count: number): string {
  const value = new Date(`${isoDate}T00:00:00.000Z`); value.setUTCDate(value.getUTCDate() + count); return value.toISOString().slice(0, 10);
}
function pointMap(points: AvailabilityPoint[]): Map<string, number> { return new Map(points.map((p) => [`${p.roomTypeId}|${p.stayDate}`, p.units])); }

export interface ReconcilerOptions { horizonDays?: number; }
export async function reconcileProperty(property: ReconciliationProperty, pms: PmsInventoryTruthReader,
  targets: ReconciliationTargetProvider, store: ReconciliationStore, now = new Date(), options: ReconcilerOptions = {}): Promise<number> {
  const fromDate = dayInZone(now, property.timezone);
  const toDate = addDays(fromDate, options.horizonDays ?? 365);
  const truth = await pms.readTruth(property, fromDate, toDate);
  const expected = pointMap(truth);
  const destinations = await targets.listTargets(property);
  let driftCount = 0;
  for (const target of destinations) {
    const startedAt = new Date();
    let reported: AvailabilityPoint[] = [];
    let readFailed = false;
    try { reported = await target.readReportedAvailability(fromDate, toDate); }
    catch (error) { readFailed = true; logger.error({ err: error, propertyId: property.propertyId, connectionId: target.connectionId }, "reconciliation read failed"); }
    const actual = pointMap(reported);
    const keys = new Set([...expected.keys(), ...actual.keys()]);
    const drift: DriftItem[] = [];
    for (const key of keys) {
      const [roomTypeId, stayDate] = key.split("|") as [UUID, string];
      const expectedUnits = expected.get(key) ?? 0;
      const reportedUnits = actual.has(key) ? actual.get(key)! : null;
      if (reportedUnits !== expectedUnits) drift.push({ roomTypeId, stayDate, expectedUnits, reportedUnits, channelConnectionId: target.connectionId });
    }
    driftCount += drift.length;
    let corrected = !readFailed && drift.length === 0;
    if (drift.length > 0 && !readFailed) {
      const roomTypeIds = [...new Set(drift.map((d) => d.roomTypeId))];
      const driftDates = drift.map((d) => d.stayDate).sort();
      const result = await target.adapter.fullSync({ organizationId: property.organizationId, propertyId: property.propertyId,
        channelConnectionId: target.connectionId, roomTypeIds, fromDate: driftDates[0]!, toDate: addDays(driftDates.at(-1)!, 1) });
      corrected = result.ok;
    }
    await store.record({ property, channelConnectionId: target.connectionId, drift, corrected, startedAt });
  }
  return driftCount;
}

/** Property timezone is authoritative; default window is 02:00–04:00 local time, configurable per deployment. */
export function isOffPeakWindow(now: Date, timezone: string, startHour = 2, endHour = 4): boolean {
  const hour = Number(new Intl.DateTimeFormat("en-US", { timeZone: timezone, hour: "2-digit", hourCycle: "h23" }).format(now));
  return hour >= startHour && hour < endHour;
}
export async function runNightlyReconciliation(properties: ReconciliationPropertyProvider, pms: PmsInventoryTruthReader,
  targets: ReconciliationTargetProvider, store: ReconciliationStore, redis: Redis, now = new Date()): Promise<void> {
  for (const property of await properties.listProperties()) {
    if (!isOffPeakWindow(now, property.timezone)) continue;
    const localDate = dayInZone(now, property.timezone);
    const claim = `hcm:reconciler:${property.organizationId}:${property.propertyId}:${localDate}`;
    if (!(await redis.set(claim, "running", "EX", 21_600, "NX"))) continue;
    try { await withSpan("inventory.reconcile.property", { "hcm.organization_id": property.organizationId, "hcm.property_id": property.propertyId }, () => reconcileProperty(property, pms, targets, store, now)); await redis.set(claim, "done", "EX", 172_800); }
    catch (error) { await redis.del(claim); logger.error({ err: error, propertyId: property.propertyId }, "nightly reconciliation failed"); }
  }
}
// OUT OF SCOPE (Part 4B): deployment CronJob wiring, dashboard drift views, and live SSE.
