import { CloudEventSchema, AriDeltaSchema } from "@hcm/hospitality-contracts";
import { sql, type Kysely } from "kysely";
import type { Producer } from "kafkajs";
import type { HcmDatabase, UUID } from "@hcm/db";
import type { AdapterError } from "@hcm/adapter-base";
import type { CloudEvent, AriDelta } from "@hcm/hospitality-contracts";

/** Replays one persisted inventory-delta DLQ event; send is durable before marking replayed (at-least-once on crash). */
export async function replayInventoryDlq(db: Kysely<HcmDatabase>, producer: Producer, input: { organizationId: UUID; dlqEventId: UUID; actorUserId?: UUID }): Promise<void> {
  const row = await db.transaction().execute(async (trx) => {
    await sql`SET LOCAL ROLE hcm_worker`.execute(trx);
    await sql`SELECT set_config('app.organization_id', ${input.organizationId}, true)`.execute(trx);
    return trx.selectFrom("dlq_events").selectAll().where("organization_id", "=", input.organizationId)
      .where("dlq_event_id", "=", input.dlqEventId).where("source_topic", "=", "inventory.delta.events")
      .where("replayed_at", "is", null).executeTakeFirst();
  });
  if (!row) throw new Error("DLQ event not found, wrong source topic, or already replayed");
  const envelope = CloudEventSchema.parse(row.sanitized_payload);
  if (envelope.type !== "com.hospitality.ari.delta") throw new Error("DLQ event is not an inventory delta");
  const delta = AriDeltaSchema.parse(envelope.data);
  if (delta.tenantId !== input.organizationId) throw new Error("Tenant mismatch in replay payload");
  await producer.send({ topic: "inventory.delta.events", acks: -1, messages: [{ key: delta.propertyId, value: JSON.stringify(envelope) }] });
  await db.transaction().execute(async (trx) => {
    await sql`SET LOCAL ROLE hcm_worker`.execute(trx);
    await sql`SELECT set_config('app.organization_id', ${input.organizationId}, true)`.execute(trx);
    await trx.updateTable("dlq_events").set({ replayed_at: new Date(), replayed_by: input.actorUserId ?? null })
      .where("organization_id", "=", input.organizationId).where("dlq_event_id", "=", input.dlqEventId)
      .where("replayed_at", "is", null).executeTakeFirstOrThrow();
  });
}
// CLI uses WORKER_DATABASE_URL; actor identity must be supplied from the authenticated operator context.

/** Durable store used before publishing the Kafka DLQ copy; supports replay even if the DLQ topic is temporarily unavailable. */
export class KyselyOutboundDeadLetterStore {
  constructor(private readonly db: Kysely<HcmDatabase>) {}
  async persist(input: { organizationId: UUID; propertyId: UUID; event: CloudEvent<AriDelta>; error: AdapterError; operation: string }): Promise<void> {
    await this.db.transaction().execute(async (trx) => {
      await sql`SET LOCAL ROLE hcm_worker`.execute(trx);
      await sql`SELECT set_config('app.organization_id', ${input.organizationId}, true)`.execute(trx);
      const existing = await trx.selectFrom("dlq_events").select("dlq_event_id")
        .where("organization_id", "=", input.organizationId).where("source_topic", "=", "inventory.delta.events")
        .where("event_id", "=", input.event.id).where("replayed_at", "is", null).executeTakeFirst();
      if (existing) return;
      await trx.insertInto("dlq_events").values({ organization_id: input.organizationId, source_topic: "inventory.delta.events",
        source_partition: null, source_offset: null, event_id: input.event.id, event_type: input.event.type,
        sanitized_payload: input.event as never, error_code: errorCode(input.error),
        error_detail: `${input.operation}:${input.error.classification}:${input.error.code}`.slice(0, 1000),
        replayed_at: null, replayed_by: null }).execute();
    });
  }
}
function errorCode(error: AdapterError): string { return `${error.classification.toUpperCase()}_${error.code}`.slice(0, 120); }
