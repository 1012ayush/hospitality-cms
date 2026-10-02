import { Kysely, PostgresDialect } from "kysely";
import { Pool } from "pg";
import { createKafkaClient, createKafkaProducer, loadConfig } from "@hcm/shared";
import type { HcmDatabase, UUID } from "@hcm/db";
import { replayInventoryDlq } from "./dlq-replay.js";

const [organizationId, dlqEventId, actorUserId] = process.argv.slice(2);
if (!organizationId || !dlqEventId || !process.env.WORKER_DATABASE_URL) {
  throw new Error("Usage: pnpm --filter @hcm/outbound-engine replay-dlq <organizationId> <dlqEventId> [actorUserId]; WORKER_DATABASE_URL is required");
}
const config = loadConfig();
const pool = new Pool({ connectionString: process.env.WORKER_DATABASE_URL, max: 2, application_name: "hcm-dlq-replay" });
const db = new Kysely<HcmDatabase>({ dialect: new PostgresDialect({ pool }) });
const kafka = createKafkaClient({ serviceName: "hcm-dlq-replay", kafkaBrokers: config.kafkaBrokers });
const producer = createKafkaProducer(kafka);
try {
  await producer.connect();
  await replayInventoryDlq(db, producer, { organizationId: organizationId as UUID, dlqEventId: dlqEventId as UUID, ...(actorUserId ? { actorUserId: actorUserId as UUID } : {}) });
  process.stdout.write(`Replayed ${dlqEventId}\n`);
} finally {
  await producer.disconnect();
  await db.destroy();
}
