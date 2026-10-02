import Fastify, { type FastifyInstance } from "fastify";
import { createHmac, createHash, timingSafeEqual } from "node:crypto";
import { CloudEventSchema } from "@hcm/hospitality-contracts";
import {
  createKafkaClient,
  createKafkaProducer,
  createLogger,
  createRedisClient,
  loadConfig,
} from "@hcm/shared";
import type { Producer } from "kafkajs";
import type { Redis } from "ioredis";

const MAX_BODY_BYTES = Number(process.env.WEBHOOK_MAX_BODY_BYTES ?? 1_048_576);
const REPLAY_WINDOW_SECONDS = Number(
  process.env.WEBHOOK_REPLAY_WINDOW_SECONDS ?? 300,
);

export interface ResolvedWebhookIdentity {
  secret: string;
  organizationId: string;
}
export interface WebhookSecretResolver {
  /** Must validate connection/property/tenant binding without querying PostgreSQL on this request path. */
  resolveSecret(
    connectionId: string,
    propertyId: string,
  ): Promise<ResolvedWebhookIdentity | undefined>;
}
export interface PciProxy {
  /** Must remove/tokenize payment fields before any broker publish. Production uses an approved PCI proxy. */
  tokenizeAndStrip(raw: Buffer): Promise<Buffer>;
}
export interface GatewayDependencies {
  producer: Producer;
  redis: Redis;
  secrets: WebhookSecretResolver;
  pciProxy: PciProxy;
  logger?: ReturnType<typeof createLogger>;
}

/** Local-only demonstration: static secret map; never use in production. */
export class MockWebhookSecretResolver implements WebhookSecretResolver {
  constructor(
    private readonly secrets: ReadonlyMap<string, ResolvedWebhookIdentity>,
  ) {}
  async resolveSecret(
    connectionId: string,
    propertyId: string,
  ): Promise<ResolvedWebhookIdentity | undefined> {
    return this.secrets.get(`${connectionId}:${propertyId}`);
  }
}

/** Local-only PCI hook. Scrubs sensitive-shaped keys and replaces PAN-like values with opaque test tokens.
 * It is not a PCI tokenization service; production must inject VGS/Spreedly-style approved proxy.
 */
export class MockPciProxy implements PciProxy {
  async tokenizeAndStrip(raw: Buffer): Promise<Buffer> {
    const value: unknown = JSON.parse(raw.toString("utf8"));
    const walk = (node: unknown): unknown => {
      if (Array.isArray(node)) return node.map(walk);
      if (node === null || typeof node !== "object") return node;
      const out: Record<string, unknown> = {};
      for (const [key, child] of Object.entries(node)) {
        if (
          /^(payment|payment_.*|card|credit_?card|virtual_?card|virtual_?credit_?card|vcc)$/i.test(
            key,
          )
        ) {
          out.payment_reference = "mock-tokenized-reference";
          continue;
        }
        if (
          /(^|_)(pan|card_?number|credit_?card_?number|virtual_?card_?number|vcc_?number|cvv|cvc|security_?code|track_?data)(_|$)/i.test(
            key,
          )
        )
          continue;
        out[key] = walk(child);
      }
      return out;
    };
    return Buffer.from(JSON.stringify(walk(value)), "utf8");
  }
}

function secureHexEqual(expectedHex: string, supplied: string): boolean {
  if (!/^[a-f0-9]{64}$/i.test(supplied)) return false;
  const expected = Buffer.from(expectedHex, "hex");
  const actual = Buffer.from(supplied, "hex");
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}

export function buildGateway(deps: GatewayDependencies): FastifyInstance {
  const logger = deps.logger;
  const app = Fastify({ bodyLimit: MAX_BODY_BYTES, logger: false });
  app.removeContentTypeParser("application/json");
  app.addContentTypeParser(
    "application/json",
    { parseAs: "buffer", bodyLimit: MAX_BODY_BYTES },
    (_req, body, done) => done(null, body),
  );

  app.post<{ Params: { propertyId: string; connectionId: string } }>(
    "/v1/webhooks/:propertyId/:connectionId",
    async (request, reply) => {
      const raw = request.body as Buffer;
      if (!Buffer.isBuffer(raw) || raw.length > MAX_BODY_BYTES)
        return reply.code(413).send({ error: "payload_too_large" });

      // Header names and signature format are this gateway's normalized integration contract.
      // ASSUMPTION – verify against partner docs for each adapter; do not assume partner header compatibility.
      const timestampText = request.headers["x-hcm-timestamp"];
      const signatureText = request.headers["x-hcm-signature"];
      if (
        typeof timestampText !== "string" ||
        typeof signatureText !== "string" ||
        !/^\d{10}$/.test(timestampText)
      ) {
        return reply.code(401).send({ error: "invalid_signature_headers" });
      }
      const timestamp = Number(timestampText);
      const now = Math.floor(Date.now() / 1000);
      if (Math.abs(now - timestamp) > REPLAY_WINDOW_SECONDS)
        return reply.code(401).send({ error: "timestamp_outside_window" });

      const identity = await deps.secrets.resolveSecret(
        request.params.connectionId,
        request.params.propertyId,
      );
      if (!identity)
        return reply.code(401).send({ error: "unknown_connection" });
      // Our normalized signing input binds the timestamp and body; partner-specific signing formats vary.
      const expected = createHmac("sha256", identity.secret)
        .update(timestampText)
        .update(".")
        .update(raw)
        .digest("hex");
      if (!secureHexEqual(expected, signatureText))
        return reply.code(401).send({ error: "invalid_signature" });

      const bodyDigest = createHash("sha256").update(raw).digest("hex");
      const replayKey = `hcm:webhook:replay:${request.params.connectionId}:${timestampText}:${bodyDigest}`;
      try {
        const accepted = await deps.redis.set(
          replayKey,
          "1",
          "EX",
          REPLAY_WINDOW_SECONDS,
          "NX",
        );
        if (accepted !== "OK")
          return reply.code(409).send({ error: "replay_detected" });
      } catch (error) {
        logger?.error(
          { err: error },
          "Replay protection unavailable; refusing webhook",
        );
        return reply.code(503).send({ error: "replay_protection_unavailable" });
      }

      let sanitized: Buffer;
      try {
        sanitized = await deps.pciProxy.tokenizeAndStrip(raw);
      } catch (error) {
        await deps.redis.del(replayKey).catch(() => undefined);
        logger?.error({ err: error }, "PCI proxy rejected webhook payload");
        return reply.code(422).send({ error: "payload_redaction_failed" });
      }

      let data: unknown;
      try {
        data = JSON.parse(sanitized.toString("utf8"));
      } catch {
        await deps.redis.del(replayKey).catch(() => undefined);
        return reply.code(400).send({ error: "invalid_json" });
      }

      const event = {
        specversion: "1.0" as const,
        id: createHash("sha256")
          .update(
            `${request.params.connectionId}:${timestampText}:${bodyDigest}`,
          )
          .digest("hex"),
        source: `channel-connection/${request.params.connectionId}`,
        type: "com.hospitality.webhook.received" as const,
        time: new Date().toISOString(),
        datacontenttype: "application/json",
        data: {
          organization_id: identity.organizationId,
          property_id: request.params.propertyId,
          channel_connection_id: request.params.connectionId,
          received_at: new Date().toISOString(),
          body_sha256: bodyDigest,
          payload: data,
        },
      };
      const valid = CloudEventSchema.safeParse(event);
      if (!valid.success) {
        await deps.redis.del(replayKey).catch(() => undefined);
        return reply.code(400).send({ error: "invalid_event_envelope" });
      }

      try {
        await deps.producer.send({
          topic: "webhook.raw.events",
          acks: -1,
          messages: [
            {
              key: request.params.propertyId,
              value: JSON.stringify(valid.data),
              headers: { "content-type": "application/cloudevents+json" },
            },
          ],
        });
      } catch (error) {
        await deps.redis.del(replayKey).catch(() => undefined);
        logger?.error({ err: error }, "Kafka publish failed");
        return reply.code(503).send({ error: "ingestion_unavailable" });
      }
      return reply.code(202).send({ accepted: true, eventId: event.id });
    },
  );

  return app;
}

// OUT OF SCOPE (Part 4A): partner-specific header extraction and secret registry implementation; confirm mappings against partner documentation.

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config);
  const kafka = createKafkaClient({
    ...config,
    serviceName: "ingestion-gateway",
  });
  const producer = createKafkaProducer(kafka);
  const redis = createRedisClient(config);
  const secret = process.env.DEV_WEBHOOK_HMAC_SECRET;
  if (config.nodeEnv === "production")
    throw new Error(
      "Production startup requires injected Secrets Manager resolver and PCI proxy",
    );
  const identity =
    secret && process.env.DEV_ORGANIZATION_ID
      ? { secret, organizationId: process.env.DEV_ORGANIZATION_ID }
      : undefined;
  const secrets = new MockWebhookSecretResolver(
    new Map(
      identity
        ? [
            [
              `${process.env.DEV_CONNECTION_ID}:${process.env.DEV_PROPERTY_ID}`,
              identity,
            ],
          ]
        : [],
    ),
  );
  const app = buildGateway({
    producer,
    redis,
    secrets,
    pciProxy: new MockPciProxy(),
    logger,
  });
  await Promise.all([producer.connect(), redis.ping()]);
  await app.listen({ host: "0.0.0.0", port: Number(process.env.PORT ?? 8080) });
  logger.info(
    { port: Number(process.env.PORT ?? 8080) },
    "Ingestion gateway listening",
  );
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
