import { randomUUID } from "node:crypto";
import {
  CloudEventSchema,
  AriDeltaSchema,
  ReservationSchema,
  type CloudEvent,
  type HospitalityEventType,
} from "@hcm/hospitality-contracts";
import {
  createKafkaClient,
  createKafkaConsumer,
  createKafkaProducer,
  createLogger,
  loadConfig,
} from "@hcm/shared";
import type { Consumer, EachMessagePayload, Producer } from "kafkajs";

export interface NormalizedEvent {
  event: CloudEvent<unknown, HospitalityEventType>;
  topic: "reservation.canonical.events" | "inventory.delta.events";
}
export interface PartnerPayloadNormalizer {
  normalize(rawEvent: CloudEvent<unknown>): NormalizedEvent;
}

/** Generic normalized-envelope bridge. Partner field mapping belongs in adapter packages.
 * Input payload must contain `canonicalEvent`; no Booking.com/Expedia/Airbnb wire names are inferred here.
 */
export class CanonicalEnvelopeNormalizer implements PartnerPayloadNormalizer {
  normalize(rawEvent: CloudEvent<unknown>): NormalizedEvent {
    const data = rawEvent.data as { payload?: unknown } | null;
    const payload = data && typeof data === "object" ? data.payload : undefined;
    if (
      !payload ||
      typeof payload !== "object" ||
      !("canonicalEvent" in payload)
    ) {
      throw new NormalizationError(
        "CANONICAL_EVENT_MISSING",
        "Adapter did not supply a canonicalEvent envelope",
      );
    }
    const parsedEnvelope = CloudEventSchema.safeParse(
      (payload as { canonicalEvent: unknown }).canonicalEvent,
    );
    if (!parsedEnvelope.success) {
      throw new NormalizationError(
        "INVALID_CLOUDEVENT",
        parsedEnvelope.error.issues
          .map((issue) => `${issue.path.join(".")}:${issue.code}`)
          .join(","),
      );
    }
    const event = parsedEnvelope.data as CloudEvent<
      unknown,
      HospitalityEventType
    >;
    if (event.type === "com.hospitality.ari.delta") {
      const result = AriDeltaSchema.safeParse(event.data);
      if (!result.success)
        throw new NormalizationError(
          "INVALID_ARI_DELTA",
          result.error.issues
            .map((issue) => `${issue.path.join(".")}:${issue.code}`)
            .join(","),
        );
      return {
        event: { ...event, data: result.data },
        topic: "inventory.delta.events",
      };
    }
    if (
      [
        "com.hospitality.reservation.created",
        "com.hospitality.reservation.modified",
        "com.hospitality.reservation.cancelled",
        "com.hospitality.reservation.no_show",
      ].includes(event.type)
    ) {
      const result = ReservationSchema.safeParse(event.data);
      if (!result.success)
        throw new NormalizationError(
          "INVALID_RESERVATION",
          result.error.issues
            .map((issue) => `${issue.path.join(".")}:${issue.code}`)
            .join(","),
        );
      return {
        event: { ...event, data: result.data },
        topic: "reservation.canonical.events",
      };
    }
    throw new NormalizationError(
      "UNSUPPORTED_EVENT_TYPE",
      `Unsupported canonical type ${event.type}`,
    );
  }
}

export class NormalizationError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "NormalizationError";
  }
}

export async function processRawMessage(
  message: EachMessagePayload,
  producer: Producer,
  normalizer: PartnerPayloadNormalizer,
): Promise<void> {
  const value = message.message.value?.toString("utf8");
  let parsed: unknown;
  try {
    parsed = value ? JSON.parse(value) : undefined;
  } catch {
    parsed = undefined;
  }
  const rawResult = CloudEventSchema.safeParse(parsed);
  const key = message.message.key?.toString() ?? "unknown-property";
  if (!rawResult.success) {
    await publishDlq(producer, key, {
      sourceTopic: message.topic,
      sourcePartition: message.partition,
      sourceOffset: message.message.offset,
      errorCode: "INVALID_RAW_CLOUDEVENT",
      errorDetail: rawResult.error.issues
        .map((issue) => `${issue.path.join(".")}:${issue.code}`)
        .join(","),
      raw: parsed ?? { undecodable: true },
    });
    return;
  }
  let normalized: NormalizedEvent;
  try {
    normalized = normalizer.normalize(rawResult.data as CloudEvent<unknown>);
  } catch (error) {
    const normalizationError =
      error instanceof NormalizationError
        ? error
        : new NormalizationError(
            "NORMALIZER_FAILURE",
            "Unexpected normalizer failure",
          );
    await publishDlq(producer, key, {
      sourceTopic: message.topic,
      sourcePartition: message.partition,
      sourceOffset: message.message.offset,
      eventId: rawResult.data.id,
      eventType: rawResult.data.type,
      errorCode: normalizationError.code,
      errorDetail: normalizationError.message,
      raw: rawResult.data,
    });
    return;
  }
  // Kafka transport failures must fail this handler so the source offset remains uncommitted.
  await producer.send({
    topic: normalized.topic,
    acks: -1,
    messages: [
      {
        key,
        value: JSON.stringify(normalized.event),
        headers: { "content-type": "application/cloudevents+json" },
      },
    ],
  });
}

async function publishDlq(
  producer: Producer,
  key: string,
  detail: Record<string, unknown>,
): Promise<void> {
  const dlqEvent = {
    specversion: "1.0",
    id: randomUUID(),
    source: "normalizer",
    type: "com.hospitality.event.dead_lettered",
    time: new Date().toISOString(),
    datacontenttype: "application/json",
    data: { ...detail, deadLetteredAt: new Date().toISOString() },
  };
  await producer.send({
    topic: "dead.letter.queue",
    acks: -1,
    messages: [{ key, value: JSON.stringify(dlqEvent) }],
  });
}

export async function startNormalizer(
  consumer: Consumer,
  producer: Producer,
  normalizer: PartnerPayloadNormalizer,
): Promise<void> {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({
    topic: "webhook.raw.events",
    fromBeginning: false,
  });
  await consumer.run({
    autoCommit: false,
    eachMessage: async (payload) => {
      // Commit the source offset only after canonical output or DLQ is durably acknowledged by Kafka.
      await processRawMessage(payload, producer, normalizer);
      const nextOffset = (BigInt(payload.message.offset) + 1n).toString();
      await consumer.commitOffsets([
        {
          topic: payload.topic,
          partition: payload.partition,
          offset: nextOffset,
        },
      ]);
    },
  });
}

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({ ...config, serviceName: "normalizer" });
  const kafka = createKafkaClient({ ...config, serviceName: "normalizer" });
  const consumer = createKafkaConsumer(
    kafka,
    process.env.KAFKA_GROUP_ID ?? "hcm-normalizer-v1",
  );
  const producer = createKafkaProducer(kafka);
  logger.info("Starting raw webhook normalizer");
  await startNormalizer(consumer, producer, new CanonicalEnvelopeNormalizer());
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
