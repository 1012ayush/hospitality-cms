import { CloudEventSchema, AriDeltaSchema, type AriDelta, type CloudEvent } from "@hcm/hospitality-contracts";
import { createHash } from "node:crypto";
import type { Redis } from "ioredis";
import type { Consumer, EachBatchPayload, Producer } from "kafkajs";
import { BaseChannelAdapter, type AdapterError, type AdapterResult, type CanonicalRateUpdate } from "@hcm/adapter-base";
import { withSpan } from "@hcm/shared";
import { sql, type Kysely } from "kysely";
import type { HcmDatabase } from "@hcm/db";

type Priority = "reservation" | "bulk";
export interface ChannelTarget {
  connectionId: string;
  adapter: BaseChannelAdapter;
  bucketCapacity: number;
  refillPerSecond: number;
  circuitThreshold?: number;
  circuitCooldownMs?: number;
}
export interface ChannelTargetProvider {
  listTargets(input: { organizationId: string; propertyId: string; roomTypeId: string }): Promise<ChannelTarget[]>;
}
export interface OutboundMetrics {
  attempt(labels: { channel: string; result: string }): void;
  duration(seconds: number, labels: { channel: string }): void;
  queueAge(seconds: number): void;
  dlq(reason: string): void;
}
const noopMetrics: OutboundMetrics = { attempt() {}, duration() {}, queueAge() {}, dlq() {} };

const TOKEN_BUCKET_LUA = `
local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local state=redis.call('HMGET',KEYS[1],'tokens','at'); local tokens=tonumber(state[1]); local at=tonumber(state[2])
local capacity=tonumber(ARGV[1]); local refill=tonumber(ARGV[2]); local ttl=tonumber(ARGV[3])
if tokens==nil then tokens=capacity; at=now end
tokens=math.min(capacity,tokens+math.max(0,now-at)*refill/1000)
if tokens>=1 then tokens=tokens-1; redis.call('HSET',KEYS[1],'tokens',tokens,'at',now); redis.call('PEXPIRE',KEYS[1],ttl); return {1,0} end
local wait=math.ceil((1-tokens)*1000/refill); redis.call('HSET',KEYS[1],'tokens',tokens,'at',now); redis.call('PEXPIRE',KEYS[1],ttl); return {0,wait}
`;
const CIRCUIT_ENTER_LUA = `
local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local state=redis.call('HGET',KEYS[1],'state') or 'closed'; local untilAt=tonumber(redis.call('HGET',KEYS[1],'until') or '0')
local probe=tonumber(redis.call('HGET',KEYS[1],'probe') or '0')
if state=='closed' then return {1,0} end
if state=='open' and now<untilAt then return {0,untilAt-now} end
if state=='half_open' and now<probe then return {0,probe-now} end
redis.call('HSET',KEYS[1],'state','half_open','probe',now+tonumber(ARGV[1])); redis.call('PEXPIRE',KEYS[1],tonumber(ARGV[1])*4); return {1,0}
`;
const CIRCUIT_SUCCESS_LUA = `redis.call('DEL',KEYS[1]); return 1`;
const CIRCUIT_FAILURE_LUA = `
local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local state=redis.call('HGET',KEYS[1],'state') or 'closed'; local failures=redis.call('HINCRBY',KEYS[1],'failures',1)
local threshold=tonumber(ARGV[1]); local cooldown=tonumber(ARGV[2])
if state=='half_open' or failures>=threshold then redis.call('HSET',KEYS[1],'state','open','until',now+cooldown,'probe',0) end
redis.call('PEXPIRE',KEYS[1],cooldown*4); return failures
`;

/** Atomic, shared per-connection rate limit. Partner-specific values must come from contracted docs/config. */
export async function acquireChannelToken(redis: Redis, target: ChannelTarget): Promise<number> {
  if (!(target.bucketCapacity > 0) || !(target.refillPerSecond > 0)) throw new Error("Token bucket capacity and refill must be positive");
  const ttlMs = Math.ceil((target.bucketCapacity / target.refillPerSecond) * 2_000);
  const result = (await redis.eval(TOKEN_BUCKET_LUA, 1, `hcm:outbound:bucket:${target.connectionId}`,
    target.bucketCapacity, target.refillPerSecond, ttlMs)) as [number, number];
  return Number(result[0]) === 1 ? 0 : Number(result[1]);
}
export async function enterChannelCircuit(redis: Redis, target: ChannelTarget): Promise<number> {
  const result = (await redis.eval(CIRCUIT_ENTER_LUA, 1, `hcm:outbound:circuit:${target.connectionId}`, 10_000)) as [number, number];
  return Number(result[0]) === 1 ? 0 : Number(result[1]);
}
async function circuitSuccess(redis: Redis, target: ChannelTarget): Promise<void> {
  await redis.eval(CIRCUIT_SUCCESS_LUA, 1, `hcm:outbound:circuit:${target.connectionId}`);
}
async function circuitFailure(redis: Redis, target: ChannelTarget): Promise<void> {
  await redis.eval(CIRCUIT_FAILURE_LUA, 1, `hcm:outbound:circuit:${target.connectionId}`, target.circuitThreshold ?? 5, target.circuitCooldownMs ?? 30_000);
}

interface QueuedWork<T> { priority: Priority; sequence: number; execute: () => Promise<T>; resolve: (value: T) => void; reject: (reason: unknown) => void; }
/** Reservation-driven inventory has precedence over bulk rate updates within an engine instance. */
export class PriorityDeliveryQueue {
  private queue: QueuedWork<unknown>[] = [];
  private draining = false;
  private sequence = 0;
  enqueue<T>(priority: Priority, execute: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.queue.push({ priority, sequence: this.sequence++, execute, resolve, reject } as QueuedWork<unknown>);
      this.queue.sort((a, b) => (a.priority === b.priority ? 0 : a.priority === "reservation" ? -1 : 1) || a.sequence - b.sequence);
      void this.drain();
    });
  }
  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.queue.length) {
        const item = this.queue.shift()!;
        try { item.resolve(await item.execute()); } catch (error) { item.reject(error); }
      }
    } finally { this.draining = false; }
  }
}

/** Coalesce same room/date deltas in a Kafka batch; later canonical versions win. */
export function coalesceAriDeltas(events: CloudEvent<AriDelta>[]): CloudEvent<AriDelta>[] {
  const latest = new Map<string, CloudEvent<AriDelta>>();
  for (const event of events) {
    const data = AriDeltaSchema.parse(event.data);
    for (let date = data.stay.startDate; date < data.stay.endDate; date = addDays(date, 1)) {
      const key = `${data.tenantId}|${data.propertyId}|${data.roomTypeId}|${data.ratePlanId ?? "-"}|${date}`;
      const existing = latest.get(key);
      // Kafka offset order is authoritative within the property partition; partner versions stay opaque.
      const next = { ...event, id: `${event.id}:${date}`, data: { ...data, stay: { ...data.stay, startDate: date, endDate: addDays(date, 1) } } };
      latest.set(key, next);
    }
  }
  return [...latest.values()];
}

export interface OutboundDeadLetterStore { persist(input: { organizationId: string; propertyId: string; event: CloudEvent<AriDelta>; error: AdapterError; operation: string }): Promise<void>; }

export interface OutboundEngineOptions {
  debounceMs?: number;
  maxAttempts?: number;
  metrics?: OutboundMetrics;
  sleep?: (milliseconds: number) => Promise<void>;
}
export class OutboundDispatcher {
  private readonly queue = new PriorityDeliveryQueue();
  private readonly metrics: OutboundMetrics;
  private readonly sleep: (milliseconds: number) => Promise<void>;
  constructor(private readonly redis: Redis, private readonly targets: ChannelTargetProvider, private readonly dlq: Producer,
    private readonly dlqStore: OutboundDeadLetterStore, private readonly options: OutboundEngineOptions = {}) {
    this.metrics = options.metrics ?? noopMetrics;
    this.sleep = options.sleep ?? ((milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)));
  }

  dispatch(event: CloudEvent<AriDelta>, priority: Priority = "reservation"): Promise<void> {
    return this.queue.enqueue(priority, () => this.deliverDelta(event));
  }

  /** Bulk rate calls share the same queue and are preempted by queued reservation availability. */
  dispatchBulkRate(target: ChannelTarget, update: CanonicalRateUpdate): Promise<void> {
    return this.queue.enqueue("bulk", async () => { await this.deliverWithRetry(target, "rates", () => target.adapter.pushRates(update)); });
  }

  private async deliverDelta(event: CloudEvent<AriDelta>): Promise<void> {
    const data = AriDeltaSchema.parse(event.data);
    const started = Date.now();
    const targets = await this.targets.listTargets({ organizationId: data.tenantId, propertyId: data.propertyId, roomTypeId: data.roomTypeId });
    for (const target of targets) {
      const dedupeKey = `hcm:outbound:sent:${event.id}:${target.connectionId}`;
      const alreadySent = await this.redis.get(dedupeKey);
      if (alreadySent) continue;
      const oneNight = { ...data, closed: data.closed ?? data.restrictions?.closedToArrival ?? data.restrictions?.closedToDeparture ?? false };
      const result = await this.deliverWithRetry(target, "availability", () => target.adapter.pushAvailability(oneNight), event);
      if (result) {
        // Redis is only a cache; a lost write may yield duplicate idempotent availability updates on Kafka redelivery.
        await this.redis.set(dedupeKey, "1", "EX", 604_800, "NX");
      }
    }
    this.metrics.duration((Date.now() - started) / 1_000, { channel: targets[0]?.adapter.connection.channel ?? "none" });
  }

  private async deliverWithRetry<T>(target: ChannelTarget, operation: string, send: () => Promise<AdapterResult<T>>, originalEvent?: CloudEvent<AriDelta>): Promise<boolean> {
    const maxAttempts = this.options.maxAttempts ?? 8;
    let attempts = 0;
    while (attempts < maxAttempts) {
      const circuitWait = await enterChannelCircuit(this.redis, target);
      if (circuitWait > 0) { await this.sleep(circuitWait); continue; }
      const rateWait = await acquireChannelToken(this.redis, target);
      if (rateWait > 0) { await this.sleep(rateWait); continue; }
      attempts++;
      const started = Date.now();
      let result: AdapterResult<T>;
      try { result = await withSpan("ota.outbound.attempt", { "hcm.channel_connection_id": target.connectionId, "hcm.operation": operation }, send); }
      catch (error) { result = { ok: false, error: target.adapter.mapError(error) }; }
      const channel = target.adapter.connection.channel;
      if (result.ok) {
        await circuitSuccess(this.redis, target);
        this.metrics.attempt({ channel, result: "success" });
        this.metrics.duration((Date.now() - started) / 1_000, { channel });
        return true;
      }
      const failure: AdapterError = result.error;
      this.metrics.attempt({ channel, result: failure.classification });
      if (failure.classification === "auth" || failure.classification === "permanent") {
        await this.publishFailure(target, operation, failure, originalEvent);
        return false;
      }
      await circuitFailure(this.redis, target);
      if (attempts >= maxAttempts) {
        await this.publishFailure(target, operation, failure, originalEvent);
        return false;
      }
      const backoff = failure.retryAfterMs ?? Math.min(30_000, 250 * 2 ** (attempts - 1)) * (0.5 + Math.random());
      await this.sleep(backoff);
    }
    return false;
  }

  private async publishFailure(target: ChannelTarget, operation: string, error: AdapterError, originalEvent?: CloudEvent<AriDelta>): Promise<void> {
    if (originalEvent) await this.dlqStore.persist({ organizationId: target.adapter.connection.organizationId, propertyId: target.adapter.connection.propertyId, event: originalEvent, error, operation });
    const event = { specversion: "1.0", id: createHash("sha256").update(`${target.connectionId}:${operation}:${Date.now()}`).digest("hex"),
      source: "outbound-engine", type: "com.hospitality.inventory.propagation.failed", time: new Date().toISOString(),
      data: { organizationId: target.adapter.connection.organizationId, propertyId: target.adapter.connection.propertyId,
        channelConnectionId: target.connectionId, operation, classification: error.classification, code: error.code,
        httpStatus: error.httpStatus, attempts: this.options.maxAttempts ?? 8 }, };
    await this.dlq.send({ topic: "dead.letter.queue", acks: -1, messages: [{ key: target.adapter.connection.propertyId, value: JSON.stringify(event) }] });
    this.metrics.dlq(error.code);
  }
}

/** Kafka batch means rapid updates for one property/date can be coalesced without committing before delivery. */
export async function processOutboundBatch(payload: EachBatchPayload, dispatcher: OutboundDispatcher,
  producer: Producer, options: OutboundEngineOptions = {}): Promise<void> {
  const decoded: CloudEvent<AriDelta>[] = [];
  for (const message of payload.batch.messages) {
    let parsed: unknown;
    try { parsed = JSON.parse(message.value?.toString("utf8") ?? ""); } catch { parsed = undefined; }
    const envelope = CloudEventSchema.safeParse(parsed);
    if (!envelope.success || envelope.data.type !== "com.hospitality.ari.delta") {
      await producer.send({ topic: "dead.letter.queue", acks: -1, messages: [{ key: message.key?.toString() ?? payload.batch.topic,
        value: JSON.stringify({ specversion: "1.0", id: `${payload.batch.topic}:${payload.batch.partition}:${message.offset}`,
          source: "outbound-engine", type: "com.hospitality.event.dead_lettered", time: new Date().toISOString(),
          data: { sourceTopic: payload.batch.topic, sourcePartition: payload.batch.partition, sourceOffset: message.offset,
            reason: "INVALID_INVENTORY_DELTA", schemaIssues: envelope.success ? [] : envelope.error.issues.map((issue) => `${issue.path.join(".")}:${issue.code}`), }, }) }] });
      continue;
    }
    const delta = AriDeltaSchema.safeParse(envelope.data.data);
    if (!delta.success) {
      await producer.send({ topic: "dead.letter.queue", acks: -1, messages: [{ key: message.key?.toString() ?? payload.batch.topic,
        value: JSON.stringify({ specversion: "1.0", id: envelope.data.id, source: "outbound-engine",
          type: "com.hospitality.event.dead_lettered", time: new Date().toISOString(),
          data: { sourceTopic: payload.batch.topic, sourcePartition: payload.batch.partition, sourceOffset: message.offset,
            reason: "INVALID_ARI_DELTA", schemaIssues: delta.error.issues.map((issue) => `${issue.path.join(".")}:${issue.code}`), }, }) }] });
      continue;
    }
    decoded.push({ ...envelope.data, data: delta.data } as CloudEvent<AriDelta>);
  }
  const coalesced = coalesceAriDeltas(decoded);
  const debounce = options.debounceMs ?? 100;
  if (coalesced.length) await new Promise((resolve) => setTimeout(resolve, debounce));
  // Reservation-driven availability is dispatched on the priority lane before separately enqueued bulk rates.
  for (const event of coalesced) await dispatcher.dispatch(event, "reservation");
  const last = payload.batch.messages.at(-1);
  if (last) await payload.heartbeat();
}

export async function startOutboundEngine(consumer: Consumer, dispatcher: OutboundDispatcher, producer: Producer): Promise<void> {
  await Promise.all([consumer.connect(), producer.connect()]);
  await consumer.subscribe({ topic: "inventory.delta.events", fromBeginning: false });
  await consumer.run({ autoCommit: false, eachBatch: async (payload) => {
    await processOutboundBatch(payload, dispatcher, producer);
    const last = payload.batch.messages.at(-1);
    if (last) await consumer.commitOffsets([{ topic: payload.batch.topic, partition: payload.batch.partition, offset: (BigInt(last.offset) + 1n).toString() }]);
  } });
}

function addDays(date: string, amount: number): string { const result = new Date(`${date}T00:00:00.000Z`); result.setUTCDate(result.getUTCDate() + amount); return result.toISOString().slice(0, 10); }
