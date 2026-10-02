import { Kafka, logLevel, type Consumer, type KafkaConfig, type Producer } from "kafkajs";
import type { EnvironmentConfig } from "./config.js";

export function createKafkaClient(
  config: Pick<EnvironmentConfig, "serviceName" | "kafkaBrokers">,
  overrides: Partial<KafkaConfig> = {},
): Kafka {
  const isCloud = !!process.env.KAFKA_SASL_USERNAME;
  const caCert = process.env.KAFKA_CA_CERT?.replace(/\\n/g, '\n');

  // Base configuration that works everywhere (local Docker)
  const kafkaConfig: KafkaConfig = {
    clientId: config.serviceName,
    brokers: config.kafkaBrokers,
    logLevel: logLevel.NOTHING,
    retry: { initialRetryTime: 100, retries: 8 },
    ...overrides,
  };

  // If credentials exist in the environment (Render/Production), dynamically inject SSL/SASL
  if (isCloud) {
    kafkaConfig.ssl = {
      rejectUnauthorized: false,
      ca: caCert ? [caCert] : undefined,
    };
    kafkaConfig.sasl = {
      mechanism: 'plain' as const, // The "as const" fixes the red line!
      username: process.env.KAFKA_SASL_USERNAME!,
      password: process.env.KAFKA_SASL_PASSWORD!,
    };
  }

  return new Kafka(kafkaConfig);
}

export function createKafkaProducer(kafka: Kafka): Producer {
  return kafka.producer({ idempotent: true, allowAutoTopicCreation: false, maxInFlightRequests: 5 });
}

export function createKafkaConsumer(kafka: Kafka, groupId: string): Consumer {
  if (!groupId.trim()) throw new TypeError("Kafka consumer groupId is required");
  return kafka.consumer({ groupId, allowAutoTopicCreation: false });
}