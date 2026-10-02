import { z } from "zod";

const EnvironmentSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  SERVICE_NAME: z.string().min(1).default("hotel-channel-manager-service"),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
  DATABASE_URL: z.string().url().optional(),
  REDIS_URL: z.string().url().default("redis://localhost:6379"),
  KAFKA_BROKERS: z.string().default("localhost:9092"),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().url().optional(),
  OTEL_ENABLED: z.enum(["true", "false"]).default("true").transform((value) => value === "true"),
}).passthrough();

type ParsedEnvironment = z.infer<typeof EnvironmentSchema>;
export type EnvironmentConfig = {
  nodeEnv: ParsedEnvironment["NODE_ENV"];
  serviceName: string;
  logLevel: ParsedEnvironment["LOG_LEVEL"];
  databaseUrl: string | undefined;
  redisUrl: string;
  kafkaBrokers: string[];
  otelExporterOtlpEndpoint: string | undefined;
  otelEnabled: boolean;
};

/** Parse and validate environment variables at service startup, before opening clients. */
export function loadConfig(source: NodeJS.ProcessEnv = process.env): EnvironmentConfig {
  const parsed = EnvironmentSchema.parse(source);
  const kafkaBrokers = parsed.KAFKA_BROKERS.split(",").map((broker) => broker.trim()).filter(Boolean);
  if (kafkaBrokers.length === 0) {
    throw new Error("KAFKA_BROKERS must contain at least one host:port value");
  }
  return {
    nodeEnv: parsed.NODE_ENV,
    serviceName: parsed.SERVICE_NAME,
    logLevel: parsed.LOG_LEVEL,
    databaseUrl: parsed.DATABASE_URL,
    redisUrl: parsed.REDIS_URL,
    kafkaBrokers,
    otelExporterOtlpEndpoint: parsed.OTEL_EXPORTER_OTLP_ENDPOINT,
    otelEnabled: parsed.OTEL_ENABLED,
  };
}
