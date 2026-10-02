import { Redis, type RedisOptions } from "ioredis";
import type { EnvironmentConfig } from "./config.js";

export function createRedisClient(
  config: Pick<EnvironmentConfig, "redisUrl">,
  overrides: RedisOptions = {}
): Redis {
  const isCloud = config.redisUrl.startsWith("rediss://");

  const redisOptions: RedisOptions = {
    enableReadyCheck: true,
    maxRetriesPerRequest: 3,
    retryStrategy: (attempt: number) => Math.min(attempt * 100, 2_000),
    ...overrides,
  };

  // If the URL is secure (Upstash/Production), inject TLS configuration
  if (isCloud) {
    redisOptions.tls = {
      rejectUnauthorized: false, // Prevents certificate verification errors in cloud environments
    };
  }

  return new Redis(config.redisUrl, redisOptions);
}