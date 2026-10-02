import pino, { type Logger, type LoggerOptions } from "pino";
import type { EnvironmentConfig } from "./config.js";

export function createLogger(config: Pick<EnvironmentConfig, "serviceName" | "logLevel" | "nodeEnv">): Logger {
  const options: LoggerOptions = {
    name: config.serviceName,
    level: config.logLevel,
    redact: {
      paths: [
        "req.headers.authorization",
        "headers.authorization",
        "*.access_token",
        "*.refresh_token",
        "*.client_secret",
        "*.credential_secret_ref",
        "*.tokenized_payment_reference",
        "*.cardNumber",
        "*.cvv",
        "*.cvc",
      ],
      censor: "[REDACTED]",
    },
  };
  return pino(options);
}
