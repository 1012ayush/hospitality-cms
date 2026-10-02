import { SpanStatusCode, trace, type Attributes } from "@opentelemetry/api";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { NodeSDK } from "@opentelemetry/sdk-node";
import type { EnvironmentConfig } from "./config.js";

/** Start once, before importing HTTP/Kafka/PostgreSQL clients, so instrumentation can patch them. */
export async function startTelemetry(config: Pick<EnvironmentConfig, "otelEnabled" | "otelExporterOtlpEndpoint" | "serviceName">): Promise<NodeSDK | undefined> {
  if (!config.otelEnabled) return undefined;
  process.env.OTEL_SERVICE_NAME ??= config.serviceName;
  if (config.otelExporterOtlpEndpoint) {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ??= config.otelExporterOtlpEndpoint;
  }

  const sdk = new NodeSDK({
    traceExporter: new OTLPTraceExporter(),
    instrumentations: [getNodeAutoInstrumentations()],
  });
  await sdk.start();
  return sdk;
}

/** Create a business-operation span and preserve errors for the configured exporter. */
export async function withSpan<T>(name: string, attributes: Attributes, operation: () => Promise<T>): Promise<T> {
  const tracer = trace.getTracer("hcm-business-operations");
  return tracer.startActiveSpan(name, { attributes }, async (span) => {
    try { return await operation(); }
    catch (error) { span.recordException(error instanceof Error ? error : String(error)); span.setStatus({ code: SpanStatusCode.ERROR }); throw error; }
    finally { span.end(); }
  });
}
