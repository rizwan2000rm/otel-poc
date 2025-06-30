import { WebTracerProvider } from "@opentelemetry/sdk-trace-web";
import { ZoneContextManager } from "@opentelemetry/context-zone";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { UserInteractionInstrumentation } from "@opentelemetry/instrumentation-user-interaction";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { trace } from "@opentelemetry/api";

const otlpExporter = new OTLPTraceExporter({
  url: "http://localhost:4318/v1/traces",
});

const provider = new WebTracerProvider({
  spanProcessors: [new BatchSpanProcessor(otlpExporter)],
});

provider.register({
  contextManager: new ZoneContextManager(),
});

registerInstrumentations({
  instrumentations: [
    new UserInteractionInstrumentation({
      eventNames: ["click"],
    }),
  ],
});

export const tracer = trace.getTracer("tonbo-frontend");
