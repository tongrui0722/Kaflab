export type Direction = "request" | "response" | "local";
export type EvStatus = "pending" | "success" | "error";

export interface BrokerRef { id?: number; host: string; port: number; }

export interface KafkaTraceEvent {
  traceId: string;
  timestamp: number;
  apiKey: number;
  apiName: string;
  apiVersion: number;
  correlationId: number;
  direction: Direction;
  step: string;
  stepIndex: number;
  status: EvStatus;
  broker?: BrokerRef;
  topic?: string;
  partition?: number;
  latencyMs?: number;
  errorCode?: number;
  errorName?: string;
  request?: unknown;
  response?: unknown;
  rawRequest?: string;
  rawResponse?: string;
}

export interface WsMessage { type: string; sessionId: string; event: KafkaTraceEvent; }

export interface Session {
  id: string; clientId: string; operation: string; topic: string;
  status: "running" | "success" | "error";
  startTime: number; endTime?: number; durationMs?: number;
  bootstrap: string; partition?: number; offset?: number; mode?: "live";
  error?: string;
}

export interface TraceStep {
  index: number; title: string; desc: string;
  latency?: string; status: EvStatus; event?: KafkaTraceEvent;
}

export interface RecentMsg {
  id: string; topic: string; partition: number; offset: number;
  latency: string; time: string; status: "SUCCESS" | "ERROR" | "RUNNING";
  traceId: string; events: KafkaTraceEvent[];
  key?: string; value?: string; size?: number;
}
