import { useCallback, useEffect, useRef, useState } from "react";
import type { KafkaTraceEvent, Session, WsMessage } from "../types";
import { authHeaders } from "../auth";

const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";
const WS = (import.meta.env.VITE_WS_BASE as string) || "ws://localhost:8000";

export async function apiGet<T>(path: string): Promise<T> {
  const r = await fetch(`${API}${path}`);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
  const r = await fetch(url, {
    method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}

export interface SecurityForm {
  securityProtocol: "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL";
  saslMechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
  saslUsername: string;
  saslPassword: string;
}

export const DEFAULT_SECURITY: SecurityForm = {
  securityProtocol: "PLAINTEXT",
  saslMechanism: "PLAIN",
  saslUsername: "",
  saslPassword: "",
};

export interface ProduceForm extends SecurityForm {
  bootstrap: string; topic: string; partition: string;
  key: string; value: string; acks: string; retries: number;
  requestTimeoutMs: number; deliveryTimeoutMs: number;
  messages?: { key: string; value: string; keyHex?: string; valueHex?: string }[];
  keyHex?: string; valueHex?: string;
  headers?: { k: string; v: string }[];
}

export const DEFAULT_PRODUCE: ProduceForm = {
  ...DEFAULT_SECURITY,
  bootstrap: "192.168.127.129:19092", topic: "test-topic", partition: "auto",
  key: "", value: "", acks: "all", retries: 3,
  requestTimeoutMs: 10000, deliveryTimeoutMs: 30000,
};

export interface ClusterStatus {
  connected: boolean;
  brokers: { id?: number; host: string; port: number }[];
  brokerCount: number;
  topicCount: number;
  partitionCount: number;
  topic: string;
  partitions: { partition: number; leader: number; replicas: number[]; isr: number[] }[];
  events: KafkaTraceEvent[];
  error?: string;
}

export function clusterQuery(form: Pick<ProduceForm, "bootstrap" | "securityProtocol" | "saslMechanism" | "saslUsername" | "saslPassword">, topic: string) {
  const q = new URLSearchParams({
    bootstrap: form.bootstrap, topic,
    securityProtocol: form.securityProtocol, saslMechanism: form.saslMechanism,
    saslUsername: form.saslUsername, saslPassword: form.saslPassword,
  });
  return `/api/cluster/status?${q.toString()}`;
}

export function useProducerSession() {
  const [session, setSession] = useState<Session | null>(null);
  const [events, setEvents] = useState<KafkaTraceEvent[]>([]);
  const [running, setRunning] = useState(false);
  const [sendError, setSendError] = useState<string | null>(null);
  const [wsDown, setWsDown] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  const connectWs = useCallback((sid: string) => {
    wsRef.current?.close();
    const ws = new WebSocket(`${WS}/ws/session/${sid}`);
    wsRef.current = ws;
    ws.onmessage = (m) => {
      try {
        const msg = JSON.parse(m.data) as WsMessage;
        if (msg.type === "ping" || !msg.event) return;
        setEvents((prev) => [...prev, msg.event]);
        if (msg.type === "trace.completed" || msg.type === "trace.failed") {
          setRunning(false);
          fetch(`${API}/api/sessions/${sid}`).then((r) => r.json()).then((s: Session) => {
            setSession(s);
            if (s.status === "error") setSendError(s.error ?? "unknown error");
          }).catch(() => undefined);
        }
      } catch { /* ignore */ }
    };
    ws.onerror = () => setWsDown(true);
    ws.onopen = () => setWsDown(false);
  }, []);

  useEffect(() => () => wsRef.current?.close(), []);

  const send = useCallback(async (form: ProduceForm) => {
    setRunning(true);
    setEvents([]);
    setSendError(null);
    // POST 失败 = 后端不可达，直接报错
    const res = await postJson<{ sessionId: string }>(`${API}/api/producer/send`, {
      ...form, clientId: "kafka-sim-01",
    });
    setSession({
      id: res.sessionId, clientId: "kafka-sim-01", operation: "Producer",
      topic: form.topic, status: "running", startTime: Date.now(),
      bootstrap: form.bootstrap, mode: "live",
    });
    connectWs(res.sessionId);
  }, [connectWs]);

  return { session, events, running, sendError, wsDown, send };
}

export function useClusterStatus(form: ProduceForm, topic: string) {
  const [data, setData] = useState<ClusterStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const d = await apiGet<ClusterStatus>(clusterQuery(form, topic));
      setData(d);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [form.bootstrap, form.securityProtocol, form.saslMechanism, form.saslUsername, form.saslPassword, topic]);
  return { data, loading, error, refresh };
}
