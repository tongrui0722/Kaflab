import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import type { KafkaTraceEvent } from "../types";
import { KV } from "./bits";

function redact(value: unknown): unknown {
  if (value == null || typeof value !== "object") {
    if (typeof value === "string" && /^(scram|admin|kafka|.+123)/i.test(value) && value.length >= 4) return value;
    return value;
  }
  if (Array.isArray(value)) return value.map(redact);
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (/password|passwd|pwd|secret|token/i.test(k)) out[k] = "••••••";
    else out[k] = redact(v);
  }
  return out;
}

function Tree({ data, depth = 0 }: { data: unknown; depth?: number }) {
  if (data == null) return <span className="text-slate-500">null</span>;
  if (typeof data !== "object") return <span className="mono text-slate-700">{String(data)}</span>;
  if (Array.isArray(data))
    return (
      <div className="ml-3 border-l border-[var(--border-soft)] pl-2">
        {data.map((v, i) => (
          <div key={i} className="py-[1px] text-[12px]">
            <span className="mono text-slate-500">[{i}] </span><Tree data={v} depth={depth + 1} />
          </div>
        ))}
      </div>
    );
  return (
    <div className="ml-3 border-l border-[var(--border-soft)] pl-2">
      {Object.entries(data as Record<string, unknown>).map(([k, v]) => (
        <details key={k} open={depth < 2} className="py-[1px] text-[12px]">
          <summary className="cursor-pointer text-slate-400 hover:text-slate-700">▸ {k}</summary>
          <Tree data={v} depth={depth + 1} />
        </details>
      ))}
    </div>
  );
}

const API_LABEL: Record<number, string> = {
  0: "Produce", 1: "Fetch", 2: "ListOffsets", 3: "Metadata", 9: "OffsetFetch",
  10: "FindCoordinator", 11: "JoinGroup", 12: "Heartbeat", 13: "LeaveGroup",
  14: "SyncGroup", 15: "DescribeGroups", 16: "ListGroups", 17: "SaslHandshake",
  18: "ApiVersions", 36: "SaslAuthenticate",
};

export function Inspector({ events, stepIndex, preferResponse }: { events: KafkaTraceEvent[]; stepIndex: number; preferResponse?: boolean }) {
  const [tab, setTab] = useState<"req" | "res">("req");
  useEffect(() => setTab(preferResponse ? "res" : "req"), [stepIndex, preferResponse]);
  const group = useMemo(
    () => events.filter((e) => e.stepIndex === stepIndex),
    [events, stepIndex]
  );
  const req = group.find((e) => e.direction !== "response") ?? group[0];
  const res = [...group].reverse().find((e) => e.direction === "response");
  const cur = tab === "req" ? req : res ?? req;
  if (!cur) return <div className="text-[12px] text-slate-500">点击左侧 Trace 节点查看详情</div>;

  const apiLabel = cur.apiKey >= 0 ? API_LABEL[cur.apiKey] ?? cur.apiName : cur.apiName;
  const reqKv: [string, ReactNode][] = [
    ["API", <span className="mono" key="a">{cur.apiKey >= 0 ? `${cur.apiKey} (${apiLabel})` : apiLabel}</span>],
    ["Version", <span className="mono" key="b">{cur.apiVersion}</span>],
    ["Correlation", <span className="mono" key="c">{cur.correlationId}</span>],
  ];
  const r = (redact(cur.request) ?? {}) as Record<string, unknown>;
  for (const k of ["clientId", "mechanism", "phase", "topics", "topic", "partition",
                   "key", "value", "acks", "timeoutMs", "attempt", "offset",
                   "strategy", "target", "groupId", "generation", "memberId",
                   "securityProtocol", "saslMechanism", "bootstrap", "clientId ", "op"])
    if (r[k] != null && !reqKv.some(([x]) => x === k))
      reqKv.push([k, <span className="mono break-all" key={k}>{String(r[k])}</span>]);

  const resKv: [string, ReactNode][] = [];
  const s = (redact(cur.response ?? res?.response) ?? {}) as Record<string, unknown>;
  for (const k of ["errorCode", "errorName", "baseOffset", "logAppendTime", "logStartOffset",
                   "throttleMs", "highWatermark", "clusterId", "controller", "status",
                   "leader", "partition", "totalMs", "offset", "count", "attempts",
                   "generation", "memberId", "protocol", "groupId", "state", "error"])
    if (s[k] != null) resKv.push([k, <span className="mono break-all" key={k}>{String(s[k])}</span>]);

  return (
    <div>
      <div className="seg mb-2">
        {(["req", "res"] as const).map((t) => (
          <button key={t} onClick={() => setTab(t)} className={tab === t ? "on" : ""}>
            {t === "req" ? "Request" : "Response"}
          </button>
        ))}
      </div>
      <div className="text-[12.5px] font-semibold text-slate-800">
        {apiLabel} {tab === "req" ? "Request" : "Response"}
      </div>
      <div className="mt-1 divide-y divide-[var(--border-soft)]">
        {(tab === "req" ? reqKv : resKv).map(([k, v]) => <KV key={k} k={k} v={v} mono />)}
        {cur.topic && <KV k="Topic" v={cur.topic} mono />}
        {cur.partition != null && <KV k="Partition" v={cur.partition} mono />}
        {cur.broker && <KV k="Broker" v={`${cur.broker.id != null ? `#${cur.broker.id} ` : ""}${cur.broker.host}:${cur.broker.port}`} mono />}
        {cur.latencyMs != null && <KV k="Latency" v={`${cur.latencyMs} ms`} mono />}
        {cur.errorCode != null && cur.errorCode !== 0 && (
          <KV k="Error" v={`${cur.errorCode} (${cur.errorName ?? "?"})`} mono />
        )}
      </div>
      <div className="mt-3">
        <div className="card-title mb-1">Raw {tab === "req" ? "Request" : "Response"} (Hex){!(tab === "req" ? cur.rawRequest : cur.rawResponse) && " — 无（本地步骤）"}</div>
        {(tab === "req" ? cur.rawRequest : cur.rawResponse) && (
          <pre className="codebox max-h-36">
            {tab === "req" ? cur.rawRequest : cur.rawResponse}
          </pre>
        )}
      </div>
      <div className="mt-3">
        <div className="card-title mb-1">Protocol 解析</div>
        <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-2">
          <Tree data={tab === "req" ? redact(cur.request) : redact(cur.response ?? res?.response)} />
        </div>
      </div>
      {cur.apiName === "Metadata" && Array.isArray((cur.response as Record<string, unknown> | undefined)?.["partitions"]) && (
        <div className="mt-3">
          <div className="card-title mb-1">Topic: {cur.topic ?? ""}</div>
          <div className="tbl-wrap">
          <table className="tbl mono">
            <thead><tr>
              <th>Partition</th>
              <th>Leader</th>
              <th>Replicas</th>
              <th>ISR</th>
            </tr></thead>
            <tbody>
              {((cur.response as Record<string, unknown>)["partitions"] as Array<Record<string, unknown>>).map((p) => (
                <tr key={String(p["partition"])}>
                  <td>{String(p["partition"])}</td>
                  <td>{String(p["leader"])}</td>
                  <td>{Array.isArray(p["replicas"]) ? (p["replicas"] as unknown[]).join(",") : ""}</td>
                  <td>{Array.isArray(p["isr"]) ? (p["isr"] as unknown[]).join(",") : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        </div>
      )}
    </div>
  );
}
