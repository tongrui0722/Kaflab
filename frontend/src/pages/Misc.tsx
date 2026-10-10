import { useEffect, useState } from "react";
import { apiGet } from "../api/producer";
import { useLab } from "../lab";
import { authHeaders, useAuth } from "../auth";
import { PageHeader } from "../components/bits";
import type { KafkaTraceEvent } from "../types";

const inputCls = "field mono";

export function ProtocolPage() {
  const [traceId, setTraceId] = useState("");
  const [events, setEvents] = useState<KafkaTraceEvent[] | null>(null);
  const [recent, setRecent] = useState<{ traceId: string; events: number; startTime: number; lastStep: string; status: string; topic?: string; api?: string }[]>([]);
  const [error, setError] = useState<string | null>(null);
  const loadRecent = async () => {
    try { setRecent(await apiGet<typeof recent>("/api/traces?limit=30")); }
    catch { /* 后端不可达则只保留手动输入 */ }
  };
  useEffect(() => { loadRecent(); }, []);
  const load = async (tid?: string) => {
    setError(null);
    const id = (tid ?? traceId).trim();
    if (!id) return;
    try {
      setEvents(await apiGet<KafkaTraceEvent[]>(`/api/traces/${id}`));
      setTraceId(id);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  return (
    <div className="mx-auto max-w-3xl space-y-3">
      <PageHeader title="协议解析" desc="按 traceId 回放协议事件" />
      <div className="card p-3">
        <div className="flex gap-2">
          <input className={`${inputCls} mono`} placeholder="tr-xxxxxxxx（也可从下方列表直接选择）"
            value={traceId} onChange={(e) => setTraceId(e.target.value)} />
          <button onClick={() => load()} className="btn btn-info shrink-0">加载</button>
        </div>
        {recent.length > 0 && (
          <div className="mt-2 max-h-44 space-y-1 overflow-auto">
            {recent.map((r) => (
              <button key={r.traceId} onClick={() => load(r.traceId)}
                className={`flex w-full items-center gap-2 rounded-md px-2 py-1 text-left text-[11px] hover:bg-white/[0.04] ${r.traceId === traceId ? "bg-sky-500/10" : ""}`}>
                <span className={`inline-block h-1.5 w-1.5 shrink-0 rounded-full ${r.status === "success" ? "bg-emerald-400" : r.status === "error" ? "bg-red-400" : "bg-sky-400"}`} />
                <span className="mono shrink-0 text-slate-300">{r.traceId}</span>
                <span className="mono truncate text-slate-500">{r.topic ?? ""} · {r.events} events</span>
                <span className="mono ml-auto shrink-0 text-slate-600">
                  {new Date(r.startTime).toLocaleTimeString("zh-CN", { hour12: false })}
                </span>
              </button>
            ))}
          </div>
        )}
        {error && <div className="mt-2 text-[12px] text-red-300">{error}</div>}
      </div>
      {events && (
        <div className="card p-3">
          <div className="card-title mb-2">{events.length} events</div>
          {events.map((e, i) => (
            <details key={i} className="border-t border-[var(--border-soft)] py-1.5 text-[12px]">
              <summary className="mono cursor-pointer text-slate-300">
                #{e.stepIndex} {e.apiName} {e.direction} corr={e.correlationId}
                {e.latencyMs != null ? ` ${e.latencyMs}ms` : ""}
                {e.errorCode ? ` ERR=${e.errorCode}` : ""}
              </summary>
              <pre className="codebox mt-1 max-h-48">
                {JSON.stringify({ request: e.request, response: e.response }, null, 1)}
              </pre>
              {(e.rawRequest || e.rawResponse) && (
                <pre className="codebox mt-1 max-h-48">
                  {e.rawRequest ?? e.rawResponse}
                </pre>
              )}
            </details>
          ))}
        </div>
      )}
    </div>
  );
}

export function FaultsPage() {
  const [states, setStates] = useState<Record<string, boolean>>({});
  const { requireLab } = useLab();
  const { mode } = useAuth();
  const readonly = mode === "readonly";
  const [out, setOut] = useState("");
  const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";
  const FAULTS: [string, string][] = [
    ["wrong_broker", "故意连非Leader → NotLeader → Metadata刷新重试"],
    ["disconnect_after_metadata", "Metadata后断开leader重连一次"],
    ["produce_timeout", "跳过发送模拟超时（按retries计数）"],
  ];
  const toggle = async (name: string, on: boolean) => {
    const r = await fetch(`${API}/api/faults`, {
      method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify({ name, enabled: on }),
    });
    const t = await r.text();
    setOut(t);
    try { setStates((JSON.parse(t) as { faults: Record<string, boolean> }).faults ?? {}); }
    catch { setStates((p) => ({ ...p, [name]: on })); }
  };
  const clear = async () => {
    await fetch(`${API}/api/faults`, { method: "DELETE", headers: { ...authHeaders() } });
    setStates({});
    setOut("cleared");
  };
  return (
    <div>
      <PageHeader title="故障模拟" desc="开关即时生效，故障会出现在链路里" />
    <div className="card mx-auto max-w-xl p-4">
      <div className="mb-2 flex items-center justify-between">
        <span className="card-title">Fault Injection（默认全关）</span>
        <button onClick={() => requireLab(() => { void clear(); })} disabled={readonly} title={readonly ? "只读巡检模式下禁止修改故障注入状态" : undefined} className="btn btn-ghost btn-sm">全部关闭</button>
      </div>
      <div className="space-y-2">
        {FAULTS.map(([name, desc]) => {
          const on = !!states[name];
          return (
            <div key={name} className="flex items-center gap-2 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
              <button onClick={() => requireLab(() => { void toggle(name, !on); })} disabled={readonly} title={readonly ? "只读巡检模式下禁止修改故障注入状态" : undefined}
                className={`mono w-12 shrink-0 rounded-md py-1 text-[11px] font-bold transition-all ${on ? "bg-amber-400 text-[#2b1a02] shadow-[0_4px_14px_-6px_rgba(251,191,36,0.7)]" : "bg-[#e5ebf3] text-slate-400 hover:text-slate-200"}`}>
                {on ? "ON" : "OFF"}
              </button>
              <div>
                <div className="mono text-[12px] text-slate-200">{name}</div>
                <div className="text-[11px] text-slate-500">{desc}</div>
              </div>
            </div>
          );
        })}
      </div>
      {out && <pre className="codebox mt-2 max-h-24">{out}</pre>}
      <div className="mt-2 text-[11.5px] leading-relaxed text-slate-500">
        开启后去 Producer 页发送一次，链路里会出现对应的故障节点。
      </div>
    </div>
    </div>
  );
}

export function TracePage() {
  return <ProtocolPage />;
}

export function SettingsPage() {
  return (
    <div>
      <PageHeader title="设置" desc="环境与连接说明" />
    <div className="card mx-auto max-w-xl space-y-2 p-4 text-[12.5px] leading-relaxed text-slate-300">
      <div>连接配置统一在左侧「集群」抽屉与集群管理页维护，各功能页共用当前集群。</div>
      <div className="mono text-[11.5px] text-slate-500">
        集群示例：PLAINTEXT://192.168.127.129:19092<br />
        SASL_PLAINTEXT://192.168.127.129:19094（PLAIN admin / 口令见部署文档）<br />
        SCRAM_PLAINTEXT://192.168.127.129:19095（SCRAM scramuser/口令）
      </div>
      <div>后端：`uvicorn app.main:app --port 8000`；前端 `.env`：VITE_API_BASE / VITE_WS_BASE。</div>
    </div>
    </div>
  );
}
