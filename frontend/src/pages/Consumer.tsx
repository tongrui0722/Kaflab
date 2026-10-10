import { useEffect, useRef, useState } from "react";
import { DEFAULT_SECURITY } from "../api/producer";
import { useConn } from "../conn";
import { useAuth, authHeaders } from "../auth";
import { useLab } from "../lab";
import { Inspector } from "../components/Inspector";
import { FullTimeline } from "../components/Topology";
import { TraceTimeline } from "../components/TraceTimeline";
import { KV, PageHeader, StatusBadge } from "../components/bits";
import { stepsFromEvents } from "../mock/effect";
import type { KafkaTraceEvent } from "../types";

const inputCls = "field mono";
const labelCls = "lbl";
const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";
const WS = (import.meta.env.VITE_WS_BASE as string) || "ws://localhost:8000";

interface Msg { offset: number; key: string | null; value: string | null; partition: number; topic: string; }
interface FetchResult {
  messages: Msg[]; count: number; startOffset: number; highWatermark: number;
  commit?: { committedOffset: number; generation: number; memberId: string; errorCode: number; errorName: string } | null;
}

type Tab = "messages" | "trace" | "timeline";

export function ConsumerPage() {
  const { conn, health, probe } = useConn();
  const { mode } = useAuth();
  const readonly = mode === "readonly";
  const { requireLab } = useLab();
  const h = health[conn.id];
  const [probing, setProbing] = useState(false);
  const [form, setForm] = useState({
    bootstrap: "192.168.127.129:19092", topic: "test-topic", partition: 0,
    offset: "", maxMessages: 20, groupId: "",
    ...DEFAULT_SECURITY, requestTimeoutMs: 10000,
  });
  const [events, setEvents] = useState<KafkaTraceEvent[]>([]);
  const [running, setRunning] = useState(false);
  const [selected, setSelected] = useState(7);
  const [result, setResult] = useState<FetchResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("messages");
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState<number | null>(null);
  const [partitions, setPartitions] = useState<{ partition: number; leader: number }[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  // Topic 分区数：限制 Partition 只能在有效范围内选择
  useEffect(() => {
    let dead = false;
    (async () => {
      try {
        const r = await fetch(`${API}/api/clusters/${conn.id}/topics/${encodeURIComponent(form.topic)}`);
        if (!r.ok) throw new Error(await r.text());
        const d = await r.json() as { partitions: { partition: number; leader: number }[] };
        if (dead) return;
        setPartitions(d.partitions ?? []);
        setForm((f) => {
          const ps = (d.partitions ?? []).map((p) => p.partition);
          if (ps.length === 0) return f;
          return ps.includes(f.partition) ? f : { ...f, partition: ps[0] };
        });
      } catch {
        if (!dead) setPartitions([]);
      }
    })();
    return () => { dead = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [conn.id, form.topic]);

  // Topics 页跳过来的预填（topic/partition/maxMessages）
  useEffect(() => {
    try {
      const raw = localStorage.getItem("kcl-consumer-prefill");
      if (!raw) return;
      const p = JSON.parse(raw) as Partial<typeof form>;
      setForm((f) => ({ ...f, ...p }));
      localStorage.removeItem("kcl-consumer-prefill");
    } catch { /* ignore */ }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => () => { wsRef.current?.close(); }, []);

  const fetchMsgs = async () => {
    if (partitions.length > 0 && !partitions.some((p) => p.partition === form.partition)) {
      setError(`分区 P${form.partition} 不存在，该 Topic 仅有 ${partitions.length} 个分区（0–${partitions.length - 1}）`);
      return;
    }
    setRunning(true);
    setEvents([]);
    setResult(null);
    setError(null);
    setExpanded(null);
    try {
      const r = await fetch(`${API}/api/consumer/fetch`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({
          ...form,
          bootstrap: conn.bootstrap,
          securityProtocol: conn.securityProtocol,
          saslMechanism: conn.saslMechanism,
          saslUsername: conn.saslUsername,
          saslPassword: conn.saslPassword,
          offset: form.offset === "" ? null : Number(form.offset),
          clientId: "kafka-sim-01",
        }),
      });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const { sessionId } = await r.json() as { sessionId: string };
      const ws = new WebSocket(`${WS}/ws/session/${sessionId}`);
      wsRef.current = ws;
      ws.onmessage = (m) => {
        try {
          const msg = JSON.parse(m.data);
          if (msg.type === "ping" || !msg.event) return;
          const ev = msg.event as KafkaTraceEvent;
          setEvents((prev) => [...prev, ev]);
          if (ev.step === "session.done") {
            setRunning(false);
            setResult(ev.response as FetchResult);
            ws.close();
          }
          if (ev.step === "session.failed") {
            setRunning(false);
            setError(String((ev.response as Record<string, unknown>)?.["error"] ?? "failed"));
            ws.close();
          }
        } catch { /* ignore */ }
      };
      ws.onerror = () => { setRunning(false); setError("WebSocket 连接失败"); };
    } catch (e) {
      setRunning(false);
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const dot = h?.connected == null ? "bg-slate-500" : h.connected ? "bg-emerald-400" : "bg-rose-400";
  const statusText = h?.connected == null ? "检测中" : h.connected ? `已连接${h.brokerCount != null ? ` · ${h.brokerCount} brokers` : ""}` : "未连接";
  const reProbe = async () => {
    setProbing(true);
    try { await probe(conn.id); } finally { setProbing(false); }
  };

  const steps = stepsFromEvents(events);
  const msgs = (result?.messages ?? []).filter((m) =>
    !filter.trim() || (m.value ?? "").includes(filter) || (m.key ?? "").includes(filter));

  const tabBtn = (k: Tab, label: string, badge?: string) => (
    <button key={k} onClick={() => setTab(k)}
      className={`relative flex items-center gap-1.5 px-2.5 pb-2.5 pt-1 text-[12.5px] transition-colors ${tab === k ? "font-semibold text-sky-700" : "text-[var(--text-faint)] hover:text-slate-600"}`}>
      {label}
      {badge && <span className="badge mute !px-1.5 !text-[10px]">{badge}</span>}
      {tab === k && <span className="absolute inset-x-1 -bottom-px h-[2px] rounded-full bg-sky-400" />}
    </button>
  );

  return (
    <div>
      <PageHeader
        title="Consumer"
        desc="从 Leader 拉消息，可选入组自动提交位点"
        right={running ? <StatusBadge status="pending" text="Fetch 中" />
          : result ? <StatusBadge status="success" text={`取回 ${result.count} 条`} />
            : error ? <StatusBadge status="error" text="失败" /> : undefined}
      />
      <div className="layout-grid layout-3">
        {/* 左：配置 */}
        <div className="card col-side sticky-side h-fit p-3.5">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-800">Fetch 配置</span>
            <span className={`ml-auto inline-flex items-center gap-1.5 text-[11px] ${h?.connected ? "text-emerald-700" : h?.connected === false ? "text-rose-600" : "text-slate-500"}`}>
              <span className={`inline-block h-1.5 w-1.5 rounded-full ${dot} ${h?.connected === false ? "pulsing" : ""}`} />
              {probing ? "检测中…" : statusText}
            </span>
          </div>
          <div className="flex items-center gap-2 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
            <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-[rgba(56,189,248,0.12)] text-[14px] text-sky-700">⬇</span>
            <div className="min-w-0 flex-1">
              <div className="truncate text-[12px] font-semibold text-slate-700">{conn.name || "自定义"}</div>
              <div className="mono truncate text-[10.5px] text-[var(--text-faint)]">{conn.bootstrap} · {conn.securityProtocol}</div>
            </div>
            <button onClick={reProbe} disabled={probing} title="立即检测连接"
              className="btn btn-ghost btn-sm shrink-0 !px-2">{probing ? "…" : "↻"}</button>
          </div>
          {h?.connected === false && (
            <div className="alert err mt-2" title={h.error ?? ""}>
              集群未连接{h.error ? `：${h.error.slice(0, 90)}` : "，请检查 Bootstrap 与网络"}。
            </div>
          )}

          <div className="mt-2.5 grid grid-cols-2 gap-2">
            <div className="col-span-2"><label className={labelCls}>Topic</label>
              <input className={inputCls} value={form.topic}
                onChange={(e) => setForm({ ...form, topic: e.target.value })} /></div>
            <div><label className={labelCls}>Partition{partitions.length > 0 && <span className="ml-1 font-normal text-slate-600">共 {partitions.length} 个</span>}</label>
              {partitions.length > 0 ? (
                <select className={inputCls} value={form.partition}
                  title={`P${form.partition}`}
                  onChange={(e) => setForm({ ...form, partition: Number(e.target.value) })}>
                  {partitions.map((p) => (
                    <option key={p.partition} value={p.partition}>
                      P{p.partition} · L{p.leader}
                    </option>
                  ))}
                </select>
              ) : (
                <input type="number" min={0} className={inputCls} value={form.partition}
                  onChange={(e) => setForm({ ...form, partition: Math.max(0, Number(e.target.value)) })} />
              )}</div>
            <div><label className={labelCls}>Max Messages</label>
              <input type="number" min={1} max={200} className={inputCls} value={form.maxMessages}
                onChange={(e) => setForm({ ...form, maxMessages: Math.min(200, Math.max(1, Number(e.target.value))) })} /></div>
            <div className="col-span-2"><label className={labelCls}>Offset</label>
              <input className={inputCls} value={form.offset} placeholder="空 = 最新 N 条"
                onChange={(e) => setForm({ ...form, offset: e.target.value })} /></div>
            <div className="col-span-2"><label className={labelCls}>Group ID</label>
              <input className={inputCls} disabled={readonly} title={readonly ? "只读模式下不可提交消费位点" : undefined} value={form.groupId} placeholder="空 = 不入组"
                onChange={(e) => setForm({ ...form, groupId: e.target.value })} />
              <div className="mt-1 text-[10.5px] leading-relaxed text-[var(--text-faint)]">
                填了则先读已提交位点，Fetch 成功后自动 OffsetCommit。
              </div>
            </div>
          </div>

          <button onClick={() => { if (form.groupId.trim()) requireLab(() => { void fetchMsgs(); }); else void fetchMsgs(); }} disabled={running || h?.connected === false || (readonly && Boolean(form.groupId.trim()))} className="btn btn-fetch mt-3" title={readonly && form.groupId.trim() ? "只读巡检模式禁止提交消费位点，请清空 Group ID 后仅查看消息" : h?.connected === false ? "集群未连接，先恢复连接" : form.groupId.trim() ? "将提交位点，需实验模式确认" : undefined}>
            {running ? "Fetch 中…" : "⬇ Fetch 消息"}
          </button>
          {h?.connected === false && !error && (
            <div className="mt-1.5 text-[10.5px] text-slate-600">集群恢复连接后自动解锁 Fetch（心跳 20s）。</div>
          )}
          {error && <div className="alert err mt-2">{error}</div>}
        </div>

        {/* 中：结果区（消息为主） */}
        <div className="card min-w-0 px-3 pt-2">
          <div className="flex flex-wrap items-center gap-1 border-b border-[var(--border-soft)]">
            {tabBtn("messages", "消息", result ? `${msgs.length}` : undefined)}
            {tabBtn("trace", "请求追踪", events.length ? `${steps.length} 步` : undefined)}
            {tabBtn("timeline", "时间线", events.length ? `${events.length}` : undefined)}
            {tab === "messages" && result && (
              <input placeholder="过滤 key/value" value={filter} onChange={(e) => setFilter(e.target.value)}
                className="field mono mb-1.5 ml-auto !h-[28px] !w-40 !text-[11px]" />
            )}
          </div>
          <div className="py-3">
            {tab === "messages" && (
              !result ? (
                <div className="py-12 text-center">
                  <div className="text-[13px] font-semibold text-slate-600">还没有数据</div>
                  <div className="mono mt-2 text-[11px] leading-relaxed text-slate-500">
                    TCP → ApiVersions → Metadata → Partition → Leader → ListOffsets/OffsetFetch → Fetch
                  </div>
                  <div className="mt-2 text-[11.5px] text-slate-600">
                    左侧填好 Topic / 分区，点「Fetch 消息」，结果显示在这里。
                  </div>
                </div>
              ) : msgs.length === 0 ? (
                <div className="py-10 text-center text-[12px] text-slate-500">
                  {filter ? "没有匹配的消息。" : "该区间没有可读 records（分区为空或 offset 越界）。"}
                </div>
              ) : (
                <div className="scroll-box max-h-[560px] min-h-[200px] space-y-1 overflow-auto overscroll-contain pr-1">
                  {msgs.map((m) => {
                    const open = expanded === m.offset;
                    const long = (m.value ?? "").length > 160;
                    return (
                      <div key={m.offset} className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-3 py-2">
                        <div className="flex items-center gap-2">
                          <span className="badge run mono">offset {m.offset}</span>
                          {m.key != null && <span className="mono truncate text-[11px] text-slate-400">key: {m.key}</span>}
                          <span className="mono ml-auto shrink-0 text-[10.5px] text-[var(--text-faint)]">
                            {(m.value ?? "").length} B
                          </span>
                        </div>
                        <div className={`mono mt-1 break-all text-[11.5px] text-slate-700 ${open ? "" : "line-clamp-3"}`}>
                          {m.value}
                        </div>
                        {long && (
                          <button onClick={() => setExpanded(open ? null : m.offset)}
                            className="mt-1 text-[10.5px] text-sky-700/80 hover:text-sky-700">
                            {open ? "收起" : "展开全部"}
                          </button>
                        )}
                      </div>
                    );
                  })}
                </div>
              )
            )}
            {tab === "trace" && (
              events.length === 0
                ? <div className="py-12 text-center text-[12px] text-slate-500">Fetch 后显示协议步骤。</div>
                : <TraceTimeline steps={steps} selected={selected} onSelect={setSelected} />
            )}
            {tab === "timeline" && (
              events.length === 0
                ? <div className="py-12 text-center text-[12px] text-slate-500">Fetch 后按时间生成。</div>
                : <FullTimeline events={events} />
            )}
          </div>
        </div>

        {/* 右：Fetch 结果 + Inspector */}
        <div className="col-right sticky-side space-y-3">
          <div className="card p-3.5">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-800">Fetch 结果</span>
              {result && <span className="badge ok ml-auto">{result.count} 条</span>}
            </div>
            {!result && <div className="py-3 text-center text-[11.5px] text-slate-500">尚无结果。</div>}
            {result && (
              <>
                <KV k="Topic / P" v={`${form.topic} P${form.partition}`} mono />
                <KV k="Start Offset" v={String(result.startOffset)} mono />
                <KV k="High Watermark" v={String(result.highWatermark)} mono />
                <KV k="Records" v={String(result.count)} mono />
                {result.commit ? (
                  <>
                    <KV k="Commit Offset" v={String(result.commit.committedOffset)} mono />
                    <KV k="Generation" v={String(result.commit.generation)} mono />
                    <div className={`mt-1 ${result.commit.errorCode === 0 ? "" : ""}`}>
                      {result.commit.errorCode === 0
                        ? <span className="badge ok">OffsetCommit 成功</span>
                        : <span className="badge err">OffsetCommit {result.commit.errorName} ({result.commit.errorCode})</span>}
                    </div>
                  </>
                ) : (
                  <div className="mt-1.5">
                    <span className="badge mute">{form.groupId ? "OffsetCommit 未执行" : "未入组，无提交"}</span>
                  </div>
                )}
              </>
            )}
          </div>
          <div className="card p-3.5">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-800">请求 / 响应详情</span>
              {events.length > 0 && <span className="badge mute mono">step {selected}</span>}
            </div>
            {events.length === 0 ? (
              <div className="py-6 text-center text-[11.5px] text-slate-500">Fetch 后点击「请求追踪」里的步骤查看报文。</div>
            ) : (
              <Inspector events={events} stepIndex={selected} />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
