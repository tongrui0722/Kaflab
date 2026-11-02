import { apiFetch, apiWebSocket } from "../api/http";
import { useCallback, useEffect, useRef, useState } from "react";
import { useConn } from "../conn";
import { authHeaders } from "../auth";
import { Inspector } from "../components/Inspector";
import { FullTimeline } from "../components/Topology";
import { TraceTimeline } from "../components/TraceTimeline";
import { KV, PageHeader, StatusBadge } from "../components/bits";
import { TopicPicker } from "../components/TopicPicker";
import { stepsFromEvents } from "../traceSteps";
import type { KafkaTraceEvent } from "../types";

const inputCls = "field mono";
const labelCls = "lbl";
const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;
const WS = (import.meta.env.VITE_WS_BASE as string) || `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}`;

interface Msg { offset: number; key: string | null; value: string | null; partition: number; topic: string; }
interface FetchResult {
  messages: Msg[]; count: number; startOffset: number; highWatermark: number;
}

type Tab = "messages" | "trace" | "timeline";

export function ConsumerPage() {
  const { conn, health, probe } = useConn();
  const h = health[conn.id];
  const [probing, setProbing] = useState(false);
  const [form, setForm] = useState({
    topic: "", partition: 0, offset: "", maxMessages: 20,
  });
  const [offsetMode, setOffsetMode] = useState<"latest" | "specific">("latest");
  const [topicOptions, setTopicOptions] = useState<string[]>([]);
  const [topicLoading, setTopicLoading] = useState(false);
  const [topicError, setTopicError] = useState(false);
  const topicRequestSeq = useRef(0);
  const [events, setEvents] = useState<KafkaTraceEvent[]>([]);
  const [running, setRunning] = useState(false);
  const [selected, setSelected] = useState(7);
  const [result, setResult] = useState<FetchResult | null>(null);
  const [resultTarget, setResultTarget] = useState<{ topic: string; partition: number } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("messages");
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState<number | null>(null);
  const [partitions, setPartitions] = useState<{ partition: number; leader: number }[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    wsRef.current?.close();
    wsRef.current = null;
    setEvents([]);
    setRunning(false);
    setResult(null);
    setResultTarget(null);
    setError(null);
    setPartitions([]);
    setForm((current) => ({ ...current, topic: "", partition: 0, offset: "" }));
    setOffsetMode("latest");
  }, [conn.id]);

  const loadTopics = useCallback(async () => {
    const seq = ++topicRequestSeq.current;
    setTopicLoading(true);
    setTopicError(false);
    try {
      const response = await apiFetch(`${API}/api/clusters/${conn.id}/topics`);
      if (!response.ok) throw new Error(String(response.status));
      const data = await response.json() as { topics?: { topic: string }[] };
      if (seq !== topicRequestSeq.current) return;
      setTopicOptions([...new Set((data.topics ?? []).map((item) => item.topic).filter(Boolean))].sort());
    } catch {
      if (seq === topicRequestSeq.current) { setTopicOptions([]); setTopicError(true); }
    } finally {
      if (seq === topicRequestSeq.current) setTopicLoading(false);
    }
  }, [conn.id]);

  useEffect(() => {
    setTopicOptions([]);
    void loadTopics();
    return () => { ++topicRequestSeq.current; };
  }, [loadTopics]);

  // Topic 分区数：限制 Partition 只能在有效范围内选择
  useEffect(() => {
    if (!form.topic.trim()) { setPartitions([]); return; }
    let dead = false;
    (async () => {
      try {
        const r = await apiFetch(`${API}/api/clusters/${conn.id}/topics/${encodeURIComponent(form.topic)}`);
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
      setForm((f) => ({ ...f,
        topic: typeof p.topic === "string" ? p.topic : f.topic,
        partition: Number.isInteger(p.partition) && Number(p.partition) >= 0 ? Number(p.partition) : f.partition,
        maxMessages: Number.isInteger(p.maxMessages) ? Math.min(200, Math.max(1, Number(p.maxMessages))) : f.maxMessages,
      }));
      localStorage.removeItem("kcl-consumer-prefill");
    } catch { /* ignore */ }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => () => { wsRef.current?.close(); }, []);

  const fetchMsgs = async () => {
    const topic = form.topic.trim();
    if (!topic) { setError("请选择 Topic。"); return; }
    if (offsetMode === "specific" && (!/^\d+$/.test(form.offset) || !Number.isSafeInteger(Number(form.offset)))) {
      setError("起始 Offset 必须是非负整数。"); return;
    }
    if (partitions.length > 0 && !partitions.some((p) => p.partition === form.partition)) {
      setError(`分区 P${form.partition} 不存在，请重新选择分区。`);
      return;
    }
    wsRef.current?.close();
    setRunning(true);
    setEvents([]);
    setResult(null);
    setResultTarget(null);
    setError(null);
    setExpanded(null);
    try {
      const r = await apiFetch(`${API}/api/consumer/fetch`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({
          topic, partition: form.partition, maxMessages: form.maxMessages,
          clusterId: conn.id,
          bootstrap: conn.bootstrap,
          securityProtocol: conn.securityProtocol,
          saslMechanism: conn.saslMechanism,
          saslUsername: conn.saslUsername,
          saslPassword: conn.saslPassword,
          offset: offsetMode === "latest" ? null : Number(form.offset),
          groupId: "",
          clientId: "kafka-sim-01",
        }),
      });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const { sessionId } = await r.json() as { sessionId: string };
      const ws = apiWebSocket(`${WS}/ws/session/${sessionId}`);
      let completed = false;
      wsRef.current = ws;
      ws.onmessage = (m) => {
        if (wsRef.current !== ws) return;
        try {
          const msg = JSON.parse(m.data);
          if (msg.type === "ping" || !msg.event) return;
          const ev = msg.event as KafkaTraceEvent;
          setEvents((prev) => [...prev, ev]);
          if (ev.step === "session.done") {
            completed = true;
            setRunning(false);
            setResult(ev.response as FetchResult);
            setResultTarget({ topic, partition: form.partition });
            ws.close();
          }
          if (ev.step === "session.failed") {
            completed = true;
            setRunning(false);
            setError(String((ev.response as Record<string, unknown>)?.["error"] ?? "failed"));
            ws.close();
          }
        } catch { /* ignore */ }
      };
      ws.onerror = () => {
        if (wsRef.current === ws) { setRunning(false); setError("实时连接失败，请重新读取。"); }
      };
      ws.onclose = () => {
        if (wsRef.current !== ws) return;
        wsRef.current = null;
        if (!completed) { setRunning(false); setError("实时连接已中断，请重新读取。"); }
      };
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
      className={`consumer-tab relative flex items-center gap-1.5 px-2.5 pb-2.5 pt-1 text-[12.5px] ${tab === k ? "is-active font-semibold text-sky-700" : "text-[var(--text-faint)] hover:text-slate-600"}`}>
      {label}
      {badge && <span className="badge mute !px-1.5 !text-[10px]">{badge}</span>}
      {tab === k && <span className="absolute inset-x-1 -bottom-px h-[2px] rounded-full bg-sky-400" />}
    </button>
  );

  return (
    <div className="consumer-page">
      <PageHeader
        title="消费消息"
        right={running ? <StatusBadge status="pending" text="Fetch 中" />
          : result ? <StatusBadge status="success" text={`取回 ${result.count} 条`} />
            : error ? <StatusBadge status="error" text="失败" /> : undefined}
      />
      <div className="layout-grid layout-3 consumer-layout">
        {/* 左：配置 */}
        <div className="card col-side consumer-column p-3.5">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-800">读取配置</span>
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
              className="btn btn-ghost btn-sm shrink-0">{probing ? "检测中…" : "刷新连接"}</button>
          </div>
          {h?.connected === false && (
            <div className="alert err mt-2" title={h.error ?? ""}>
              集群未连接{h.error ? `：${h.error.slice(0, 90)}` : "，请检查 Bootstrap 与网络"}。
            </div>
          )}

          <div className="mt-2.5 grid grid-cols-2 gap-2">
            <div className="col-span-2"><TopicPicker value={form.topic} options={topicOptions}
              loading={topicLoading} error={topicError} onRefresh={() => { void loadTopics(); }}
              onChange={(topic) => { setPartitions([]); setForm((current) => ({ ...current, topic, partition: 0 })); }} /></div>
            <div><label className={labelCls} htmlFor="consumer-partition">Partition{partitions.length > 0 && <span className="ml-1 font-normal text-slate-600">共 {partitions.length} 个</span>}</label>
              {partitions.length > 0 ? (
                <select id="consumer-partition" className={inputCls} value={form.partition}
                  title={`P${form.partition}`}
                  onChange={(e) => setForm({ ...form, partition: Number(e.target.value) })}>
                  {partitions.map((p) => (
                    <option key={p.partition} value={p.partition}>
                      P{p.partition} · L{p.leader}
                    </option>
                  ))}
                </select>
              ) : (
                <input id="consumer-partition" type="number" min={0} className={inputCls} value={form.partition}
                  onChange={(e) => setForm({ ...form, partition: Math.max(0, Number(e.target.value)) })} />
              )}</div>
            <div><label className={labelCls} htmlFor="consumer-limit">最多读取</label>
              <input id="consumer-limit" type="number" min={1} max={200} className={inputCls} value={form.maxMessages}
                onChange={(e) => setForm({ ...form, maxMessages: Math.min(200, Math.max(1, Number(e.target.value))) })} /></div>
            <div className="col-span-2"><label className={labelCls}>读取位置</label>
              <div className="consumer-offset-switch" role="group" aria-label="读取位置">
                <button type="button" className={offsetMode === "latest" ? "is-active" : ""}
                  aria-pressed={offsetMode === "latest"} onClick={() => setOffsetMode("latest")}>最近 {form.maxMessages} 条</button>
                <button type="button" className={offsetMode === "specific" ? "is-active" : ""}
                  aria-pressed={offsetMode === "specific"} onClick={() => setOffsetMode("specific")}>指定 Offset</button>
              </div>
              {offsetMode === "specific" && <input type="number" min={0} step={1} aria-label="起始 Offset" className={`${inputCls} mt-2`}
                value={form.offset} placeholder="输入起始 Offset"
                onChange={(e) => setForm({ ...form, offset: e.target.value })} />}
            </div>
          </div>

          <button onClick={() => void fetchMsgs()} disabled={running || h?.connected === false || !form.topic.trim()}
            className="btn btn-fetch mt-3" title={h?.connected === false ? "集群未连接，先恢复连接" : !form.topic.trim() ? "请先选择 Topic" : undefined}>
            {running ? "读取中…" : "读取消息"}
          </button>
          {error && <div className="alert err mt-2">{error}</div>}
        </div>

        {/* 中：结果区（消息为主） */}
        <div className="card consumer-column min-w-0 px-3 pt-2">
          <div className="flex flex-wrap items-center gap-1 border-b border-[var(--border-soft)]">
            {tabBtn("messages", "消息", result ? `${msgs.length}` : undefined)}
            {tabBtn("trace", "请求追踪", events.length ? `${steps.length} 步` : undefined)}
            {tabBtn("timeline", "时间线", events.length ? `${events.length}` : undefined)}
            {tab === "messages" && result && (
              <input aria-label="过滤消息 Key 或 Value" placeholder="过滤 Key / Value" value={filter} onChange={(e) => setFilter(e.target.value)}
                className="field mono mb-1.5 ml-auto !h-[28px] !w-40 !text-[11px]" />
            )}
          </div>
          <div className="consumer-content-scroll scroll-box py-3">
            {tab === "messages" && (
              !result ? (
                <div className="py-12 text-center">
                  <div className="text-[13px] font-semibold text-slate-600">还没有数据</div>
                  <div className="mt-2 ui-note">选择 Topic 后读取消息</div>
                </div>
              ) : msgs.length === 0 ? (
                <div className="py-10 text-center text-[12px] text-slate-500">
                  {filter ? "没有匹配的消息。" : "该区间没有可读 records（分区为空或 offset 越界）。"}
                </div>
              ) : (
                <div className="space-y-1 pr-1">
                  {msgs.map((m) => {
                    const open = expanded === m.offset;
                    const long = (m.value ?? "").length > 160;
                    return (
                      <div key={m.offset} className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-3 py-2">
                        <div className="flex items-center gap-2">
                          <span className="badge run mono">offset {m.offset}</span>
                          {m.key != null && <span className="mono truncate text-[11px] text-slate-400">key: {m.key}</span>}
                          <span className="mono ml-auto shrink-0 text-[10.5px] text-[var(--text-faint)]">
                            {new TextEncoder().encode(m.value ?? "").length} B
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
                ? <div className="py-12 text-center ui-note">读取后显示协议步骤</div>
                : <TraceTimeline steps={steps} selected={selected} onSelect={setSelected} />
            )}
            {tab === "timeline" && (
              events.length === 0
                ? <div className="py-12 text-center ui-note">读取后显示时间线</div>
                : <FullTimeline events={events} />
            )}
          </div>
        </div>

        {/* 右：Fetch 结果 + Inspector */}
        <div className="col-right consumer-column consumer-result-column space-y-3">
          <div className="card p-3.5">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-800">Fetch 结果</span>
              {result && <span className="badge ok ml-auto">{result.count} 条</span>}
            </div>
            {!result && <div className="py-3 text-center text-[11.5px] text-slate-500">尚无结果。</div>}
            {result && (
              <>
                <KV k="Topic / P" v={`${resultTarget?.topic ?? "—"} P${resultTarget?.partition ?? "—"}`} mono />
                <KV k="Start Offset" v={String(result.startOffset)} mono />
                <KV k="High Watermark" v={String(result.highWatermark)} mono />
                <KV k="Records" v={String(result.count)} mono />
                <KV k="读取方式" v="直接读取" />
              </>
            )}
          </div>
          <div className="card p-3.5">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-800">请求 / 响应详情</span>
              {events.length > 0 && <span className="badge mute mono">step {selected}</span>}
            </div>
            {events.length === 0 ? (
              <div className="py-6 text-center ui-note">选择追踪步骤查看报文</div>
            ) : (
              <Inspector events={events} stepIndex={selected} />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
