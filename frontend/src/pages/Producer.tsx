import { useEffect, useRef, useState } from "react";
import { DEFAULT_PRODUCE, clusterQuery, type ClusterStatus, type ProduceForm } from "../api/producer";
import { useConn } from "../conn";
import { reportConn } from "../App";
import { Inspector } from "../components/Inspector";
import { TopologyGraph, FullTimeline } from "../components/Topology";
import { TraceTimeline } from "../components/TraceTimeline";
import { KV, PageHeader, StatusBadge } from "../components/bits";
import { useProducerSession } from "../api/producer";
import { friendlyWriteError, useAuth } from "../auth";
import { useLab } from "../lab";
import { stepsFromEvents, fmtTime } from "../mock/effect";
import type { RecentMsg } from "../types";

const inputCls = "field mono";
const labelCls = "lbl";

async function getJSON<T>(path: string): Promise<T> {
  const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";
  let r: Response;
  try {
    r = await fetch(`${API}${path}`);
  } catch (e) {
    throw new Error(
      `后端不可达（${API}）：请先启动 backend（uvicorn app.main:app --port 8000）。原始错误：${e instanceof Error ? e.message : String(e)}`);
  }
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}

export function ProducerPage() {
  const { session, events, running, sendError, wsDown, send } = useProducerSession();
  const { conn } = useConn();
  const { mode: authMode } = useAuth();
  const readonly = authMode === "readonly";
  const { requireLab } = useLab();
  const connFields = () => ({
    bootstrap: conn.bootstrap,
    securityProtocol: conn.securityProtocol,
    saslMechanism: conn.saslMechanism,
    saslUsername: conn.saslUsername,
    saslPassword: conn.saslPassword,
  });
  const [form, setForm] = useState<ProduceForm>({ ...DEFAULT_PRODUCE });
  const [selected, setSelected] = useState(6);
  const [recent, setRecent] = useState<RecentMsg[]>(() => {
    try {
      const raw = localStorage.getItem("kcl-recent");
      return raw ? JSON.parse(raw) as RecentMsg[] : [];
    } catch { return []; }
  });
  const activeRecentId = useRef<string | null>(null);
  const [mode, setMode] = useState<"single" | "batch">("single");
  const [batchCount, setBatchCount] = useState(10);
  const [sending, setSending] = useState(false);
  const [showConnDetail, setShowConnDetail] = useState(false);
  const [topicOptions, setTopicOptions] = useState<string[]>([]);
  const [topicOpen, setTopicOpen] = useState(false);
  const [detailId, setDetailId] = useState<string | null>(null);
  const [copiedWhat, setCopiedWhat] = useState<string | null>(null);

  const copyText = async (tag: string, text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedWhat(tag);
      window.setTimeout(() => setCopiedWhat((c) => (c === tag ? null : c)), 1500);
    } catch { /* ignore */ }
  };
  const [copied, setCopied] = useState(false);
  const [cluster, setCluster] = useState<ClusterStatus | null>(null);
  const [clusterErr, setClusterErr] = useState<string | null>(null);
  const [probing, setProbing] = useState(false);
  const [sendFail, setSendFail] = useState<string | null>(null);
  const [midTab, setMidTab] = useState<"trace" | "timeline" | "faults">("trace");
  const [faultStates, setFaultStates] = useState<Record<string, boolean>>({});
  const loadFaults = async () => {
    try {
      const d = await getJSON<{ faults: Record<string, boolean> }>("/api/faults");
      setFaultStates(d.faults ?? {});
    } catch { /* 后端不可达则保持空 */ }
  };
  useEffect(() => { loadFaults(); }, []);
  useEffect(() => { if (midTab === "faults") loadFaults(); }, [midTab]);

  const pushRecent = (r: RecentMsg) => setRecent((prev) => {
    const next = [r, ...prev].slice(0, 30);
    try { localStorage.setItem("kcl-recent", JSON.stringify(next)); } catch { /* ignore */ }
    return next;
  });

  useEffect(() => {
    // 刷新后从后端拉历史 session，补齐最近消息（后端已落盘持久化）
    getJSON<import("../types").Session[]>("/api/sessions").then((list) => {
      if (recent.length > 0) return;
      const mapped: RecentMsg[] = list.slice(0, 20).map((s) => ({
        id: s.id, topic: s.topic, partition: s.partition ?? -1,
        offset: s.offset ?? -1, latency: s.durationMs != null ? `${s.durationMs}ms` : "—",
        time: fmtTime(s.startTime), status: s.status === "success" ? "SUCCESS" : s.status === "error" ? "ERROR" : "RUNNING",
        traceId: "", events: [],
      }));
      if (mapped.length) {
        setRecent(mapped);
        try { localStorage.setItem("kcl-recent", JSON.stringify(mapped)); } catch { /* ignore */ }
      }
    }).catch(() => undefined);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const steps = stepsFromEvents(events);
  const doneEv = events.find((e) => e.step === "session.done");
  const failEv = events.find((e) => e.step === "session.failed");
  const doneResp = (doneEv?.response ?? {}) as Record<string, unknown>;
  const failResp = (failEv?.response ?? {}) as Record<string, unknown>;

  // 本次发送完成 → 把最上面那条 RUNNING 的最近消息更新为真实结果
  useEffect(() => {
    if (!doneEv && !failEv) return;
    const ok = !!doneEv;
    const partition = Number(doneResp["partition"] ?? failEv?.partition ?? -1);
    const offset = Number(doneResp["offset"] ?? -1);
    const latency = doneResp["totalMs"] != null ? `${doneResp["totalMs"]}ms` : "—";
    const traceId = (doneEv ?? failEv)?.traceId ?? "";
    const currentId = activeRecentId.current;
    if (!currentId) return;
    setRecent((prev) => {
      const i = prev.findIndex((r) => r.id === currentId);
      if (i < 0) return prev;
      const next = [...prev];
      next[i] = { ...next[i], status: ok ? "SUCCESS" : "ERROR",
                  partition: Number.isNaN(partition) ? next[i].partition : partition,
                  offset: Number.isNaN(offset) ? next[i].offset : offset,
                  latency, traceId };
      try { localStorage.setItem("kcl-recent", JSON.stringify(next.slice(0, 30))); } catch { /* ignore */ }
      return next;
    });
    activeRecentId.current = null;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [doneEv, failEv]);
  // 失败 → Trace 自动选中出错步骤，右侧切到 Response（成功不抢用户当前选择）
  const [preferRes, setPreferRes] = useState(false);
  useEffect(() => {
    if (!failEv) { setPreferRes(false); return; }
    if (failEv.stepIndex != null) setSelected(failEv.stepIndex);
    setPreferRes(true);
  }, [failEv]);
  useEffect(() => { setPreferRes(false); }, [selected]);
  const lastSentPartition = doneEv?.topic === form.topic && doneResp["partition"] != null
    ? Number(doneResp["partition"]) : undefined;
  const activePartition = form.partition !== "auto" ? Number(form.partition)
    : events.find((e) => e.partition != null && e.topic === form.topic)?.partition
      ?? lastSentPartition
      ?? cluster?.partitions[0]?.partition ?? 0;
  const previewPartition = cluster?.partitions.find((p) => p.partition === activePartition);
  const previewLeader = cluster?.brokers.find((b) => b.id === previewPartition?.leader);
  const leaderOf = (p?: number) => cluster?.partitions.find((x) => x.partition === p)?.leader;
  const leaderAddr = (p?: number) => {
    const id = leaderOf(p);
    const b = cluster?.brokers.find((x) => x.id === id);
    return { id, addr: b ? `${b.host}:${b.port}` : "N/A" };
  };
  // Auto 模式下本次真实选中的分区（来自 partition.selected / session.done）
  const selPartEv = events.find((e) => e.step === "partition.selected");
  const selPart = (selPartEv?.response as Record<string, unknown> | undefined)?.["partition"]
    ?? doneResp["partition"];
  const selLeader = (selPartEv?.response as Record<string, unknown> | undefined)?.["leader"];

  const probe = async () => {
    setProbing(true);
    setClusterErr(null);
    try {
      const f = { ...form, ...connFields() };
      const d = await getJSON<ClusterStatus>(clusterQuery(f, f.topic));
      setCluster(d);
      reportConn({ ok: true, bootstrap: f.bootstrap, detail: `${d.brokerCount} brokers` });
    } catch (e) {
      setCluster(null);
      const msg = e instanceof Error ? e.message : String(e);
      setClusterErr(msg);
      reportConn({ ok: false, bootstrap: conn.bootstrap, detail: msg.slice(0, 80) });
    } finally {
      setProbing(false);
    }
  };

  // Topics 页跳过来的预填 / 全局集群切换 → 刷新分区
  useEffect(() => {
    try {
      const raw = localStorage.getItem("kcl-producer-prefill");
      if (raw) {
        const p = JSON.parse(raw) as Partial<ProduceForm>;
        if (p.topic) setForm((f) => ({ ...f, topic: p.topic as string }));
        localStorage.removeItem("kcl-producer-prefill");
      }
    } catch { /* ignore */ }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  // 全局集群切换 → 自动刷新本页分区
  useEffect(() => { probe(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ },
    [conn.bootstrap, conn.securityProtocol, conn.saslMechanism, conn.saslUsername, conn.saslPassword, form.topic]);
  // 主题下拉候选：当前集群 Topic 列表，供输入过滤选择
  useEffect(() => {
    let dead = false;
    (async () => {
      try {
        const d = await getJSON<{ topics?: { topic: string }[] }>(
          `/api/clusters/${conn.id}/topics`);
        if (!dead) {
          setTopicOptions(
            [...new Set((d.topics ?? []).map((t) => t.topic).filter(Boolean))].sort());
        }
      } catch { if (!dead) setTopicOptions([]); }
    })();
    return () => { dead = true; };
  }, [conn.id]);

  const randStr = (n = 16) => {
    const cs = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789";
    let s = "";
    const buf = new Uint32Array(n);
    crypto.getRandomValues(buf);
    for (let i = 0; i < n; i++) s += cs[buf[i] % cs.length];
    return s;
  };

  const valueSize = new TextEncoder().encode(form.value).length;

  const buildKV = (key: string, value: string) => ({ key, value, keyHex: "", valueHex: "" });

  const doOneSend = async (payload: ProduceForm) => {
    const payloadSize = new TextEncoder().encode(payload.value).length;
    const id = `r-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
    activeRecentId.current = id;
    pushRecent({
      id, topic: payload.topic,
      partition: payload.partition === "auto" ? -1 : Number(payload.partition),
      offset: -1, latency: "…", time: fmtTime(Date.now()),
      status: "RUNNING" as const, traceId: "", events: [],
      key: payload.key, value: payload.value,
      size: payloadSize || undefined,
    });
    await send({ ...payload, ...connFields() });
  };

  const resendRecord = async (r: RecentMsg) => {
    setSendFail(null);
    setSending(true);
    try {
      if (r.key != null) setForm((f) => ({ ...f, key: r.key as string, value: r.value ?? f.value }));
      await doOneSend({ ...form, key: r.key ?? form.key, value: r.value ?? form.value, keyHex: "", valueHex: "" });
    } catch (e) { setSendFail(e instanceof Error ? e.message : String(e)); }
    finally { setSending(false); }
  };

  useEffect(() => {
    if (!detailId) return;
    const h = (e: KeyboardEvent) => { if (e.key === "Escape") setDetailId(null); };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [detailId]);

  const onSend = async () => {
    if (!form.topic.trim()) {
      setSendFail("请先填写 Topic（可输入新名称，或从下拉列表选择）。");
      return;
    }
    setSendFail(null);
    setSending(true);
    try {
      if (mode === "single") {
        const key = form.key || `k-${randStr(6)}`;
        const p = { ...form, key, value: form.value || randStr(32), keyHex: "", valueHex: "" };
        await doOneSend(p);
        return;
      }
      // 批量：一次 Produce 请求带 N 条 records（真批量），Trace 里只有一条 Produce
      const n = Math.max(2, Math.min(100, Math.floor(batchCount) || 10));
      const messages = Array.from({ length: n }, () => {
        const key = form.key || `k-${randStr(6)}`;
        return buildKV(key, form.value || randStr(32));
      });
      const id = `r-${Date.now()}-b${n}`;
      activeRecentId.current = id;
      pushRecent({
        id, topic: form.topic,
        partition: form.partition === "auto" ? -1 : Number(form.partition),
        offset: -1, latency: "…", time: fmtTime(Date.now()),
        status: "RUNNING" as const, traceId: "", events: [],
        key: messages[0].key, value: messages[0].value,
        size: messages.reduce((sum, m) => sum + new TextEncoder().encode(m.value).length, 0),
      });
      await send({ ...form, ...connFields(), messages });
    } catch (e) {
      setSendFail(friendlyWriteError(e));
      const currentId = activeRecentId.current;
      if (currentId) setRecent((prev) => {
        const next = prev.map((r) => r.id === currentId ? { ...r, status: "ERROR" as const, latency: "—" } : r);
        try { localStorage.setItem("kcl-recent", JSON.stringify(next.slice(0, 30))); } catch { /* ignore */ }
        return next;
      });
      activeRecentId.current = null;
    } finally {
      setSending(false);
    }
  };

  const connected = !!cluster?.connected;
  const statusBadge = running ? "pending"
    : failEv || sendError || sendFail || session?.status === "error" ? "error"
    : doneEv || session?.status === "success" ? "success" : "pending";
  const statusText = running ? "发送中"
    : failEv || sendError || sendFail || session?.status === "error" ? "失败"
    : doneEv || session?.status === "success" ? "成功" : "待发送";

  return (
    <div className="producer-page">
      <PageHeader title="生产消息" desc="构造消息并发送到 Kafka，实时观察分区路由、Broker 确认和 Offset"
      right={<><StatusBadge status={statusBadge} text={statusText} />{session?.id && <span className="mono text-[10.5px] text-slate-500" title="本次 Producer Session">{session.id}</span>}{doneEv && <span className="mono text-[11px] text-slate-500">Offset: {String(doneResp["offset"] ?? "")}</span>}</>} />
    <div className="producer-workspace">
    <div className="layout-grid producer-layout">
      {/* 左：配置（连接 / 消息 / 发送模式分区展示） */}
      <div className="producer-column producer-config-column">
        <div className="card p-3">
          <div className="mb-2 flex items-center gap-2">
            <span className="card-title !mb-0">连接</span>
            {cluster && <span className="mono text-[10.5px] text-emerald-300">✓ {cluster.brokerCount} brokers</span>}
            <button onClick={probe} disabled={probing} title="刷新分区"
              className="btn btn-ghost btn-sm ml-auto !px-2">↻</button>
          </div>
          <div className="mono truncate rounded-lg border border-[#c9d6e3] bg-white px-2 py-1.5 text-[11px] text-slate-700" title={`${conn.bootstrap} · ${conn.securityProtocol}`}>
            {conn.bootstrap}
          </div>
          {clusterErr && (
            <div className="alert err mt-2">
              集群不可达：{clusterErr}
            </div>
          )}
          {cluster && (
            <>
              <button onClick={() => setShowConnDetail((v) => !v)}
                className="mono mt-1 text-[10.5px] text-slate-500 hover:text-slate-300">
                {showConnDetail ? "▾ 详情" : "▸ 详情"}
              </button>
              {showConnDetail && (
                <div className="mono mt-1 space-y-[2px] rounded-lg bg-black/20 p-2 text-[10.5px] text-slate-400">
                  {cluster.brokers.map((b) => (
                    <div key={b.id ?? `${b.host}:${b.port}`}>Broker {b.id ?? "?"}: {b.host}:{b.port}</div>
                  ))}
                </div>
              )}
            </>
          )}
        </div>

        <div className="card p-3">
          <div className="mb-2 flex items-center justify-between">
            <span className="card-title !mb-0">消息内容</span>
            <span className="badge mute">{mode === "batch" ? `批量 · ${batchCount} 条` : "单条消息"}</span>
          </div>
          <div><label className={labelCls}>Topic</label>
            <div className="relative">
              <input className={`${inputCls} mono`} value={form.topic}
                placeholder="输入或选择 Topic"
                onFocus={() => setTopicOpen(true)}
                onBlur={() => window.setTimeout(() => setTopicOpen(false), 150)}
                onChange={(e) => { setTopicOpen(true); setForm({ ...form, topic: e.target.value, partition: "auto" }); }} />
              {topicOpen && (form.topic.trim() || topicOptions.length > 0) && (() => {
                const q = form.topic.trim().toLowerCase();
                const matched = topicOptions.filter((t) => !q || t.toLowerCase().includes(q)).slice(0, 8);
                const exact = matched.some((t) => t === form.topic.trim());
                if (!matched.length && !form.topic.trim()) return null;
                return (
                  <div className="absolute inset-x-0 top-full z-30 mt-1 max-h-56 overflow-auto rounded-lg border border-[var(--border)] bg-white py-1 shadow-lg">
                    {matched.map((t) => (
                      <button key={t} onMouseDown={(e) => e.preventDefault()}
                        onClick={() => { setForm({ ...form, topic: t, partition: "auto" }); setTopicOpen(false); }}
                        title={t}
                        className="flex w-full items-center gap-2 px-2.5 py-1.5 text-left hover:bg-slate-900/[0.04]">
                        <span className="mono min-w-0 flex-1 truncate text-[12px] text-slate-700">{t}</span>
                        {t.startsWith("_") && <span className="badge mute shrink-0">系统</span>}
                      </button>
                    ))}
                    {form.topic.trim() && !exact && (
                      <button onMouseDown={(e) => e.preventDefault()}
                        onClick={() => setTopicOpen(false)}
                        title={`直接使用“${form.topic.trim()}”`}
                        className="flex w-full items-center gap-2 border-t border-[var(--border-soft)] px-2.5 py-1.5 text-left hover:bg-slate-900/[0.04]">
                        <span className="min-w-0 flex-1 truncate text-[12px] text-slate-500">
                          直接使用“<span className="mono text-slate-700">{form.topic.trim()}</span>”
                        </span>
                      </button>
                    )}
                  </div>
                );
              })()}
            </div></div>
          <div className="mt-2"><label className={labelCls}>Key</label>
            <input className={`${inputCls} mono`} value={form.key} placeholder="空 = 随机 Key"
              onChange={(e) => setForm({ ...form, key: e.target.value })} /></div>
          <div className="mt-2">
            <div className="mb-1 flex items-baseline justify-between">
              <label className={labelCls} style={{ marginBottom: 0 }}>Value</label>
              <span className="mono text-[10.5px] text-slate-600">{form.value ? `${valueSize} B` : "随机 32B"}</span>
            </div>
            <textarea rows={2} spellCheck={false} placeholder="空 = 随机 32 位字符串"
              className={`${inputCls} mono resize-y`} value={form.value}
              onChange={(e) => setForm({ ...form, value: e.target.value })} />
          </div>
          <details className="mt-2 rounded-lg bg-black/20 px-2 py-1.5">
            <summary className="cursor-pointer text-[11px] text-slate-500 hover:text-slate-300">更多参数</summary>
          <div className="mt-2 grid grid-cols-2 gap-2">
            <div className="min-w-0"><label className={labelCls}>目标分区</label>
              <select className={inputCls} value={form.partition}
                title={form.partition === "auto" ? "自动：发送前按集群 Metadata 选择首个健康分区"
                  : `P${form.partition} · Leader ${leaderOf(Number(form.partition)) ?? "未知"}`}
                onChange={(e) => setForm({ ...form, partition: e.target.value })}>
                <option value="auto">自动</option>
                {(cluster?.partitions ?? []).map((p) => (
                  <option key={p.partition} value={String(p.partition)}>
                    P{p.partition} · L{p.leader}
                  </option>
                ))}
              </select>
              {(form.partition === "auto" ? selPart != null : form.partition !== "auto") && (
                <div className="mono mt-1 text-[10.5px] text-slate-500">
                  {form.partition === "auto"
                    ? (selPart != null ? <>当前命中 P{String(selPart)}{selLeader != null ? <> · Leader {String(selLeader)}</> : null}</> : <>发送前按集群 Metadata 选择</>)
                    : (() => { const l = leaderAddr(Number(form.partition)); return (
                      <>P{form.partition} · Leader {l.id ?? "N/A"}</>); })()}
                </div>
              )}
            </div>
            <div className="min-w-0"><label className={labelCls}>Acks</label>
              <select className={inputCls} value={form.acks}
                onChange={(e) => setForm({ ...form, acks: e.target.value })}>
                <option value="all">all</option><option value="1">1</option><option value="0">0</option>
              </select></div>
          </div>
          <div className="mt-2 grid grid-cols-2 gap-2">
            <div><label className={labelCls}>Retries</label>
              <input type="number" className={`${inputCls} mono`} value={form.retries}
                onChange={(e) => setForm({ ...form, retries: Number(e.target.value) })} /></div>
            <div><label className={labelCls}>请求超时 (ms)</label>
              <input type="number" className={`${inputCls} mono`} value={form.requestTimeoutMs}
                onChange={(e) => setForm({ ...form, requestTimeoutMs: Number(e.target.value) })} /></div>
          </div>
          </details>
        </div>

        <div className="card p-3">
          <div className="card-title mb-2">发送方式</div>
          <div className="flex gap-4 text-[12px]">
            {(["single", "batch"] as const).map((m) => (
              <button key={m} onClick={() => setMode(m)} className="flex items-center gap-1.5">
                <span className={`inline-flex h-3.5 w-3.5 items-center justify-center rounded-full ring-1 ${mode === m ? "bg-sky-500/30 ring-sky-400" : "ring-slate-600"}`}>
                  {mode === m && <span className="h-1.5 w-1.5 rounded-full bg-sky-300" />}
                </span>
                <span className={mode === m ? "text-slate-100" : "text-slate-500"}>
                  {m === "single" ? "发送单条" : "批量发送"}
                </span>
              </button>
            ))}
          </div>
          {mode === "batch" && (
            <div className="mt-2 flex items-center gap-2"><label className={labelCls} style={{ marginBottom: 0 }}>消息条数</label>
              <input type="number" min={2} max={100} className={`${inputCls} mono`} value={batchCount}
                onChange={(e) => setBatchCount(Number(e.target.value))} /></div>
          )}
          <button onClick={() => requireLab(() => { void onSend(); })} disabled={readonly || running || sending || !form.topic.trim()} title={readonly ? "只读巡检模式下禁止发送消息" : !form.topic.trim() ? "请先填写 Topic" : undefined}
            className={`btn btn-send mt-3 ${!running && !sending && (failEv || sendError || sendFail) ? "is-failed" : ""}`}>
            {sending || running ? "正在发送…" : failEv || sendError || sendFail ? "重试发送" : mode === "batch" ? `发送 ${batchCount} 条` : "发送消息"}
          </button>
          {(sendFail || sendError) && (
            <div className="alert err mt-2">
              发送失败：{sendFail ?? sendError}
            </div>
          )}
          {wsDown && (
            <div className="alert warn mt-2">
              连接中断，可通过 Session 查看结果。
            </div>
          )}
        </div>

        <div className="card producer-route-card p-3">
          <div className="mb-2 flex items-center justify-between gap-2">
            <span className="card-title !mb-0">路由预览</span>
            <span className={`badge ${previewPartition && (previewPartition.isr?.length ?? 0) === (previewPartition.replicas?.length ?? 0) ? "ok" : "mute"}`}>
              {form.partition === "auto" ? "自动分区" : `固定 P${activePartition}`}
            </span>
          </div>
          <div className="mono mb-2 truncate text-[10.5px] text-slate-500" title={form.topic || "未填写 Topic"}>
            {form.topic || "未填写 Topic"}
          </div>
          <div className="grid grid-cols-2 gap-2">
            <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
              <div className="text-[10px] text-[var(--text-faint)]">目标分区</div>
              <div className="mono mt-0.5 truncate text-[12px] font-semibold text-slate-200">
                P{activePartition}{form.partition === "auto" ? " · 自动" : ""}
              </div>
            </div>
            <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
              <div className="text-[10px] text-[var(--text-faint)]">Leader</div>
              <div className="mono mt-0.5 truncate text-[12px] font-semibold text-slate-200" title={previewLeader ? `${previewLeader.host}:${previewLeader.port}` : "当前没有 Leader"}>
                {previewLeader ? `Broker ${previewLeader.id}` : "暂无"}
              </div>
              {previewLeader && <div className="mono truncate text-[11px] text-slate-500">{previewLeader.host}:{previewLeader.port}</div>}
            </div>
            <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
              <div className="text-[10px] text-[var(--text-faint)]">副本</div>
              <div className="mono mt-0.5 text-[12px] font-semibold text-slate-200">{previewPartition?.replicas.length ?? "—"}</div>
            </div>
            <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
              <div className="text-[10px] text-[var(--text-faint)]">ISR / 副本</div>
              <div className={`mono mt-0.5 text-[12px] font-semibold ${previewPartition && previewPartition.isr.length < previewPartition.replicas.length ? "text-amber-300" : "text-emerald-300"}`}>
                {previewPartition ? `${previewPartition.isr.length} / ${previewPartition.replicas.length}` : "—"}
              </div>
            </div>
          </div>
          <div className="mt-2 text-[10.5px] leading-relaxed text-slate-500">
            {form.acks === "all" ? "acks=all：等待当前 ISR 副本确认。" : form.acks === "1" ? "acks=1：等待 Leader 确认。" : "acks=0：不等待 Broker 确认。"}
          </div>
        </div>
      </div>

      {/* 中：Trace */}
      <div className="producer-column producer-observe-column min-w-0">
        <div className="card producer-trace-card px-3 pt-2">
          <div className="flex gap-1 border-b border-[var(--border-soft)] text-[12.5px]">
            {([["trace", "发送追踪"], ["timeline", "时间线"], ["faults", "故障分析"]] as const).map(([k, t]) => (
              <button key={k} onClick={() => setMidTab(k)}
                className={`relative px-2.5 pb-2.5 pt-1 transition-colors ${midTab === k ? "font-semibold text-sky-300" : "text-[var(--text-faint)] hover:text-slate-300"}`}>
                {t}
                {midTab === k && <span className="absolute inset-x-1 -bottom-px h-[2px] rounded-full bg-sky-400" />}
              </button>
            ))}
          </div>
          <div className="py-3">
            {midTab === "trace" && (
              events.length === 0 ? (
                <div className="py-10 text-center text-[12.5px] leading-relaxed text-slate-500">
                  {connected
                    ? "已连接集群，点击左侧「发送消息」查看真实链路。"
                    : "未连接集群。填写 Bootstrap + SASL 口令后点「更新连接」。"}
                  <br />
                  <span className="mono text-[11px] text-slate-600">
                    无连接时 Trace 为空，连接后显示真实链路。
                  </span>
                </div>
              ) : (
                <TraceTimeline steps={steps} selected={selected} onSelect={setSelected}
                  total={doneResp["totalMs"] != null ? `${doneResp["totalMs"]} ms` : undefined} />
              )
            )}
            {midTab === "timeline" && <FullTimeline events={events} />}
            {midTab === "faults" && (
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-[12px] text-slate-400">
                    当前发送生效的故障开关（与「故障模拟」页同源，默认全关）
                  </span>
                  <button onClick={loadFaults} className="btn btn-ghost btn-sm">
                    刷新
                  </button>
                </div>
                {Object.keys(faultStates).length === 0 ? (
                  <div className="py-6 text-center text-[12px] text-slate-500">
                    未开启任何故障注入。开启后 Trace 会出现 fault.injected / produce.retry / metadata.refreshed 节点。
                  </div>
                ) : (
                  <div className="space-y-1">
                    {Object.entries(faultStates).map(([k, v]) => (
                      <div key={k} className="flex items-center gap-2 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2 py-1.5">
                        <span className={`badge ${v ? "warn" : "mute"} mono !w-12 justify-center`}>
                          {v ? "ON" : "OFF"}
                        </span>
                        <span className="mono text-[11.5px] text-slate-300">{k}</span>
                      </div>
                    ))}
                  </div>
                )}
                <div className="card-title mt-3">本次 Trace 中的故障/重试事件</div>
                {events.filter((e) => e.step.startsWith("fault") || e.step.includes("retry") || e.step.includes("refresh")).length === 0 ? (
                  <div className="py-3 text-center text-[11.5px] text-slate-600">本次链路无故障事件（正常路径）。</div>
                ) : (
                  <div className="space-y-1">
                    {events.filter((e) => e.step.startsWith("fault") || e.step.includes("retry") || e.step.includes("refresh")).map((e, i) => (
                      <div key={i} className="alert err mt-1">
                        <span className="mono">{e.step}</span>
                        <span className="mono ml-2 text-slate-400">{e.errorName ?? ""}</span>
                        <div className="mono mt-0.5 break-all text-[10.5px] text-slate-400">
                          {JSON.stringify(e.response ?? {})}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
        <div className="card producer-topology-card p-3">
          <TopologyGraph
            topic={form.topic}
            partitions={(cluster?.partitions ?? []).map((p) => ({
              p: p.partition, leader: p.leader, replicas: p.replicas, isr: p.isr,
            }))}
            brokers={cluster?.brokers ?? []}
            activePartition={activePartition}
            actualRoute={lastSentPartition === activePartition}
            actualBrokerId={lastSentPartition === activePartition
              ? events.find((e) => e.apiName === "Produce" && e.direction === "request")?.broker?.id : undefined}
            onSelectPartition={(partition) => setForm((f) => ({ ...f, partition: String(partition) }))}
          />
        </div>
      </div>

      {/* 右：结果列 */}
      <div className="producer-column col-right">
      <div className="card h-fit p-3">
        <div className="mb-2 flex items-center justify-between">
          <span className="card-title">请求 / 响应详情</span>
          <span className="flex gap-1">
            <span className="badge run">Request</span>
            <span className="badge mute">Response</span>
          </span>
        </div>
        {events.length === 0 ? (
          <div className="space-y-1.5">
            <div className="mono rounded-lg bg-black/20 px-3 py-2.5 text-[11px] leading-relaxed text-slate-400">
              左侧填 Key / Value（空即随机），点 Send 后这里展示真实报文。
            </div>
            {[
              ["API", "Produce (0)"],
              ["Topic", form.topic || "未填写"],
              ["Partition", "发送后确定"],
              ["Broker", "由 Metadata Leader 决定"],
              ["Offset", "发送后回填"],
            ].map(([k, v]) => (
              <div key={k} className="flex items-center justify-between gap-3 py-[3px] text-[12px]">
                <span className="shrink-0 text-[var(--text-dim)]">{k}</span>
                <span className="mono min-w-0 truncate text-right text-slate-700">{v}</span>
              </div>
            ))}
          </div>
        ) : (
          <>
            <Inspector events={events} stepIndex={selected} preferResponse={preferRes} />
            <div className="mt-3 border-t border-[var(--border-soft)] pt-2">
              <div className="card-title mb-1">Result</div>
              {doneEv ? (
                <>
                  <KV k="Partition" v={String(doneResp["partition"])} mono />
                  <KV k="Offset" v={String(doneResp["offset"])} mono />
                  <KV k="Total" v={`${String(doneResp["totalMs"])} ms`} mono />
                </>
              ) : failEv ? (
                <div className="alert err">
                  {String(failResp["error"] ?? sendError ?? "failed")}
                </div>
              ) : (
                <div className="mono text-[12px] text-slate-500">running…</div>
              )}
            </div>
          </>
        )}
        <div className="mt-2 flex items-center gap-2">
          {doneEv && <span className="mono text-[11px] text-slate-500">Offset: {String(doneResp["offset"] ?? "")}</span>}
        </div>
      </div>
      <div className="space-y-3">
        <div className="card p-3">
          <div className="card-title mb-2">Produce Result</div>
          {doneEv ? (
            <>
              <KV k="Status" v="SUCCESS" />
              <KV k="Topic" v={String(form.topic)} mono />
              <KV k="Partition" v={String(doneResp["partition"] ?? "N/A")} mono />
              <KV k="Offset" v={String(doneResp["offset"] ?? "N/A")} mono />
              {Array.isArray(doneResp["offsets"]) && (
                <KV k="Offsets" v={(doneResp["offsets"] as unknown[]).join(", ")} mono />
              )}
              <KV k="Timestamp" v={fmtTime(doneEv.timestamp)} mono />
              <KV k="Message Size" v={valueSize != null && !Number.isNaN(valueSize) ? `${valueSize} B${mode === "batch" ? ` × ${batchCount}` : ""}` : "N/A"} mono />
              <KV k="Broker" v={(() => {
                const b = events.find((e) => e.stepIndex === 6)?.broker;
                return b ? `${b.host}:${b.port}` : "N/A";
              })()} mono />
              <KV k="Latency" v={`${String(doneResp["totalMs"] ?? "N/A")} ms`} mono />
              <div className="mt-2 flex gap-1.5">
                <button
                  onClick={() => {
                    const t = `Topic=${form.topic} Partition=${String(doneResp["partition"] ?? "")} Offset=${String(doneResp["offset"] ?? "")}`;
                    try { navigator.clipboard.writeText(t); setCopied(true); setTimeout(() => setCopied(false), 1500); }
                    catch { /* ignore */ }
                  }}
                  className="btn btn-ghost btn-sm">
                  {copied ? "Copied ✓" : "Copy Offset"}
                </button>
              </div>
            </>
          ) : failEv || sendFail || sendError ? (
            <>
              <KV k="Status" v="FAILED" />
              {(() => {
                const name = failEv?.errorName ?? "";
                const code = failEv?.errorCode;
                const hint: Record<string, string> = {
                  NOT_LEADER_FOR_PARTITION: "Leader 已迁移：已自动 Metadata Refresh 重试仍失败，检查分区 Leader",
                  LEADER_NOT_AVAILABLE: "分区暂无 Leader：等待选主后重试",
                  UNKNOWN_TOPIC_OR_PARTITION: "Topic 或 Partition 不存在：检查 Topic 名与 Metadata",
                  REQUEST_TIMED_OUT: "请求超时：检查 broker 负载与 request.timeout.ms",
                  NETWORK_EXCEPTION: "网络异常：检查 Bootstrap 与 broker 端口连通性",
                  SASL_AUTHENTICATION_FAILED: "SASL 鉴权失败：检查用户名口令与机制",
                };
                return (
                  <>
                    {code != null && <KV k="Error Code" v={String(code)} mono />}
                    {name ? <KV k="Error" v={name} mono /> : null}
                    <div className="alert err mt-1">
                      {hint[name] ?? String((failResp["error"] as string | undefined) ?? sendFail ?? sendError ?? "failed")}
                    </div>
                    {(() => {
                      const b = failEv?.broker;
                      return b ? <KV k="Broker" v={`${b.host}:${b.port}`} mono /> : null;
                    })()}
                  </>
                );
              })()}
            </>
          ) : (
            <div className="py-2 text-[11.5px] text-slate-500">发送后显示真实 Topic / Partition / Offset，失败显示真实 Error Code。</div>
          )}
        </div>
        <div className="card p-3">
          <div className="mb-2 flex items-center justify-between">
            <span className="card-title">发送记录 <span className="font-normal text-[var(--text-faint)]">· 本地保留</span></span>
            <button onClick={() => { setRecent([]); try { localStorage.removeItem("kcl-recent"); } catch { /* ignore */ } }}
              className="text-[11px] text-slate-500 hover:text-slate-300">清空</button>
          </div>
          {recent.length === 0 ? (
            <div className="py-3 text-center text-[11.5px] text-slate-500">
              暂无发送记录。连接集群后点“发送消息”，成功后此处追加真实 Offset。
            </div>
          ) : (
            <div className="space-y-1.5">
              {recent.map((r) => {
                const preview = (() => {
                  if (r.value == null) return null;
                  const v = r.value.length > 80 ? `${r.value.slice(0, 80)}…` : r.value;
                  try {
                    const j = JSON.parse(r.value);
                    return <pre className="codebox mt-1 max-h-16">{JSON.stringify(j, null, 1).slice(0, 200)}</pre>;
                  } catch { return <div className="mono mt-1 break-all text-[10.5px] text-slate-400">{v}</div>; }
                })();
                return (
                  <div key={r.id} onClick={() => setDetailId(r.id)} title="点击查看完整记录"
                    className="cursor-pointer rounded-md bg-black/20 px-1.5 py-1 text-[11px] hover:bg-slate-900/[0.04]">
                    <div className="flex w-full items-center gap-1.5 text-left">
                      <span className={`inline-block h-1.5 w-1.5 shrink-0 rounded-full ${r.status === "SUCCESS" ? "bg-emerald-400" : r.status === "ERROR" ? "bg-red-400" : "bg-sky-400 pulsing"}`} />
                      <span className="mono truncate text-slate-300">{r.topic}</span>
                      <span className="mono shrink-0 text-slate-500">{r.partition >= 0 ? `P${r.partition}` : "…"}</span>
                      <span className="mono shrink-0 text-slate-500">{r.offset >= 0 ? `Offset ${r.offset}` : ""}</span>
                      <span className="mono ml-auto shrink-0 text-slate-600">{r.time}{r.size != null ? ` · ${r.size}B` : ""}</span>
                    </div>
                    {preview}
                    {r.status === "SUCCESS" && r.value != null && (
                      <div className="mt-1 flex gap-1.5" onClick={(e) => e.stopPropagation()}>
                        <button
                          onClick={() => setDetailId(r.id)}
                          className="btn btn-ghost btn-sm !px-1.5 !py-[1px] !text-[10px]">详情</button>
                        <button
                          onClick={() => void resendRecord(r)}
                          disabled={readonly} title={readonly ? "只读巡检模式下禁止重发消息" : undefined}
                          className="btn btn-ghost btn-sm !px-1.5 !py-[1px] !text-[10px]">Resend</button>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {(() => {
          const r = recent.find((x) => x.id === detailId);
          if (!r) return null;
          const coord = `Topic=${r.topic} Partition=${r.partition} Offset=${r.offset}`;
          const copyBtn = (tag: string, text: string, label = "复制") => (
            <button onClick={() => void copyText(tag, text)}
              className="btn btn-ghost btn-sm shrink-0 !px-2 !py-px !text-[10px]">
              {copiedWhat === tag ? "已复制 ✓" : label}
            </button>
          );
          return (
            <div className="fixed inset-0 z-50">
              <div className="absolute inset-0 bg-black/50" onClick={() => setDetailId(null)} />
              <aside className="absolute inset-y-0 right-0 flex w-[420px] max-w-[94vw] flex-col border-l border-[var(--border)] bg-white shadow-2xl">
                <div className="flex items-center gap-2 border-b border-[var(--border-soft)] px-4 py-3">
                  <span className="text-[13px] font-bold text-slate-800">发送记录详情</span>
                  <StatusBadge status={r.status === "SUCCESS" ? "success" : r.status === "ERROR" ? "error" : "pending"} />
                  <button onClick={() => setDetailId(null)} title="关闭（Esc）"
                    className="ml-auto rounded-md px-2 py-1 text-[14px] text-slate-500 hover:bg-slate-900/[0.04] hover:text-slate-700">✕</button>
                </div>
                <div className="min-h-0 flex-1 space-y-3 overflow-y-auto px-4 py-3">
                  <div className="rounded-lg border border-[var(--border-soft)]">
                    <KV k="Topic" v={r.topic} mono />
                    <div className="border-t border-[var(--border-soft)]" />
                    <KV k="Partition" v={r.partition >= 0 ? String(r.partition) : "—"} mono />
                    <div className="border-t border-[var(--border-soft)]" />
                    <KV k="Offset" v={r.offset >= 0 ? String(r.offset) : "—"} mono />
                    <div className="border-t border-[var(--border-soft)]" />
                    <KV k="时间" v={r.time} mono />
                    <div className="border-t border-[var(--border-soft)]" />
                    <KV k="耗时" v={r.latency || "—"} mono />
                    <div className="border-t border-[var(--border-soft)]" />
                    <KV k="大小" v={r.size != null ? `${r.size} B` : "—"} mono />
                  </div>
                  <div>
                    <div className="mb-1 flex items-center justify-between">
                      <span className="lbl !mb-0">Key</span>
                      {r.key != null && r.key !== "" && copyBtn("key", r.key)}
                    </div>
                    <pre className="codebox max-h-32 whitespace-pre-wrap break-all">{r.key ?? "(空)"}</pre>
                  </div>
                  <div>
                    <div className="mb-1 flex items-center justify-between">
                      <span className="lbl !mb-0">Value</span>
                      {r.value != null && r.value !== "" && copyBtn("value", r.value)}
                    </div>
                    <pre className="codebox max-h-64 whitespace-pre-wrap break-all">{r.value ?? "(空)"}</pre>
                  </div>
                </div>
                <div className="flex flex-wrap items-center gap-2 border-t border-[var(--border-soft)] px-4 py-3">
                  {copyBtn("coord", coord, "复制坐标")}
                  {r.status === "SUCCESS" && r.value != null && (
                    <button onClick={() => void resendRecord(r)} disabled={readonly || sending} title={readonly ? "只读巡检模式下禁止重发消息" : undefined}
                      className="btn btn-ghost btn-sm">再次发送相同内容</button>
                  )}
                </div>
              </aside>
            </div>
          );
        })()}

      </div>
      </div>
    </div>
    </div>
    </div>
  );
}
