import { useCallback, useEffect, useRef, useState } from "react";
import { DEFAULT_PRODUCE, clusterQuery, type ClusterStatus, type ProduceForm } from "../api/producer";
import { useConn } from "../conn";
import { reportConn } from "../App";
import { Inspector } from "../components/Inspector";
import { TopologyGraph, FullTimeline } from "../components/Topology";
import { TraceTimeline } from "../components/TraceTimeline";
import { KV, PageHeader, StatusBadge } from "../components/bits";
import { TopicPicker } from "../components/TopicPicker";
import { useProducerSession } from "../api/producer";
import { friendlyWriteError, useAuth } from "../auth";
import { useLab } from "../lab";
import { stepsFromEvents, fmtTime } from "../traceSteps";
import type { RecentMsg } from "../types";

const inputCls = "field mono";
const labelCls = "lbl";

async function getJSON<T>(path: string): Promise<T> {
  const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;
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

const recordCount = (record: RecentMsg) => record.count ?? Number(record.id.match(/-b(\d+)$/)?.[1] ?? 1);

export function ProducerPage() {
  const { session, events, running, sendError, wsDown, send, reset } = useProducerSession();
  const { conn, health, probe: probeConnection } = useConn();
  const recentKey = `kcl-recent:${conn.id}`;
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
  const [sent, setSent] = useState<{ topic: string; count: number; bytes: number } | null>(null);
  const [selected, setSelected] = useState(6);
  const [recent, setRecent] = useState<RecentMsg[]>(() => {
    try {
      const raw = localStorage.getItem(recentKey);
      return raw ? JSON.parse(raw) as RecentMsg[] : [];
    } catch { return []; }
  });
  const activeRecentId = useRef<string | null>(null);
  const [mode, setMode] = useState<"single" | "batch">("single");
  const [batchCount, setBatchCount] = useState(10);
  const [sending, setSending] = useState(false);
  const [showConnDetail, setShowConnDetail] = useState(false);
  const [topicOptions, setTopicOptions] = useState<string[]>([]);
  const [topicLoading, setTopicLoading] = useState(false);
  const [topicError, setTopicError] = useState(false);
  const topicRequestSeq = useRef(0);
  const [detailId, setDetailId] = useState<string | null>(null);
  const [copiedWhat, setCopiedWhat] = useState<string | null>(null);

  const copyText = async (tag: string, text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedWhat(tag);
      window.setTimeout(() => setCopiedWhat((c) => (c === tag ? null : c)), 1500);
    } catch { /* ignore */ }
  };
  const [cluster, setCluster] = useState<ClusterStatus | null>(null);
  const [clusterErr, setClusterErr] = useState<string | null>(null);
  const [probing, setProbing] = useState(false);
  const probeSeq = useRef(0);
  const [sendFail, setSendFail] = useState<string | null>(null);
  const [midTab, setMidTab] = useState<"trace" | "timeline">("trace");

  useEffect(() => {
    reset();
    setSent(null);
    setCluster(null);
    setClusterErr(null);
    setForm((current) => ({ ...current, topic: "", partition: "auto" }));
  }, [conn.id, reset]);

  const pushRecent = (r: RecentMsg) => setRecent((prev) => {
    const next = [r, ...prev].slice(0, 30);
    try { localStorage.setItem(recentKey, JSON.stringify(next)); } catch { /* ignore */ }
    return next;
  });

  useEffect(() => {
    let dead = false;
    let cached: RecentMsg[] = [];
    try {
      const raw = localStorage.getItem(recentKey);
      cached = raw ? JSON.parse(raw) as RecentMsg[] : [];
    } catch { /* ignore */ }
    setRecent(cached);
    activeRecentId.current = null;
    if (cached.length > 0) return;
    // 本集群没有浏览器记录时，从持久化 Session 补齐。
    getJSON<import("../types").Session[]>("/api/sessions").then((list) => {
      if (dead) return;
      const mapped: RecentMsg[] = list.filter((s) => s.bootstrap === conn.bootstrap).slice(0, 20).map((s) => ({
        id: s.id, topic: s.topic, partition: s.partition ?? -1,
        offset: s.offset ?? -1, latency: s.durationMs != null ? `${s.durationMs}ms` : "—",
        time: fmtTime(s.startTime), status: s.status === "success" ? "SUCCESS" : s.status === "error" ? "ERROR" : "RUNNING",
        traceId: "", events: [],
      }));
      if (mapped.length) {
        setRecent((current) => {
          if (current.length) return current;
          try { localStorage.setItem(recentKey, JSON.stringify(mapped)); } catch { /* ignore */ }
          return mapped;
        });
      }
    }).catch(() => undefined);
    return () => { dead = true; };
  }, [conn.id, conn.bootstrap, recentKey]);

  const steps = stepsFromEvents(events);
  const doneEv = events.find((e) => e.step === "session.done");
  const failEv = events.find((e) => e.step === "session.failed");
  const doneResp = (doneEv?.response ?? {}) as Record<string, unknown>;
  const failResp = (failEv?.response ?? {}) as Record<string, unknown>;
  const progressEv = [...events].reverse().find((e) => e.step === "produce.progress");
  const progressResp = (progressEv?.response ?? {}) as Record<string, unknown>;
  const offsetRange = doneResp["lastOffset"] != null && doneResp["lastOffset"] !== doneResp["offset"]
    ? `${String(doneResp["offset"])} → ${String(doneResp["lastOffset"])}`
    : String(doneResp["offset"] ?? "");

  // 本次发送完成 → 把最上面那条 RUNNING 的最近消息更新为真实结果
  useEffect(() => {
    if (!doneEv && !failEv) return;
    const ok = !!doneEv;
    const partition = Number(doneResp["partition"] ?? failEv?.partition ?? -1);
    const offset = Number(doneResp["offset"] ?? failResp["firstOffset"] ?? -1);
    const lastOffsetValue = doneResp["lastOffset"] ?? failResp["lastOffset"];
    const lastOffset = lastOffsetValue == null ? NaN : Number(lastOffsetValue);
    const latency = doneResp["totalMs"] != null ? `${doneResp["totalMs"]}ms` : "—";
    const traceId = (doneEv ?? failEv)?.traceId ?? "";
    const result = ok ? doneResp : failResp;
    const confirmedCount = Number(result[ok ? "records" : "sent"] ?? 0);
    const confirmedBytes = Number(result[ok ? "totalBytes" : "confirmedBytes"]);
    const batches = Number(result[ok ? "batches" : "completedBatches"]);
    const currentId = activeRecentId.current;
    if (!currentId) return;
    setRecent((prev) => {
      const i = prev.findIndex((r) => r.id === currentId);
      if (i < 0) return prev;
      const next = [...prev];
      next[i] = { ...next[i], status: ok ? "SUCCESS" : "ERROR",
                  partition: Number.isNaN(partition) ? next[i].partition : partition,
                  offset: Number.isNaN(offset) ? next[i].offset : offset,
                  lastOffset: Number.isNaN(lastOffset) ? next[i].lastOffset : lastOffset,
                  latency, traceId,
                  confirmedCount: Number.isFinite(confirmedCount) ? confirmedCount : undefined,
                  size: Number.isFinite(confirmedBytes) ? confirmedBytes : next[i].size,
                  sizeExact: Number.isFinite(confirmedBytes),
                  batches: Number.isFinite(batches) ? batches : undefined,
                  key: typeof result["sampleKey"] === "string" ? result["sampleKey"] as string : next[i].key,
                  value: typeof result["sampleValue"] === "string" ? result["sampleValue"] as string : next[i].value,
                  sampleKeyFormat: result["sampleKeyFormat"] === "hex" ? "hex" : "text",
                  sampleValueFormat: result["sampleValueFormat"] === "hex" ? "hex" : "text",
                  sampleTruncated: result["sampleTruncated"] === true,
                  error: !ok && typeof result["error"] === "string" ? result["error"] as string : undefined };
      try { localStorage.setItem(recentKey, JSON.stringify(next.slice(0, 30))); } catch { /* ignore */ }
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
  const lastSentPartition = doneEv?.topic === sent?.topic && doneResp["partition"] != null
    ? Number(doneResp["partition"]) : undefined;
  const autoCandidate = cluster?.partitions.find((p) => p.leader >= 0) ?? cluster?.partitions[0];
  const activePartition = form.partition !== "auto" ? Number(form.partition)
    : sent?.topic === form.topic ? lastSentPartition ?? autoCandidate?.partition : autoCandidate?.partition;
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
    const seq = ++probeSeq.current;
    setProbing(true);
    setClusterErr(null);
    try {
      if (!form.topic.trim()) {
        setCluster(null);
        const status = await probeConnection(conn.id);
        if (seq !== probeSeq.current) return;
        if (!status.connected) setClusterErr(status.error ?? "连接失败");
        reportConn({ ok: status.connected === true, bootstrap: conn.bootstrap,
          detail: status.connected ? `${status.brokerCount ?? 0} brokers` : status.error ?? "连接失败" });
        return;
      }
      const f = { ...form, ...connFields() };
      const d = await getJSON<ClusterStatus>(clusterQuery(f, f.topic));
      if (seq !== probeSeq.current) return;
      setCluster(d);
      reportConn({ ok: true, bootstrap: f.bootstrap, detail: `${d.brokerCount} brokers` });
    } catch (e) {
      if (seq !== probeSeq.current) return;
      setCluster(null);
      const msg = e instanceof Error ? e.message : String(e);
      setClusterErr(msg);
      reportConn({ ok: false, bootstrap: conn.bootstrap, detail: msg.slice(0, 80) });
    } finally {
      if (seq === probeSeq.current) setProbing(false);
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
  useEffect(() => {
    const timer = window.setTimeout(() => { void probe(); }, 350);
    return () => { window.clearTimeout(timer); ++probeSeq.current; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [conn.bootstrap, conn.securityProtocol, conn.saslMechanism, conn.saslUsername, conn.saslPassword, form.topic]);
  // 使用集群完整 Topic 列表；重新打开时刷新，避免创建 Topic 后候选项长期过期。
  const loadTopics = useCallback(async () => {
    const seq = ++topicRequestSeq.current;
    setTopicLoading(true);
    setTopicError(false);
    try {
      const data = await getJSON<{ topics?: { topic: string }[] }>(`/api/clusters/${conn.id}/topics`);
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
    setSent({ topic: payload.topic, count: 1,
      bytes: new TextEncoder().encode(payload.key).length + payloadSize });
    const id = `r-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
    activeRecentId.current = id;
    pushRecent({
      id, topic: payload.topic,
      partition: payload.partition === "auto" ? -1 : Number(payload.partition),
      offset: -1, latency: "…", time: fmtTime(Date.now()),
      status: "RUNNING" as const, traceId: "", events: [],
      key: payload.key, value: payload.value,
      count: 1,
      size: new TextEncoder().encode(payload.key).length + payloadSize,
    });
    await send({ ...payload, ...connFields() });
  };

  const resendRecord = async (r: RecentMsg) => {
    if (recordCount(r) > 1) return;
    setSendFail(null);
    setSending(true);
    try {
      const payload = {
        ...form, topic: r.topic, partition: r.partition >= 0 ? String(r.partition) : "auto",
        key: r.key ?? form.key, value: r.value ?? form.value, keyHex: "", valueHex: "",
      };
      setForm(payload);
      await doOneSend(payload);
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
      const n = batchCount;
      if (!Number.isInteger(n) || n < 2 || n > 10000) {
        setSendFail("批量消息条数请输入 2–10000 之间的整数");
        return;
      }
      // 100 条以内保留单次 Produce 请求；更大的批量交给后端分批生成和确认。
      const messages = n <= 100 ? Array.from({ length: n }, () => {
        const key = form.key || `k-${randStr(6)}`;
        return buildKV(key, form.value || randStr(32));
      }) : [];
      const bytes = messages.length
        ? messages.reduce((sum, message) => sum + new TextEncoder().encode(message.key).length + new TextEncoder().encode(message.value).length, 0)
        : n * ((form.key ? new TextEncoder().encode(form.key).length : 8) + (form.value ? new TextEncoder().encode(form.value).length : 32));
      setSent({ topic: form.topic, count: n,
        bytes });
      const id = `r-${Date.now()}-b${n}`;
      activeRecentId.current = id;
      pushRecent({
        id, topic: form.topic,
        partition: form.partition === "auto" ? -1 : Number(form.partition),
        offset: -1, latency: "…", time: fmtTime(Date.now()),
        status: "RUNNING" as const, traceId: "", events: [],
        key: messages[0]?.key ?? form.key, value: messages[0]?.value ?? form.value,
        count: n,
        size: bytes,
      });
      await send({ ...form, ...connFields(), messages, messageCount: n > 100 ? n : 0 });
    } catch (e) {
      setSendFail(friendlyWriteError(e));
      const currentId = activeRecentId.current;
      if (currentId) setRecent((prev) => {
        const next = prev.map((r) => r.id === currentId ? { ...r, status: "ERROR" as const, latency: "—",
          confirmedCount: 0, size: undefined, sizeExact: false,
          error: e instanceof Error ? e.message : String(e) } : r);
        try { localStorage.setItem(recentKey, JSON.stringify(next.slice(0, 30))); } catch { /* ignore */ }
        return next;
      });
      activeRecentId.current = null;
    } finally {
      setSending(false);
    }
  };

  const connected = !!cluster?.connected || health[conn.id]?.connected === true;
  const statusBadge = running ? "pending"
    : failEv || sendError || sendFail || session?.status === "error" ? "error"
    : doneEv || session?.status === "success" ? "success" : "pending";
  const statusText = running ? "发送中"
    : failEv || sendError || sendFail || session?.status === "error" ? "失败"
    : doneEv || session?.status === "success" ? "成功" : "待发送";

  return (
    <div className="producer-page">
      <PageHeader title="生产消息"
      right={<><StatusBadge status={statusBadge} text={statusText} />{doneEv && <span className="producer-header-offset mono">Offset {offsetRange}</span>}</>} />
    <div className="layout-grid producer-layout">
      {/* 左：配置（连接 / 消息 / 发送模式分区展示） */}
      <div className="producer-column producer-config-column">
        <div className="card producer-connection-card p-3">
          <div className="mb-2 flex items-center gap-2">
            <span className="card-title !mb-0">连接</span>
            {(cluster?.brokerCount ?? health[conn.id]?.brokerCount) != null &&
              <span className="badge ok producer-broker-count">{cluster?.brokerCount ?? health[conn.id]?.brokerCount} 个 Broker</span>}
            <button onClick={probe} disabled={probing} title="刷新分区"
              className="btn btn-ghost btn-sm ml-auto">{probing ? "刷新中…" : "刷新连接"}</button>
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
                className="producer-detail-toggle mt-2" aria-expanded={showConnDetail}>
                {showConnDetail ? "收起 Broker 详情" : "查看 Broker 详情"}<span className="producer-chevron" aria-hidden="true" />
              </button>
              {showConnDetail && (
                <div className="producer-broker-detail mono mt-2 space-y-[2px] p-2">
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
            <span className="card-title !mb-0">发送目标</span>
            <span className="badge mute">{mode === "batch" ? `批量 · ${batchCount} 条` : "单条消息"}</span>
          </div>
          <TopicPicker value={form.topic} options={topicOptions} loading={topicLoading} error={topicError}
            onRefresh={() => { void loadTopics(); }}
            onChange={(topic) => setForm((current) => ({ ...current, topic, partition: "auto" }))} />
          <details className="producer-payload mt-3">
            <summary>
              <span className="producer-payload-summary">
                <strong>自定义消息内容</strong>
                <small>{form.key || form.value
                  ? `${form.key ? "已指定 Key" : "随机 Key"} · ${form.value ? `已指定 Value（${valueSize} B）` : "随机 32 B Value"}`
                  : "默认随机生成 Key 和 32 B Value"}</small>
              </span>
              <span className="producer-chevron" aria-hidden="true" />
            </summary>
            <div className="producer-payload-fields">
              <div><label className={labelCls} htmlFor="producer-key">消息 Key</label>
                <input id="producer-key" className={`${inputCls} mono`} value={form.key}
                  placeholder="留空则随机生成"
                  onChange={(e) => setForm({ ...form, key: e.target.value })} /></div>
              <div className="mt-2">
                <div className="mb-1 flex items-baseline justify-between">
                  <label className={labelCls} htmlFor="producer-value" style={{ marginBottom: 0 }}>消息 Value</label>
                  <span className="mono text-[10.5px] text-slate-600">{form.value ? `${valueSize} B` : "默认 32 B"}</span>
                </div>
                <textarea id="producer-value" rows={3} spellCheck={false}
                  placeholder="留空则生成 32 B 随机内容"
                  className={`${inputCls} mono producer-value-input resize-y`} value={form.value}
                  onChange={(e) => setForm({ ...form, value: e.target.value })} />
              </div>
              {mode === "batch" && (form.key || form.value) && <p className="producer-payload-help">填写的内容会用于每条消息。</p>}
            </div>
          </details>
          <details className="producer-advanced mt-3">
            <summary>高级参数<span className="producer-chevron" aria-hidden="true" /></summary>
          <div className="mt-2 grid grid-cols-2 gap-2">
            <div className="min-w-0"><label className={labelCls} htmlFor="producer-partition">目标分区</label>
              <select id="producer-partition" className={inputCls} value={form.partition}
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
            <div className="min-w-0"><label className={labelCls} htmlFor="producer-acks">Acks</label>
              <select id="producer-acks" className={inputCls} value={form.acks}
                onChange={(e) => setForm({ ...form, acks: e.target.value })}>
                <option value="all">all</option><option value="1">1</option>
              </select></div>
          </div>
          <div className="mt-2 grid grid-cols-2 gap-2">
            <div><label className={labelCls} htmlFor="producer-retries">最多尝试次数</label>
              <input id="producer-retries" type="number" min={1} max={20} className={`${inputCls} mono`} value={form.retries}
                onChange={(e) => setForm({ ...form, retries: Math.min(20, Math.max(1, Number(e.target.value))) })} /></div>
            <div><label className={labelCls} htmlFor="producer-timeout">请求超时 (ms)</label>
              <input id="producer-timeout" type="number" min={1000} max={120000} className={`${inputCls} mono`} value={form.requestTimeoutMs}
                onChange={(e) => setForm({ ...form, requestTimeoutMs: Math.min(120000, Math.max(1000, Number(e.target.value))) })} /></div>
          </div>
          </details>
        </div>

        <div className="card producer-send-card p-3">
          <div className="card-title mb-2">发送方式</div>
          <div className="producer-mode-switch" role="group" aria-label="发送方式">
            {(["single", "batch"] as const).map((m) => (
              <button key={m} type="button" onClick={() => setMode(m)} aria-pressed={mode === m}
                className={`producer-mode-option ${mode === m ? "is-active" : ""}`}>
                {m === "single" ? "单条消息" : "批量消息"}
              </button>
            ))}
          </div>
          {mode === "batch" && (
            <div className="producer-batch-row mt-3"><label className={labelCls} htmlFor="producer-batch-count" style={{ marginBottom: 0 }}>消息条数</label>
              <input id="producer-batch-count" type="number" min={2} max={10000} step={1} className={`${inputCls} mono`} value={batchCount}
                onChange={(e) => setBatchCount(Number(e.target.value))} /></div>
          )}
          <button onClick={() => requireLab(() => { void onSend(); })} disabled={readonly || running || sending || !form.topic.trim()} title={readonly ? "只读模式下禁止发送消息" : !form.topic.trim() ? "请先填写 Topic" : undefined}
            className={`btn btn-send producer-send-button mt-3 ${!running && !sending && (failEv || sendError || sendFail) ? "is-failed" : ""}`}>
            {sending || running ? "正在发送…" : failEv || sendError || sendFail ? "重试发送" : mode === "batch" ? `发送 ${batchCount} 条` : "发送消息"}
          </button>
          {running && progressResp["sent"] != null && (
            <div className="producer-batch-progress" role="status">
              已确认 {String(progressResp["sent"])} / {String(progressResp["total"])} 条
              <span>第 {String(progressResp["batchIndex"])} / {String(progressResp["batchTotal"])} 批</span>
            </div>
          )}
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
              {form.partition === "auto" ? "自动 · 首个可用分区" : `固定 P${activePartition}`}
            </span>
          </div>
          <div className="mono mb-2 truncate text-[10.5px] text-slate-500" title={form.topic || "未填写 Topic"}>
            {form.topic || "未填写 Topic"}
          </div>
          <div className="producer-route-grid">
            <div className="producer-route-stat">
              <div className="text-[10px] text-[var(--text-faint)]">目标分区</div>
              <div className="mono mt-0.5 truncate text-[12px] font-semibold text-[var(--text)]">
                {activePartition == null ? "待获取" : `P${activePartition}${form.partition === "auto" ? lastSentPartition != null && sent?.topic === form.topic ? " · 上次命中" : " · 候选" : ""}`}
              </div>
            </div>
            <div className="producer-route-stat">
              <div className="text-[10px] text-[var(--text-faint)]">Leader</div>
              <div className="mono mt-0.5 truncate text-[12px] font-semibold text-[var(--text)]" title={previewLeader ? `${previewLeader.host}:${previewLeader.port}` : "当前没有 Leader"}>
                {previewLeader ? `Broker ${previewLeader.id}` : "暂无"}
              </div>
              {previewLeader && <div className="mono truncate text-[11px] text-slate-500">{previewLeader.host}:{previewLeader.port}</div>}
            </div>
            <div className="producer-route-stat">
              <div className="text-[10px] text-[var(--text-faint)]">副本</div>
              <div className="mono mt-0.5 text-[12px] font-semibold text-[var(--text)]">{previewPartition?.replicas.length ?? "—"}</div>
            </div>
            <div className="producer-route-stat">
              <div className="text-[10px] text-[var(--text-faint)]">ISR / 副本</div>
              <div className={`mono mt-0.5 text-[12px] font-semibold ${previewPartition && previewPartition.isr.length < previewPartition.replicas.length ? "text-amber-700" : "text-emerald-700"}`}>
                {previewPartition ? `${previewPartition.isr.length} / ${previewPartition.replicas.length}` : "—"}
              </div>
            </div>
          </div>
          <div className="producer-route-foot mono">Acks {form.acks}</div>
        </div>
      </div>

      {/* 中：Trace */}
      <div className="producer-column producer-observe-column min-w-0">
        <div className="card producer-trace-card px-3 pt-2">
          <div className="producer-trace-tabs">
            {([["trace", "发送追踪"], ["timeline", "时间线"]] as const).map(([k, t]) => (
              <button key={k} type="button" onClick={() => setMidTab(k)} aria-pressed={midTab === k}
                className={`producer-trace-tab ${midTab === k ? "is-active" : ""}`}>
                {t}
              </button>
            ))}
          </div>
          <div className="py-3">
            {midTab === "trace" && (
              events.length === 0 ? (
                <div className="producer-trace-empty">
                  <span className="producer-empty-mark" aria-hidden="true">↗</span>
                  <strong>{connected ? "等待发送消息" : "等待集群连接"}</strong>
                  <span>{connected ? "发送后显示链路与耗时" : "等待连接集群"}</span>
                </div>
              ) : (
                <TraceTimeline steps={steps} selected={selected} onSelect={setSelected}
                  total={doneResp["totalMs"] != null ? `${doneResp["totalMs"]} ms` : undefined} />
              )
            )}
            {midTab === "timeline" && <FullTimeline events={events} />}
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
            actualRoute={sent?.topic === form.topic && lastSentPartition === activePartition}
            actualBrokerId={sent?.topic === form.topic && lastSentPartition === activePartition
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
          {events.length > 0 && <span className="flex gap-1">
            <span className="badge run">Request</span>
            <span className="badge mute">Response</span>
          </span>}
        </div>
        {events.length === 0 ? (
          <div className="space-y-1.5">
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
              <div className="card-title mb-1">响应结果</div>
              {doneEv ? (
                <>
                  <KV k="Partition" v={String(doneResp["partition"])} mono />
                  <KV k="首条 / 末条 Offset" v={offsetRange} mono />
                  <KV k="已确认" v={`${String(doneResp["records"] ?? 1)} 条`} mono />
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
      </div>
      <div className="space-y-3">
        <div className="card p-3">
          <div className="card-title mb-2">发送结果</div>
          {doneEv ? (
            <>
              <KV k="Status" v="SUCCESS" />
              <KV k="Topic" v={sent?.topic ?? "N/A"} mono />
              <KV k="Partition" v={String(doneResp["partition"] ?? "N/A")} mono />
              <KV k="首条 / 末条 Offset" v={offsetRange} mono />
              <KV k="已确认" v={`${String(doneResp["records"] ?? sent?.count ?? 1)} 条`} mono />
              {Array.isArray(doneResp["offsets"]) && (
                <KV k="Offsets" v={(doneResp["offsets"] as unknown[]).join(", ")} mono />
              )}
              <KV k="Timestamp" v={fmtTime(doneEv.timestamp)} mono />
              <KV k="消息数 / 总大小" v={sent ? `${String(doneResp["records"] ?? sent.count)} 条 / ${sent.count > 100 ? "约 " : ""}${sent.bytes} B` : "N/A"} mono />
              <KV k="Broker" v={(() => {
                const b = events.find((e) => e.stepIndex === 6)?.broker;
                return b ? `${b.host}:${b.port}` : "N/A";
              })()} mono />
              <KV k="Latency" v={`${String(doneResp["totalMs"] ?? "N/A")} ms`} mono />
              <div className="mt-2 flex gap-1.5">
                <button
                  onClick={() => void copyText("result", `Topic=${sent?.topic ?? ""} Partition=${String(doneResp["partition"] ?? "")} Offset=${offsetRange}`)}
                  className="btn btn-ghost btn-sm">
                  {copiedWhat === "result" ? "已复制 ✓" : "复制消息坐标"}
                </button>
              </div>
            </>
          ) : failEv || sendFail || sendError ? (
            <>
              <KV k="Status" v="FAILED" />
              {failResp["requested"] != null && <KV k="已确认 / 计划" v={`${String(failResp["sent"] ?? 0)} / ${String(failResp["requested"])} 条`} mono />}
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
            <div className="producer-result-empty">尚无发送结果</div>
          )}
        </div>
        <div className="card p-3">
          <div className="mb-2 flex items-center justify-between">
            <span className="card-title">发送记录 <span className="font-normal text-[var(--text-faint)]">· 最近 {recent.length} 条</span></span>
            <button onClick={() => { setRecent([]); try { localStorage.removeItem(recentKey); } catch { /* ignore */ } }}
              disabled={recent.length === 0} className="btn btn-ghost btn-sm">清空</button>
          </div>
          {recent.length === 0 ? (
            <div className="producer-result-empty">暂无发送记录</div>
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
                  <div key={r.id} className="producer-history-item">
                    <button type="button" onClick={() => setDetailId(r.id)} title="查看完整记录"
                      className="producer-history-open">
                      <span className={`inline-block h-1.5 w-1.5 shrink-0 rounded-full ${r.status === "SUCCESS" ? "bg-emerald-400" : r.status === "ERROR" ? "bg-red-400" : "bg-sky-400 pulsing"}`} />
                      <span className="producer-history-main mono">{r.topic}</span>
                      {recordCount(r) > 1 && <span className="badge mute">{r.confirmedCount != null && r.status !== "RUNNING" ? `已确认 ${r.confirmedCount.toLocaleString()} / ${recordCount(r).toLocaleString()}` : `计划 ${recordCount(r).toLocaleString()} 条`}</span>}
                      <span className="producer-history-coordinate mono">{r.partition >= 0 ? `P${r.partition}` : "…"}{r.offset >= 0 ? ` · ${r.offset}${r.lastOffset != null && r.lastOffset > r.offset ? `→${r.lastOffset}` : ""}` : ""}</span>
                      <span className="producer-history-time mono">{r.time}</span>
                    </button>
                    <div className="producer-history-preview">{preview}</div>
                    {r.status === "SUCCESS" && r.value != null && recordCount(r) === 1 && (
                      <div className="producer-history-actions">
                        <button
                          onClick={() => requireLab(() => { void resendRecord(r); })}
                          disabled={readonly || sending || running} title={readonly ? "只读模式下禁止重发消息" : undefined}
                          className="btn btn-ghost btn-sm">再次发送</button>
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
          const coord = `Topic=${r.topic} Partition=${r.partition} Offset=${r.offset}${r.lastOffset != null && r.lastOffset > r.offset ? `→${r.lastOffset}` : ""}`;
          const requested = recordCount(r);
          const confirmed = r.confirmedCount;
          const sampleAvailable = r.status !== "RUNNING" && confirmed !== 0 && (confirmed != null && confirmed > 0
            ? r.key != null || r.value != null
            : r.key !== "" || r.value !== "") && (r.key != null || r.value != null);
          const sampleText = (value: string | undefined, format?: "text" | "hex") =>
            value == null ? "未记录" : value === "" ? "空内容" : format === "hex" ? `0x${value}` : value;
          const copyBtn = (tag: string, text: string, label = "复制") => (
            <button onClick={() => void copyText(tag, text)}
              className="btn btn-ghost btn-sm shrink-0">
              {copiedWhat === tag ? "已复制 ✓" : label}
            </button>
          );
          return (
            <div className="fixed inset-0 z-50" role="presentation">
              <div className="absolute inset-0 bg-slate-900/40" onClick={() => setDetailId(null)} />
              <aside className="producer-record-drawer" role="dialog" aria-modal="true" aria-label="发送记录详情">
                <div className="producer-record-header">
                  <div>
                    <div className="producer-record-eyebrow">PRODUCE / RECORD</div>
                    <h2>发送记录详情</h2>
                  </div>
                  <StatusBadge status={r.status === "SUCCESS" ? "success" : r.status === "ERROR" ? "error" : "pending"} />
                  <button onClick={() => setDetailId(null)} title="关闭（Esc）" aria-label="关闭详情"
                    className="producer-record-close">×</button>
                </div>
                <div className="producer-record-content">
                  <div className="producer-record-identity">
                    <span>发送目标</span>
                    <strong title={r.topic}>{r.topic}</strong>
                    <small>{r.partition >= 0 ? `Partition ${r.partition}` : "分区待确认"}</small>
                  </div>
                  <div className="producer-record-summary">
                    <div><span>已确认</span><strong>{confirmed != null ? confirmed.toLocaleString() : "—"}</strong><small>条消息</small></div>
                    <div><span>计划发送</span><strong>{requested.toLocaleString()}</strong><small>条消息</small></div>
                    <div><span>完成批次</span><strong>{r.batches != null ? r.batches.toLocaleString() : "—"}</strong><small>批</small></div>
                  </div>
                  {confirmed == null && <p className="producer-record-hint">此条旧记录未保存实际确认数量，无法据此核对发送总数。</p>}
                  {r.error && <div className="producer-record-error">{r.error}</div>}
                  <section className="producer-record-section">
                    <h3>写入位置</h3>
                    <div className="producer-record-offsets">
                      <div><span>首条 Offset</span><strong>{r.offset >= 0 ? r.offset.toLocaleString() : "—"}</strong></div>
                      <span className="producer-record-arrow" aria-hidden="true">→</span>
                      <div><span>末条 Offset</span><strong>{r.lastOffset != null && r.lastOffset >= 0 ? r.lastOffset.toLocaleString() : r.offset >= 0 ? r.offset.toLocaleString() : "—"}</strong></div>
                    </div>
                    <p className="producer-record-hint">首末位置来自 Broker 确认；若有其他生产者同时写入，差值不等于本次条数。</p>
                  </section>
                  <section className="producer-record-section">
                    <h3>运行信息</h3>
                    <dl className="producer-record-facts">
                      <div><dt>记录时间</dt><dd>{r.time}</dd></div>
                      <div><dt>总耗时</dt><dd>{r.latency || "—"}</dd></div>
                      <div><dt>{r.sizeExact ? "Key + Value" : "数据量估算"}</dt><dd>{r.size != null ? `${r.size.toLocaleString()} B` : "—"}</dd></div>
                    </dl>
                  </section>
                  <section className="producer-record-section">
                    <div className="producer-record-section-title">
                      <h3>首条消息样本</h3>
                      <span>仅预览，不代表整批消息</span>
                    </div>
                    {sampleAvailable ? <div className="producer-record-samples">
                      <div><div className="producer-record-sample-label"><span>KEY {r.sampleKeyFormat === "hex" ? "/ HEX" : ""}</span>{r.key && copyBtn("key", r.key)}</div><pre>{sampleText(r.key, r.sampleKeyFormat)}</pre></div>
                      <div><div className="producer-record-sample-label"><span>VALUE {r.sampleValueFormat === "hex" ? "/ HEX" : ""}</span>{r.value && copyBtn("value", r.value)}</div><pre>{sampleText(r.value, r.sampleValueFormat)}</pre></div>
                      {r.sampleTruncated && <p className="producer-record-hint">样本仅展示前 256 字节。</p>}
                    </div> : <p className="producer-record-empty">{r.status === "RUNNING" ? "发送完成后显示首条消息样本" : "此条记录未保存消息样本"}</p>}
                  </section>
                </div>
                <div className="producer-record-footer">
                  {copyBtn("coord", coord, "复制坐标")}
                  {r.status === "SUCCESS" && r.value != null && requested === 1 && r.sampleValueFormat !== "hex" && r.sampleKeyFormat !== "hex" && !r.sampleTruncated && (
                    <button onClick={() => requireLab(() => { void resendRecord(r); })} disabled={readonly || sending || running} title={readonly ? "只读模式下禁止重发消息" : undefined}
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
  );
}
