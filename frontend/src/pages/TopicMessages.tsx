import { useEffect, useRef, useState } from "react";

const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;
const LIVE_CAP = 1000;
const SUMMARY_LEN = 120;

export interface BrowseMessage {
  topic: string; partition: number; offset: number;
  timestamp: number | null; timestampText: string | null;
  key: string | null; value: string | null;
  headers: { key: string; value: string | null }[];
  size: number;
}
interface BrowseResult {
  topic: string; total: number; hasMore: boolean;
  messages: BrowseMessage[];
  nextOffsets: Record<string, number>;
  highWatermarks: Record<string, number>;
  scanned: number; truncated: boolean;
}

type Mode = "latest" | "earliest" | "offset" | "timestamp";

const inputCls = "field mono";
const labelCls = "lbl";

function fmtSize(n: number) {
  if (n < 1024) return `${n} B`;
  if (n < 1048576) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1048576).toFixed(2)} MB`;
}

function summary(v: string | null) {
  if (v == null) return <span className="text-slate-600">null</span>;
  if (v === "") return <span className="text-slate-600">空</span>;
  const s = v.length > SUMMARY_LEN ? `${v.slice(0, SUMMARY_LEN)}…` : v;
  return <span>{s}</span>;
}

export function TopicMessages({ cid, topic, partitions }: { cid: string; topic: string; partitions: number[] }) {
  const [partition, setPartition] = useState("all");
  const [mode, setMode] = useState<Mode>("latest");
  const [offset, setOffset] = useState("");
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [limit, setLimit] = useState(100);
  const [keyQ, setKeyQ] = useState("");
  const [valQ, setValQ] = useState("");
  const [msgs, setMsgs] = useState<BrowseMessage[]>([]);
  const [nextOffsets, setNextOffsets] = useState<Record<string, number> | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [scanned, setScanned] = useState(0);
  const [truncated, setTruncated] = useState(false);
  const [busy, setBusy] = useState(false);
  const [moreBusy, setMoreBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [emptyNote, setEmptyNote] = useState<string | null>(null);
  const [sel, setSel] = useState<number | null>(null);
  const [viewJson, setViewJson] = useState(true);
  const [copied, setCopied] = useState<string | null>(null);
  // live
  const [live, setLive] = useState<"off" | "run" | "pause">("off");
  const liveTimer = useRef<number | null>(null);
  const liveCursor = useRef<Record<string, number> | null>(null);

  const stopLive = () => {
    if (liveTimer.current) { clearInterval(liveTimer.current); liveTimer.current = null; }
    setLive("off");
  };
  useEffect(() => stopLive, []);
  useEffect(() => { stopLive(); setMsgs([]); setNextOffsets(null); setSel(null); setEmptyNote(null); }, [cid, topic]);

  const buildQuery = (from?: Record<string, number> | null, liveLimit?: number) => {
    const q = new URLSearchParams();
    q.set("partition", partition);
    q.set("mode", mode);
    if (mode === "offset") q.set("offset", offset);
    if (mode === "timestamp") {
      if (start) q.set("startTime", start);
      if (end) q.set("endTime", end);
    }
    q.set("limit", String(liveLimit ?? limit));
    if (keyQ.trim()) q.set("key", keyQ.trim());
    if (valQ.trim()) q.set("value", valQ.trim());
    if (from) q.set("from", JSON.stringify(from));
    return q.toString();
  };

  const query = async (append = false) => {
    if (mode === "offset" && offset.trim() === "") { setErr("offset 模式需要填写 Offset"); return; }
    if (mode === "timestamp" && !start) { setErr("timestamp 模式需要开始时间"); return; }
    stopLive();
    if (!append) { setBusy(true); setSel(null); }
    else setMoreBusy(true);
    setErr(null);
    try {
      const r = await fetch(`${API}/api/clusters/${cid}/topics/${encodeURIComponent(topic)}/messages?${buildQuery(append ? nextOffsets : null)}`);
      if (!r.ok) throw new Error(await r.text());
      const d = await r.json() as BrowseResult;
      setMsgs((p) => append ? [...p, ...d.messages].slice(-LIVE_CAP) : d.messages);
      setNextOffsets(d.nextOffsets);
      setHasMore(d.hasMore);
      setScanned(d.scanned);
      setTruncated(d.truncated);
      if (!append && d.messages.length === 0) setEmptyNote("没有消息");
      else if (d.messages.length > 0) setEmptyNote(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
      setMoreBusy(false);
    }
  };

  const pollLive = async () => {
    try {
      const r = await fetch(`${API}/api/clusters/${cid}/topics/${encodeURIComponent(topic)}/messages?${buildQuery(liveCursor.current, 200)}`);
      if (!r.ok) return;
      const d = await r.json() as BrowseResult;
      if (d.messages.length > 0) {
        setMsgs((p) => [...p, ...d.messages].slice(-LIVE_CAP));
        setEmptyNote(null);
      }
      liveCursor.current = d.nextOffsets;
      setNextOffsets(d.nextOffsets);
    } catch { /* 下轮重试 */ }
  };

  const startLive = async () => {
    stopLive();
    setErr(null);
    setEmptyNote("等待消息…");
    try {
      const r = await fetch(`${API}/api/clusters/${cid}/topics/${encodeURIComponent(topic)}/messages?${buildQuery(null, 50)}`);
      if (!r.ok) throw new Error(await r.text());
      const d = await r.json() as BrowseResult;
      setMsgs(d.messages.slice(-LIVE_CAP));
      if (d.messages.length > 0) setEmptyNote(null);
      liveCursor.current = d.nextOffsets;
      setNextOffsets(d.nextOffsets);
      setLive("run");
      liveTimer.current = window.setInterval(pollLive, 2000);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      setEmptyNote(null);
    }
  };

  const copy = (text: string, tag: string) => {
    try {
      navigator.clipboard.writeText(text);
      setCopied(tag);
      setTimeout(() => setCopied(null), 1200);
    } catch { /* ignore */ }
  };

  const selMsg = sel != null ? msgs[sel] : null;
  let pretty: string | null = null;
  if (selMsg?.value) {
    try { pretty = JSON.stringify(JSON.parse(selMsg.value), null, 2); }
    catch { pretty = null; }
  }

  return (
    <div className="topic-messages space-y-3">
      {/* 查询条件 */}
      <div className="topic-message-query">
      <div className="topic-section-heading mb-3">
        <h2>查询条件</h2>
      </div>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <div><label className={labelCls} htmlFor="message-partition">Partition</label>
          <select id="message-partition" className={inputCls} value={partition} onChange={(e) => setPartition(e.target.value)}>
            <option value="all">全部分区</option>
            {partitions.map((p) => <option key={p} value={String(p)}>{p}</option>)}
          </select></div>
        <div><label className={labelCls} htmlFor="message-mode">定位方式</label>
          <select id="message-mode" className={inputCls} value={mode} onChange={(e) => setMode(e.target.value as Mode)}>
            <option value="latest">最新 Latest</option>
            <option value="earliest">最早 Earliest</option>
            <option value="offset">指定 Offset</option>
            <option value="timestamp">时间范围</option>
          </select></div>
        <div><label className={labelCls} htmlFor="message-limit">读取条数</label>
          <select id="message-limit" className={inputCls} value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            {[10, 50, 100, 500, 1000].map((n) => <option key={n} value={n}>{n}</option>)}
          </select></div>
        {mode === "offset" ? (
          <div><label className={labelCls} htmlFor="message-offset">Offset</label>
            <input id="message-offset" className={inputCls} value={offset} placeholder="例如 10000"
              onChange={(e) => setOffset(e.target.value)} /></div>
        ) : mode === "timestamp" ? (
          <div><label className={labelCls} htmlFor="message-start">开始时间</label>
            <input id="message-start" type="datetime-local" className={inputCls} value={start}
              onChange={(e) => setStart(e.target.value)} /></div>
        ) : <div className="hidden xl:block" />}
      </div>
      {mode === "timestamp" && (
        <div className="mt-2 grid grid-cols-2 gap-2 xl:grid-cols-4">
          <div><label className={labelCls} htmlFor="message-end">结束时间（可选）</label>
            <input id="message-end" type="datetime-local" className={inputCls} value={end}
              onChange={(e) => setEnd(e.target.value)} /></div>
        </div>
      )}
      <details className="message-extra-filters mt-3">
        <summary>内容筛选{(keyQ || valQ) && <span className="badge run">已设置</span>}</summary>
        <div className="mt-2 grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div><label className={labelCls} htmlFor="message-key-filter">Key 包含</label>
            <input id="message-key-filter" className={inputCls} value={keyQ} placeholder="输入 Key 片段"
              onChange={(e) => setKeyQ(e.target.value)} /></div>
          <div><label className={labelCls} htmlFor="message-value-filter">Value 包含</label>
            <input id="message-value-filter" className={inputCls} value={valQ} placeholder="输入 Value 片段"
              onChange={(e) => setValQ(e.target.value)} /></div>
        </div>
      </details>
      <div className="topic-message-actions">
        <button onClick={() => query(false)} disabled={busy || live !== "off"} className="btn btn-primary btn-sm">
          {busy ? "查询中…" : "查询"}
        </button>
        <button onClick={() => { stopLive(); setMsgs([]); setNextOffsets(null); setSel(null); setEmptyNote(null); setErr(null); }} className="btn btn-ghost btn-sm">清空</button>
        <span className="mx-1 h-4 w-px bg-[var(--border-soft)]" />
        {live === "off" ? (
          <button onClick={startLive} className="btn btn-ghost btn-sm">▶ 实时</button>
        ) : (
          <>
            <span className="badge run pulsing">LIVE</span>
            {live === "run"
              ? <button onClick={() => { if (liveTimer.current) clearInterval(liveTimer.current); liveTimer.current = null; setLive("pause"); }} className="btn btn-ghost btn-sm">暂停</button>
              : <button onClick={() => { setLive("run"); liveTimer.current = window.setInterval(pollLive, 2000); }} className="btn btn-ghost btn-sm">继续</button>}
            <button onClick={stopLive} className="btn btn-ghost btn-sm">停止</button>
          </>
        )}
      </div>
      </div>
      {err && <div className="alert err mt-2">{err}</div>}
      {truncated && <div className="alert warn mt-2">查询已达到最大扫描范围，未继续扫描更多消息。（10000 条 / 30s）</div>}

      {/* 消息列表 */}
      <div className="mt-2">
        {msgs.length === 0 ? (
          <div className="py-8 text-center text-[12px] text-slate-500">{emptyNote ?? "没有消息"}</div>
        ) : (
          <>
            <div className="tbl-wrap scroll-box topic-message-table-wrap">
              <table className="tbl mono topic-message-table">
                <thead className="sticky top-0 bg-[var(--panel-2)]"><tr>
                  <th className="text-center">分区</th>
                  <th>Offset</th>
                  <th>时间</th>
                  <th>Key</th>
                  <th>Value</th>
                  <th className="text-center">Headers</th>
                  <th className="text-right">大小</th>
                </tr></thead>
                <tbody className="tabular-nums">
                  {msgs.map((m, i) => (
                    <tr key={`${m.partition}-${m.offset}`} onClick={() => { setSel(i); setViewJson(true); }}
                      className={`cursor-pointer ${sel === i ? "!bg-[rgba(56,189,248,0.09)]" : ""}`}>
                      <td className="text-center text-slate-400">{m.partition}</td>
                      <td className="text-sky-700">{m.offset}</td>
                      <td className="whitespace-nowrap text-slate-400">{m.timestampText ?? "—"}</td>
                      <td className="max-w-[120px] truncate text-slate-600">{m.key ?? <span className="text-slate-600">null</span>}</td>
                      <td className="max-w-[260px] truncate text-slate-700">{summary(m.value)}</td>
                      <td className="text-center">{m.headers.length > 0 ? <span className="badge mute">{m.headers.length}</span> : <span className="text-slate-600">—</span>}</td>
                      <td className="whitespace-nowrap text-right text-slate-500">{fmtSize(m.size)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-1.5 flex items-center gap-2">
              <span className="mono text-[10.5px] text-slate-600">已加载 {msgs.length} 条{truncated ? " · 已达扫描上限" : ""}</span>
              {hasMore && (
                <button onClick={() => query(true)} disabled={moreBusy} className="btn btn-ghost btn-sm ml-auto">
                  {moreBusy ? "加载中…" : "加载更多"}
                </button>
              )}
              {!hasMore && msgs.length > 0 && (
                <span className="mono ml-auto text-[10.5px] text-slate-600">已到末尾</span>
              )}
            </div>
            {msgs.length >= LIVE_CAP && (
              <div className="mono mt-1 text-[10.5px] text-slate-600">已达到本次查询最大消息数量限制。</div>
            )}
          </>
        )}
      </div>

      {/* 消息详情 Drawer */}
      {selMsg && (
        <div className="fixed inset-0 z-40" onClick={() => setSel(null)}>
          <div className="absolute inset-0 bg-black/50" />
          <div role="dialog" aria-modal="true" aria-label="消息详情" className="topic-message-drawer absolute inset-y-0 right-0 flex w-full max-w-[520px] flex-col border-l border-[var(--border-soft)] p-4" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center gap-2">
              <span className="text-[13px] font-bold text-slate-800">消息详情</span>
              <span className="mono text-[11px] text-slate-500">分区 {selMsg.partition} · Offset {selMsg.offset}</span>
              <button onClick={() => setSel(null)} className="ml-auto rounded px-2 py-0.5 text-slate-400 hover:bg-slate-900/[0.06] hover:text-slate-800">✕</button>
            </div>
            <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-[11.5px]">
              <div className="text-slate-500">Topic</div><div className="mono break-all text-slate-700">{selMsg.topic}</div>
              <div className="text-slate-500">时间</div><div className="mono text-slate-700">{selMsg.timestampText ?? "—"}{selMsg.timestamp != null && <span className="text-slate-600"> · {selMsg.timestamp}</span>}</div>
              <div className="text-slate-500">Key</div>
              <div className="mono flex items-center gap-1.5 break-all text-slate-700">{selMsg.key ?? <span className="text-slate-600">null</span>}
                {selMsg.key && <button onClick={() => copy(selMsg.key as string, "k")} className="shrink-0 text-[10px] text-sky-700/70 hover:text-sky-700">{copied === "k" ? "✓" : "复制"}</button>}</div>
              <div className="text-slate-500">大小</div><div className="mono text-slate-700">{fmtSize(selMsg.size)}</div>
            </div>
            <div className="mt-2 flex items-center gap-1.5">
              <span className="text-[11px] text-slate-500">Value</span>
              <span className="seg">
                <button onClick={() => setViewJson(true)} className={viewJson ? "on" : ""}>格式化</button>
                <button onClick={() => setViewJson(false)} className={!viewJson ? "on" : ""}>原文</button>
              </span>
              {selMsg.value && <button onClick={() => copy(selMsg.value as string, "v")} className="ml-auto text-[10.5px] text-sky-700/70 hover:text-sky-700">{copied === "v" ? "已复制 ✓" : "复制"}</button>}
            </div>
            <pre className="codebox mt-1 max-h-56 flex-none overflow-auto whitespace-pre-wrap break-all">{viewJson ? (pretty ?? selMsg.value ?? "null") : (selMsg.value ?? "null")}</pre>
            <div className="mt-2 text-[11px] text-slate-500">Headers（{selMsg.headers.length}）</div>
            <div className="scroll-box mt-1 min-h-0 flex-1 space-y-1 overflow-y-auto overscroll-contain pr-1">
              {selMsg.headers.length === 0 && <div className="py-2 text-center text-[11px] text-slate-600">无 Headers</div>}
              {selMsg.headers.map((h, i) => (
                <div key={i} className="mono flex items-center gap-2 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2 py-1.5 text-[11px]">
                  <span className="shrink-0 text-sky-700">{h.key}</span>
                  <span className="min-w-0 flex-1 truncate text-slate-600" title={h.value ?? "null"}>{h.value ?? <span className="text-slate-600">null</span>}</span>
                  {h.value != null && <button onClick={() => copy(h.value as string, `h${i}`)} className="shrink-0 text-[10px] text-sky-700/70 hover:text-sky-700">{copied === `h${i}` ? "✓" : "复制"}</button>}
                </div>
              ))}
            </div>
            <div className="mt-2 flex items-center gap-2 border-t border-[var(--border-soft)] pt-2">
              <button disabled={sel == null || sel <= 0} onClick={() => { setSel((s) => (s != null ? s - 1 : s)); setViewJson(true); }} className="btn btn-ghost btn-sm">← 上一条</button>
              <span className="mono mx-auto text-[10.5px] text-slate-600">{sel != null ? sel + 1 : 0} / {msgs.length}</span>
              <button disabled={sel == null || sel >= msgs.length - 1} onClick={() => { setSel((s) => (s != null ? s + 1 : s)); setViewJson(true); }} className="btn btn-ghost btn-sm">下一条 →</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
