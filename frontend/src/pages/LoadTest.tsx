import { useEffect, useRef, useState } from "react";
import { useConn } from "../conn";
import { authHeaders } from "../auth";
import { useLab } from "../lab";
import { PageHeader } from "../components/bits";

const inputCls = "field mono";
const labelCls = "lbl";
const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";

interface Job {
  id: string; status: string; mode?: string;
  total: number | null; durationTarget?: number | null;
  concurrency?: number; valueSize?: number;
  topic?: string; bootstrap?: string;
  done: number;
  ok?: number; fail?: number; durationS?: number; msgPerS?: number; mbPerS?: number;
  avgMs?: number | null; p50Ms?: number | null; p95Ms?: number | null;
  p99Ms?: number | null; maxMs?: number | null;
  errors?: [string, number][]; perSec?: { sec: number; n: number }[];
  error?: string;
}

export function LoadTestPage() {
  const { conn } = useConn();
  const { requireLab } = useLab();
  const [topic, setTopic] = useState("test-topic");
  const [mode, setMode] = useState<"count" | "duration">("count");
  const [total, setTotal] = useState(10000);
  const [durationSec, setDurationSec] = useState(60);
  const [concurrency, setConcurrency] = useState(8);
  const [valueSize, setValueSize] = useState(128);
  const [job, setJob] = useState<Job | null>(null);
  const [report, setReport] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const timer = useRef<number | null>(null);

  const stop = () => { if (timer.current) { clearInterval(timer.current); timer.current = null; } };
  useEffect(() => stop, []);

  const run = async () => {
    setErr(null);
    setJob(null);
    setReport(null);
    try {
      const r = await fetch(`${API}/api/loadtest/run`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({
          bootstrap: conn.bootstrap, topic,
          partition: "auto", acks: "all", retries: 1,
          requestTimeoutMs: 10000, deliveryTimeoutMs: 30000,
          mode, total, durationSec, concurrency, valueSize,
          securityProtocol: conn.securityProtocol, saslMechanism: conn.saslMechanism,
          saslUsername: conn.saslUsername, saslPassword: conn.saslPassword,
        }),
      });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const { jobId } = await r.json() as { jobId: string };
      stop();
      timer.current = window.setInterval(async () => {
        try {
          const s = await (await fetch(`${API}/api/loadtest/${jobId}`)).json() as Job;
          setJob(s);
          if (s.status === "done" || s.status === "failed") {
            stop();
            if (s.status === "done") {
              try {
                const rep = await (await fetch(`${API}/api/loadtest/${jobId}/report`)).json() as { markdown: string };
                setReport(rep.markdown);
              } catch { /* 报告拉取失败不阻塞结果展示 */ }
            }
          }
        } catch { /* 下轮重试 */ }
      }, 1000);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  };

  const download = () => {
    if (!report || !job) return;
    const blob = new Blob([report], { type: "text/markdown;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `loadtest-${job.id}.md`;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const running = job?.status === "running";
  const targetLabel = job
    ? job.mode === "duration" ? `${job.durationTarget ?? 60}s` : `${job.total ?? 0} 条`
    : mode === "duration" ? `${durationSec}s` : `${total} 条`;
  const pct = job
    ? job.mode === "duration"
      ? Math.min(99, Math.round((job.done / Math.max(1, (job.msgPerS ?? 1) * (job.durationTarget ?? 60))) * 100))
      : Math.round((job.done / Math.max(1, job.total ?? 1)) * 100)
    : 0;
  const peak = Math.max(1, ...(job?.perSec ?? []).map((x) => x.n));
  const fmtMs = (v: number | null | undefined) => v == null ? "—" : `${Math.round(v * 10) / 10}ms`;
  const stats: [string, string][] = job ? [
    ["成功", String(job.ok ?? "—")],
    ["失败", String(job.fail ?? "—")],
    ["耗时", job.durationS != null ? `${job.durationS}s` : "—"],
    ["吞吐", job.msgPerS != null ? `${job.msgPerS} msg/s` : "—"],
    ["带宽", job.mbPerS != null ? `${job.mbPerS} MB/s` : "—"],
    ["平均", fmtMs(job.avgMs)],
    ["P50", fmtMs(job.p50Ms)],
    ["P95", fmtMs(job.p95Ms)],
    ["P99", fmtMs(job.p99Ms)],
    ["最大", fmtMs(job.maxMs)],
  ] : [];

  return (
    <div>
      <PageHeader
        title="压测"
        desc={`${conn.name} · 上线前大吞吐验证`}
        right={running
          ? <span className="badge run pulsing">压测中 {job!.done}/{targetLabel}</span>
          : job?.status === "done"
            ? <span className="badge ok">完成 {job.ok}/{job.done}</span>
            : job?.status === "failed"
              ? <span className="badge err">失败</span> : undefined}
      />
      {err && <div className="alert err mb-3">{err}</div>}
      {job?.status === "failed" && <div className="alert err mb-3">压测失败：{job.error ?? "未知错误"}</div>}
      <div className="layout-grid mt-3 !grid-cols-1 xl:!grid-cols-[320px_minmax(0,1fr)]">
        <div className="card h-fit p-3.5">
          <div className="card-title mb-2">压测配置</div>
          <div><label className={labelCls}>Topic</label>
            <input className={inputCls} value={topic} onChange={(e) => setTopic(e.target.value)} /></div>
          <div className="mt-2"><label className={labelCls}>模式</label>
            <span className="seg w-full">
              {(["count", "duration"] as const).map((m) => (
                <button key={m} onClick={() => setMode(m)} className={mode === m ? "on" : ""}>
                  {m === "count" ? "按条数" : "按时长"}
                </button>
              ))}
            </span></div>
          {mode === "count" ? (
            <div className="mt-2"><label className={labelCls}>总条数（≤100万）</label>
              <input type="number" min={1} max={1000000} step={1000} className={inputCls} value={total}
                onChange={(e) => setTotal(Math.min(1000000, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
          ) : (
            <div className="mt-2"><label className={labelCls}>时长（秒，5–3600）</label>
              <input type="number" min={5} max={3600} className={inputCls} value={durationSec}
                onChange={(e) => setDurationSec(Math.min(3600, Math.max(5, Math.floor(Number(e.target.value) || 60))))} /></div>
          )}
          <div className="mt-2 grid grid-cols-2 gap-2">
            <div><label className={labelCls}>并发（≤64）</label>
              <input type="number" min={1} max={64} className={inputCls} value={concurrency}
                onChange={(e) => setConcurrency(Math.min(64, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
            <div><label className={labelCls}>Value 字节</label>
              <input type="number" min={1} max={65536} className={inputCls} value={valueSize}
                onChange={(e) => setValueSize(Math.min(65536, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
          </div>
          <div className="mono mt-2 rounded-lg bg-black/20 px-2 py-1.5 text-[10.5px] leading-relaxed text-slate-500">
            分区自动 · 全部确认 · 按条数/时长结束
          </div>
          <button onClick={() => requireLab(() => { void run(); })} disabled={running} className="btn btn-send mt-3">
            {running ? "压测中…" : "⚡ 开始压测"}
          </button>
        </div>
        <div className="min-w-0 space-y-3">
          <div className="card p-3.5">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-100">进度</span>
              {job && <span className="mono ml-auto text-[11px] text-slate-400">{job.done}/{targetLabel}{job.mode === "duration" ? ` · ${pct}%` : ` · ${pct}%`}</span>}
            </div>
            {!job && <div className="py-6 text-center text-[12px] text-slate-500">配置后点开始，结果每秒刷新。</div>}
            {job && (
              <>
                <div className="h-2 overflow-hidden rounded bg-white/[0.06]">
                  <div className="h-full rounded bg-gradient-to-r from-emerald-400 to-teal-300 transition-all" style={{ width: `${job.status === "done" ? 100 : pct}%` }} />
                </div>
                {job.status === "done" && (
                  <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-5">
                    {stats.map(([k, v]) => (
                      <div key={k} className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
                        <div className="card-title">{k}</div>
                        <div className="mono mt-0.5 text-[13px] font-bold text-slate-100">{v}</div>
                      </div>
                    ))}
                  </div>
                )}
              </>
            )}
          </div>
          {job?.status === "done" && (
            <>
              <div className="card p-3.5">
                <div className="card-title mb-2">每秒吞吐</div>
                {(job.perSec ?? []).length === 0
                  ? <div className="py-3 text-center text-[11.5px] text-slate-600">无数据</div>
                  : (
                    <div className="flex h-[96px] items-end gap-[3px]">
                      {(job.perSec ?? []).map((x) => (
                        <div key={x.sec} title={`${x.sec}s: ${x.n}`} className="min-w-[4px] flex-1 rounded-t bg-gradient-to-t from-emerald-500/40 to-teal-300/70"
                          style={{ height: `${Math.max(4, (x.n / peak) * 100)}%` }} />
                      ))}
                    </div>
                  )}
              </div>
              <div className="card p-3.5">
                <div className="card-title mb-2">错误分布</div>
                {(job.errors ?? []).length === 0
                  ? <div className="py-3 text-center text-[11.5px] text-emerald-300">零失败</div>
                  : (
                    <div className="space-y-1">
                      {job.errors!.map(([msg, n]) => (
                        <div key={msg} className="mono flex items-center gap-2 rounded-lg border border-[rgba(251,113,133,0.25)] bg-[rgba(251,113,133,0.05)] px-2.5 py-1.5 text-[11px]">
                          <span className="badge err shrink-0">×{n}</span>
                          <span className="min-w-0 flex-1 truncate text-rose-200/90" title={msg}>{msg}</span>
                        </div>
                      ))}
                    </div>
                  )}
              </div>
              <div className="card p-3.5">
                <div className="card-t">
                  <span className="text-[13px] font-bold text-slate-100">压测报告</span>
                  <span className="ml-auto flex gap-1.5">
                    <button
                      onClick={() => {
                        if (!report) return;
                        try {
                          navigator.clipboard.writeText(report);
                          setCopied(true);
                          setTimeout(() => setCopied(false), 1500);
                        } catch { /* ignore */ }
                      }}
                      className="btn btn-ghost btn-sm">复制</button>
                    <button onClick={download} className="btn btn-ghost btn-sm">下载 .md</button>
                  </span>
                </div>
                {copied && <div className="mb-1 text-[11px] text-emerald-300">已复制 ✓</div>}
                {report
                  ? <pre className="codebox max-h-80 overflow-auto whitespace-pre-wrap">{report}</pre>
                  : <div className="py-3 text-center text-[11.5px] text-slate-600">报告生成中…</div>}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
