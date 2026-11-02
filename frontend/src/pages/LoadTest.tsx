import { apiFetch, apiWebSocket } from "../api/http";
import { useEffect, useRef, useState } from "react";
import { useConn } from "../conn";
import { authHeaders, useAuth } from "../auth";
import { useLab } from "../lab";
import { PageHeader } from "../components/bits";
import { DEFAULT_SECURITY, type SecurityForm } from "../api/producer";

const inputCls = "field mono";
const labelCls = "lbl";
const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;
type LoadSecurity = Omit<SecurityForm, "securityProtocol"> & {
  securityProtocol: SecurityForm["securityProtocol"] | "SSL";
};

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
  const { conn, clusters, activeId, loading: clustersLoading } = useConn();
  const { mode: authMode } = useAuth();
  const readonly = authMode === "readonly";
  const { requireLab } = useLab();
  const [bootstrap, setBootstrap] = useState("");
  const [managedClusterId, setManagedClusterId] = useState("");
  const [security, setSecurity] = useState<LoadSecurity>(DEFAULT_SECURITY);
  const [caPath, setCaPath] = useState("");
  const [topic, setTopic] = useState("");
  const [mode, setMode] = useState<"count" | "duration">("count");
  const [total, setTotal] = useState(10000);
  const [durationSec, setDurationSec] = useState(60);
  const [concurrency, setConcurrency] = useState(8);
  const [valueSize, setValueSize] = useState(128);
  const [job, setJob] = useState<Job | null>(null);
  const [report, setReport] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  const timer = useRef<number | null>(null);
  const hasSelectedCluster = Boolean(activeId && clusters.some((cluster) => cluster.id === activeId));
  const updateSecurity = <K extends keyof LoadSecurity>(key: K, value: LoadSecurity[K]) => {
    setManagedClusterId("");
    setSecurity((previous) => ({ ...previous, [key]: value }));
  };
  const importCluster = () => {
    if (!hasSelectedCluster) return;
    setBootstrap(conn.bootstrap);
    setManagedClusterId(conn.id);
    setSecurity({
      securityProtocol: conn.securityProtocol,
      saslMechanism: conn.saslMechanism,
      saslUsername: conn.saslUsername,
      saslPassword: conn.saslPassword,
    });
    setErr(null);
  };

  const stop = () => { if (timer.current) { clearInterval(timer.current); timer.current = null; } };
  useEffect(() => stop, []);

  const run = async () => {
    setErr(null);
    if (!bootstrap.trim()) { setErr("请填写 Bootstrap Servers，或从当前工作空间带入连接配置。"); return; }
    if (!topic.trim()) { setErr("请填写压测 Topic。"); return; }
    if (security.securityProtocol.startsWith("SASL_") && (!security.saslUsername.trim() || (!managedClusterId && !security.saslPassword))) {
      setErr("SASL 连接需要填写用户名和密码。"); return;
    }
    setJob(null);
    setReport(null);
    setStarting(true);
    try {
      const r = await apiFetch(`${API}/api/loadtest/run`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({
          clusterId: managedClusterId, bootstrap: bootstrap.trim(), topic: topic.trim(),
          partition: "auto", acks: "all", retries: 1,
          requestTimeoutMs: 10000, deliveryTimeoutMs: 30000,
          mode, total, durationSec, concurrency, valueSize,
          ...security, caPath: caPath.trim(),
        }),
      });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const { jobId } = await r.json() as { jobId: string };
      setJob({ id: jobId, status: "running", mode,
        total: mode === "count" ? total : null,
        durationTarget: mode === "duration" ? durationSec : null,
        done: 0 });
      stop();
      timer.current = window.setInterval(async () => {
        try {
          const s = await (await apiFetch(`${API}/api/loadtest/${jobId}`)).json() as Job;
          setJob(s);
          if (s.status === "done" || s.status === "failed") {
            stop();
            if (s.status === "done") {
              try {
                const rep = await (await apiFetch(`${API}/api/loadtest/${jobId}/report`)).json() as { markdown: string };
                setReport(rep.markdown);
              } catch { /* 报告拉取失败不阻塞结果展示 */ }
            }
          }
        } catch { /* 下轮重试 */ }
      }, 1000);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setStarting(false);
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
    <div className="loadtest-page">
      <PageHeader
        title="压测"
        right={running
          ? <span className="badge run pulsing">压测中 {job!.done}/{targetLabel}</span>
          : job?.status === "done"
            ? <span className="badge ok">完成 {job.ok}/{job.done}</span>
            : job?.status === "failed"
              ? <span className="badge err">失败</span> : undefined}
      />
      {err && <div className="alert err mb-3">{err}</div>}
      {job?.status === "failed" && <div className="alert err mb-3">压测失败：{job.error ?? "未知错误"}</div>}
      <div className="loadtest-layout">
        <div className="card p-3.5 loadtest-config">
          <div className="loadtest-config-scroll scroll-box">
          <div className="loadtest-section">
            <div className="sim-section-head"><h2>连接目标</h2><span>01 / TARGET</span></div>
            {!clustersLoading && !hasSelectedCluster && <div className="sim-target-hint" role="status"><span className="sim-target-hint-dot" />未选集群，请手动填写连接地址和 Topic</div>}
            <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-bootstrap">Bootstrap Servers</label>
              <input id="loadtest-bootstrap" className={inputCls} value={bootstrap} onChange={(e) => { setManagedClusterId(""); setBootstrap(e.target.value); }} placeholder="broker1:9092,broker2:9092" />
              {hasSelectedCluster && <div className="loadtest-import-row"><button type="button" className="sim-inline-action" onClick={importCluster}>从当前工作空间带入连接配置 ↗</button></div>}
            </div>
            <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-topic">Topic</label>
              <input id="loadtest-topic" className="field" value={topic} onChange={(e) => setTopic(e.target.value)} placeholder="输入已有 Topic 名称" /></div>
            <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-protocol">安全协议</label>
              <select id="loadtest-protocol" className="field" value={security.securityProtocol} onChange={(e) => updateSecurity("securityProtocol", e.target.value as LoadSecurity["securityProtocol"])}>
                <option>PLAINTEXT</option><option>SSL</option><option>SASL_PLAINTEXT</option><option>SASL_SSL</option>
              </select></div>
            {security.securityProtocol.startsWith("SASL_") && <div className="loadtest-auth">
              <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-mechanism">认证机制</label><select id="loadtest-mechanism" className="field" value={security.saslMechanism} onChange={(e) => updateSecurity("saslMechanism", e.target.value as SecurityForm["saslMechanism"])}><option>PLAIN</option><option>SCRAM-SHA-256</option><option>SCRAM-SHA-512</option></select></div>
              <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-username">用户名</label><input id="loadtest-username" className="field" value={security.saslUsername} onChange={(e) => updateSecurity("saslUsername", e.target.value)} autoComplete="off" /></div>
              <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-password">密码</label><input id="loadtest-password" type="password" className="field" value={security.saslPassword} onChange={(e) => updateSecurity("saslPassword", e.target.value)} autoComplete="new-password" /></div>
            </div>}
            {(security.securityProtocol === "SSL" || security.securityProtocol === "SASL_SSL") && <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-ca">CA 文件路径（可选，KAFLAB 主机）</label><input id="loadtest-ca" className="field mono" value={caPath} onChange={(e) => setCaPath(e.target.value)} placeholder="/path/to/ca.pem" /></div>}
          </div>
          <div className="loadtest-section loadtest-scenario">
          <div className="sim-section-head"><h2>发送场景</h2><span>02 / SCENARIO</span></div>
          <div className="loadtest-field"><span className={labelCls}>模式</span>
            <span className="seg w-full" role="group" aria-label="压测模式">
              {(["count", "duration"] as const).map((m) => (
                <button key={m} type="button" onClick={() => setMode(m)} aria-pressed={mode === m} className={mode === m ? "on" : ""}>
                  {m === "count" ? "按条数" : "按时长"}
                </button>
              ))}
            </span></div>
          {mode === "count" ? (
            <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-total">总条数（≤100万）</label>
              <input id="loadtest-total" type="number" min={1} max={1000000} step={1000} className={inputCls} value={total}
                onChange={(e) => setTotal(Math.min(1000000, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
          ) : (
            <div className="loadtest-field"><label className={labelCls} htmlFor="loadtest-duration">时长（秒，5–3600）</label>
              <input id="loadtest-duration" type="number" min={5} max={3600} className={inputCls} value={durationSec}
                onChange={(e) => setDurationSec(Math.min(3600, Math.max(5, Math.floor(Number(e.target.value) || 60))))} /></div>
          )}
          <div className="grid grid-cols-2 gap-2">
            <div><label className={labelCls} htmlFor="loadtest-concurrency">并发（≤64）</label>
              <input id="loadtest-concurrency" type="number" min={1} max={64} className={inputCls} value={concurrency}
                onChange={(e) => setConcurrency(Math.min(64, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
            <div><label className={labelCls} htmlFor="loadtest-value-size">Value 字节</label>
              <input id="loadtest-value-size" type="number" min={1} max={65536} className={inputCls} value={valueSize}
                onChange={(e) => setValueSize(Math.min(65536, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
          </div>
          </div>
          </div>
          <div className="loadtest-action">
            <button onClick={() => requireLab(() => { void run(); })} disabled={readonly || running || starting} title={readonly ? "只读模式下禁止启动压测" : undefined} className="btn btn-send">
              {running ? "压测中…" : starting ? "正在启动…" : "开始压测"}
            </button>
          </div>
        </div>
        <div className="loadtest-observe">
          <div className="card p-3.5 loadtest-progress scroll-box">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-800">进度</span>
              {job && <span className="mono ml-auto text-[11px] text-slate-400">{job.done}/{targetLabel}{job.mode === "duration" ? ` · ${pct}%` : ` · ${pct}%`}</span>}
            </div>
            {!job && <div className="loadtest-empty text-center ui-note">启动后显示实时进度</div>}
            {job && (
              <>
                <div className="h-2 overflow-hidden rounded bg-slate-900/[0.08]">
                  <div className="loadtest-progress-fill h-full rounded bg-gradient-to-r from-emerald-400 to-teal-300" style={{ width: `${job.status === "done" ? 100 : pct}%` }} />
                </div>
                {job.status === "done" && (
                  <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-5">
                    {stats.map(([k, v]) => (
                      <div key={k} className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
                        <div className="card-title">{k}</div>
                        <div className="mono mt-0.5 text-[13px] font-bold text-slate-800">{v}</div>
                      </div>
                    ))}
                  </div>
                )}
              </>
            )}
          </div>
          {job?.status === "done" && (
            <div className="loadtest-results">
              <div className="card p-3.5 loadtest-result-card scroll-box">
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
              <div className="card p-3.5 loadtest-result-card scroll-box">
                <div className="card-title mb-2">错误分布</div>
                {(job.errors ?? []).length === 0
                  ? <div className="py-3 text-center text-[11.5px] text-emerald-700">零失败</div>
                  : (
                    <div className="space-y-1">
                      {job.errors!.map(([msg, n]) => (
                        <div key={msg} className="mono flex items-center gap-2 rounded-lg border border-[rgba(251,113,133,0.25)] bg-[rgba(251,113,133,0.05)] px-2.5 py-1.5 text-[11px]">
                          <span className="badge err shrink-0">×{n}</span>
                          <span className="min-w-0 flex-1 truncate text-rose-700" title={msg}>{msg}</span>
                        </div>
                      ))}
                    </div>
                  )}
              </div>
              <div className="card p-3.5 loadtest-result-card scroll-box">
                <div className="card-t">
                  <span className="text-[13px] font-bold text-slate-800">压测报告</span>
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
                {copied && <div className="mb-1 text-[11px] text-emerald-700">已复制 ✓</div>}
                {report
                  ? <pre className="codebox max-h-80 overflow-auto whitespace-pre-wrap">{report}</pre>
                  : <div className="py-3 text-center text-[11.5px] text-slate-600">报告生成中…</div>}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
