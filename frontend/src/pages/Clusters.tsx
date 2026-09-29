import { useState } from "react";
import { useConn, type ConnConfig } from "../conn";
import { PageHeader } from "../components/bits";

const inputCls = "field mono";
const labelCls = "lbl";

type Draft = {
  id?: string; name: string; bootstrap: string; securityProtocol: string;
  saslMechanism: string; saslUsername: string; saslPassword: string;
};
const EMPTY: Draft = {
  name: "", bootstrap: "", securityProtocol: "PLAINTEXT",
  saslMechanism: "PLAIN", saslUsername: "", saslPassword: "",
};

function ClusterEditor({
  draft, onChange, onSubmit, onCancel, busy, err,
}: {
  draft: Draft;
  onChange: (draft: Draft) => void;
  onSubmit: () => void;
  onCancel: () => void;
  busy: boolean;
  err: string | null;
}) {
  return (
    <div className="card fade-up p-4">
      <div className="card-t">
        <span className="text-[13px] font-bold text-slate-100">{draft.id ? "编辑集群" : "新增集群"}</span>
        {draft.id && <span className="mono text-[11px] text-[var(--text-faint)]">{draft.id}</span>}
      </div>
      <div className="grid grid-cols-1 gap-2.5 sm:grid-cols-2 xl:grid-cols-3">
        <div><label className={labelCls}>名称</label>
          <input className={inputCls} placeholder="如：生产集群" value={draft.name}
            onChange={(e) => onChange({ ...draft, name: e.target.value })} /></div>
        <div><label className={labelCls}>Bootstrap（host:port，可逗号分隔多个）</label>
          <input className={inputCls} placeholder="192.168.1.10:9092" value={draft.bootstrap}
            onChange={(e) => onChange({ ...draft, bootstrap: e.target.value })} /></div>
        <div><label className={labelCls}>Security Protocol</label>
          <select className={inputCls} value={draft.securityProtocol}
            onChange={(e) => onChange({ ...draft, securityProtocol: e.target.value })}>
            <option value="PLAINTEXT">PLAINTEXT</option>
            <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
            <option value="SASL_SSL">SASL_SSL（未实现）</option>
          </select></div>
        {draft.securityProtocol !== "PLAINTEXT" && (
          <>
            <div><label className={labelCls}>SASL Mechanism</label>
              <select className={inputCls} value={draft.saslMechanism}
                onChange={(e) => onChange({ ...draft, saslMechanism: e.target.value })}>
                <option value="PLAIN">PLAIN</option>
                <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
              </select></div>
            <div><label className={labelCls}>Username</label>
              <input className={inputCls} value={draft.saslUsername}
                onChange={(e) => onChange({ ...draft, saslUsername: e.target.value })} /></div>
            <div><label className={labelCls}>Password</label>
              <input type="password" className={inputCls} value={draft.saslPassword}
                onChange={(e) => onChange({ ...draft, saslPassword: e.target.value })} /></div>
          </>
        )}
      </div>
      {err && <div className="alert err mt-2.5">{err}</div>}
      <div className="mt-3 flex gap-2">
        <button onClick={onSubmit} disabled={busy} className="btn btn-send !w-auto !px-6">
          {busy ? "保存…" : "保存并进入"}
        </button>
        <button onClick={onCancel} className="btn btn-ghost !px-6">取消</button>
      </div>
    </div>
  );
}

export function ClustersPage() {
  const { clusters, activeId, select, save, remove, probe, health, loading, refresh } = useConn();
  const [editing, setEditing] = useState<Draft | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [probing, setProbing] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [actionError, setActionError] = useState<string | null>(null);

  const online = clusters.filter((c) => health[c.id]?.connected).length;
  const offline = clusters.filter((c) => health[c.id]?.connected === false).length;
  const visibleClusters = clusters.filter((c) =>
    `${c.name} ${c.bootstrap} ${c.securityProtocol} ${c.saslMechanism}`.toLowerCase().includes(query.trim().toLowerCase()));

  const submit = async () => {
    if (!editing || !editing.bootstrap.trim()) { setErr("bootstrap 不可为空"); return; }
    setBusy(true);
    setErr(null);
    try {
      const saved = await save(editing as ConnConfig);
      setEditing(null);
      select(saved.id);
      await probe(saved.id);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const doProbe = async (id: string) => {
    setProbing(id);
    try { await probe(id); } finally { setProbing(null); }
  };

  return (
    <div>
      <PageHeader
        title="集群管理"
        desc="纳管多个集群，切换后各页面都在该集群上工作。"
        right={
          <>
            <span className="badge mute">{clusters.length} 个集群</span>
            <span className={`badge ${offline ? "warn" : "ok"}`}>{online} 在线{offline ? ` · ${offline} 离线` : ""}</span>
            <button onClick={() => {
              setActionError(null);
              void refresh().catch((e) => setActionError(e instanceof Error ? e.message : String(e)));
            }} disabled={loading} className="btn btn-ghost btn-sm">
              {loading ? "刷新中…" : "↻ 刷新"}
            </button>
            <button
              onClick={() => { setEditing({ ...EMPTY }); setErr(null); setActionError(null); }}
              className="btn btn-primary btn-sm !min-h-[32px]">
              + 新增集群
            </button>
          </>
        }
      />

      {actionError && <div className="alert err mb-3">{actionError}</div>}

      <div className="mb-3 flex flex-wrap items-center gap-2 rounded-xl border border-[var(--border-soft)] bg-[var(--panel-3)] p-2.5">
        <label className="sr-only" htmlFor="cluster-search">搜索集群</label>
        <input id="cluster-search" value={query} onChange={(e) => setQuery(e.target.value)}
          className="field min-w-[220px] flex-1 !h-[36px]" placeholder="搜索名称、地址或安全协议…" />
        <span className="shrink-0 px-1 text-[11px] text-[var(--text-faint)]">
          {query ? `${visibleClusters.length} / ${clusters.length} 个集群` : "按名称或地址筛选"}
        </span>
      </div>

      {loading && clusters.length === 0 && <div className="card p-6 text-center text-[12px] text-slate-500">正在读取纳管集群…</div>}

      {!loading && clusters.length === 0 && (
        <div className="card flex flex-col items-center p-10 text-center">
          <span className="mb-3 flex h-12 w-12 items-center justify-center rounded-2xl bg-sky-400/10 text-xl text-sky-300">▦</span>
          <div className="text-[15px] font-semibold text-slate-100">还没有纳管集群</div>
          <div className="mt-1 max-w-sm text-[12px] text-[var(--text-faint)]">添加 Bootstrap 地址后即可连接 Kafka，并在概览中查看集群状态。</div>
          <button onClick={() => { setEditing({ ...EMPTY }); setErr(null); }} className="btn btn-primary btn-sm mt-4">+ 添加第一个集群</button>
        </div>
      )}

      {/* 每张卡片采用统一的上下两层：身份与操作、协议与运行元数据。 */}
      <div className="space-y-3">
        {visibleClusters.map((c) => {
          const h = health[c.id];
          const active = c.id === activeId;
          const status = probing === c.id ? "探测中" : h?.connected ? "在线" : h?.connected === false ? "离线" : "待检测";
          const dot = probing === c.id ? "bg-sky-400" : h?.connected == null ? "bg-slate-500" : h.connected ? "bg-emerald-400" : "bg-rose-400";
          return (
            <div key={c.id} className="space-y-2">
              <div className={`card fade-up p-4 transition-all ${active ? "!border-sky-400/50 !shadow-[0_0_0_1px_rgba(56,189,248,0.22),0_16px_32px_-24px_rgba(0,0,0,0.9)]" : "hover:!border-[#2a3c60]"}`}>
                <div className="flex flex-wrap items-center gap-x-5 gap-y-3">
                  <div className="flex min-w-[260px] flex-1 items-center gap-3">
                    <span className="relative flex h-2.5 w-2.5 shrink-0">
                      {probing === c.id && <span className="absolute h-full w-full animate-ping rounded-full bg-sky-400 opacity-60" />}
                      <span className={`relative h-2.5 w-2.5 rounded-full ${dot}`} />
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="truncate text-[15px] font-bold text-slate-100">{c.name}</span>
                        <span className={`badge ${probing === c.id ? "run" : h?.connected ? "ok" : h?.connected === false ? "err" : "mute"}`}>{status}</span>
                        {active && <span className="badge run">当前集群</span>}
                      </div>
                      <div className="mono mt-1 truncate text-[11px] text-slate-300" title={c.bootstrap}>{c.bootstrap}</div>
                    </div>
                  </div>
                  <div className="ml-auto flex shrink-0 flex-wrap items-center gap-2">
                  {!active && <button onClick={() => select(c.id)} className="btn btn-primary btn-sm">进入</button>}
                  <button onClick={() => void doProbe(c.id)} disabled={probing === c.id} className="btn btn-ghost btn-sm">
                    {probing === c.id ? "探测中…" : "探测"}
                  </button>
                  <button onClick={() => { setQuery(""); setEditing({ ...c }); setErr(null); setActionError(null); }} className="btn btn-ghost btn-sm">编辑</button>
                  <button
                  onClick={() => {
                    select(c.id);
                    window.location.hash = "#/topics";
                  }}
                  className="btn btn-ghost btn-sm">
                    Topics
                  </button>
                  <button
                  onClick={async () => {
                    const ok = window.confirm("删除集群 " + c.name + "？");
                    if (!ok) return;
                    setActionError(null);
                    try { await remove(c.id); }
                    catch (e) { setActionError(e instanceof Error ? e.message : String(e)); }
                  }}
                    className="btn btn-danger-ghost btn-sm">
                    移除
                  </button>
                  </div>
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-3 border-t border-[var(--border-soft)] pt-3">
                  <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">
                    <span className="badge vio">{c.securityProtocol}</span>
                    {c.securityProtocol !== "PLAINTEXT" && <span className="badge mute">{c.saslMechanism}</span>}
                    {h?.connected && h.brokers && h.brokers.length > 0 ? (
                      <>
                        <span className="ml-1 text-[10.5px] font-semibold text-[var(--text-faint)]">Broker 节点</span>
                        {h.brokers.map((broker) => (
                          <span key={broker.id} title={broker.rack ? `Rack: ${broker.rack}` : `${broker.host}:${broker.port}`}
                            className="mono inline-flex items-center gap-1.5 rounded-md border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-1.5 text-[10.5px] text-slate-300">
                            <span className="font-semibold text-sky-300">#{broker.id}</span>
                            <span>{broker.host}:{broker.port}</span>
                            {broker.id === h.controllerId && <span className="text-amber-300">Controller</span>}
                          </span>
                        ))}
                      </>
                    ) : (
                      <span className="ml-1 text-[10.5px] text-[var(--text-faint)]">
                        {h?.error ? "节点元数据暂不可用" : probing === c.id ? "正在获取节点信息…" : "探测后显示 Broker 节点信息"}
                      </span>
                    )}
                  </div>
                  <div className="ml-auto flex shrink-0 items-center gap-2">
                    <div className="flex min-w-[105px] items-center justify-between gap-3 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-3 py-2">
                      <span className="text-[10.5px] text-[var(--text-faint)]">Brokers</span>
                      <span className="mono text-[13px] font-semibold text-slate-100">{h?.brokerCount ?? (probing === c.id ? "…" : "—")}</span>
                    </div>
                    <div className="flex min-w-[100px] items-center justify-between gap-3 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-3 py-2">
                      <span className="text-[10.5px] text-[var(--text-faint)]">Topics</span>
                      <span className="mono text-[13px] font-semibold text-slate-100">{h?.topicCount ?? (probing === c.id ? "…" : "—")}</span>
                    </div>
                  </div>
                </div>
                {h?.error && <div className="alert err mt-3 break-words" title={h.error}>探测失败：{h.error}</div>}
              </div>
              {editing?.id === c.id && (
                <ClusterEditor draft={editing} onChange={setEditing} onSubmit={() => void submit()}
                  onCancel={() => setEditing(null)} busy={busy} err={err} />
              )}
            </div>
          );
        })}
      </div>

      {!loading && clusters.length > 0 && visibleClusters.length === 0 && (
        <div className="card mt-3 p-8 text-center">
          <div className="text-[13px] font-semibold text-slate-200">没有匹配的集群</div>
          <button onClick={() => setQuery("")} className="btn btn-ghost btn-sm mt-3">清除搜索</button>
        </div>
      )}

      {editing && !editing.id && (
        <div className="mt-3">
          <ClusterEditor draft={editing} onChange={setEditing} onSubmit={() => void submit()}
            onCancel={() => setEditing(null)} busy={busy} err={err} />
        </div>
      )}

    </div>
  );
}
