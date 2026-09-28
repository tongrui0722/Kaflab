import { useState } from "react";
import { Link } from "react-router-dom";
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

export function ClustersPage() {
  const { clusters, activeId, select, save, remove, probe, health, loading } = useConn();
  const [editing, setEditing] = useState<Draft | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [probing, setProbing] = useState<string | null>(null);

  const online = clusters.filter((c) => health[c.id]?.connected).length;

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
            <span className="badge ok">{online} 在线</span>
            <button
              onClick={() => { setEditing({ ...EMPTY }); setErr(null); }}
              className="btn btn-primary btn-sm !min-h-[32px]">
              + 新增集群
            </button>
          </>
        }
      />

      {loading && <div className="card p-6 text-center text-[12px] text-slate-500">加载中…</div>}

      {!loading && clusters.length === 0 && (
        <div className="card p-10 text-center">
          <div className="text-[14px] font-semibold text-slate-200">还没有纳管任何集群</div>
          <div className="mt-1 text-[12px] text-[var(--text-faint)]">点右上「新增集群」，填 bootstrap 即可接入。</div>
        </div>
      )}

      {/* 集群行：管理台式列表，一行一个，信息密度高 */}
      <div className="space-y-2">
        {clusters.map((c) => {
          const h = health[c.id];
          const active = c.id === activeId;
          const dot = h?.connected == null ? "bg-slate-500" : h.connected ? "bg-emerald-400" : "bg-rose-400";
          return (
            <div
              key={c.id}
              className={`card fade-up flex flex-wrap items-center gap-x-4 gap-y-2 px-4 py-3 transition-all ${active ? "!border-sky-400/50 !shadow-[0_0_0_1px_rgba(56,189,248,0.3),0_12px_28px_-22px_rgba(0,0,0,0.9)]" : "hover:!border-[#2a3c60]"}`}>
              {/* 状态 + 名称 */}
              <div className="flex min-w-[200px] flex-1 items-center gap-3">
                <span className="relative flex h-2.5 w-2.5 shrink-0">
                  {probing === c.id && <span className="absolute h-full w-full animate-ping rounded-full bg-sky-400 opacity-60" />}
                  <span className={`relative h-2.5 w-2.5 rounded-full ${dot}`} />
                </span>
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="truncate text-[13.5px] font-bold text-slate-100">{c.name}</span>
                    {active && <span className="badge run">当前</span>}
                  </div>
                  <div className="mono mt-0.5 truncate text-[11px] text-[var(--text-faint)]">
                    {c.bootstrap} · {c.id}
                  </div>
                </div>
              </div>
              {/* 协议 + 指标 */}
              <div className="flex items-center gap-2">
                <span className="badge vio">{c.securityProtocol}</span>
                {c.securityProtocol !== "PLAINTEXT" && (
                  <span className="badge mute mono">{c.saslMechanism}</span>
                )}
              </div>
              <div className="mono hidden min-w-[170px] text-right text-[11.5px] leading-relaxed text-[var(--text-faint)] md:block">
                {h?.brokerCount != null ? (
                  <>{h.brokerCount} brokers · {h.topicCount} topics</>
                ) : h?.connected === false ? (
                  <span className="text-rose-300/90">探测失败</span>
                ) : (
                  <span className="opacity-60">未探测</span>
                )}
              </div>
              {h?.error && (
                <div className="mono w-full truncate text-[11px] text-rose-300/90" title={h.error}>
                  {h.error.slice(0, 140)}
                </div>
              )}
              {/* 操作 */}
              <div className="flex shrink-0 items-center gap-1.5">
                {!active && (
                  <button onClick={() => select(c.id)} className="btn btn-primary btn-sm">进入</button>
                )}
                <button onClick={() => doProbe(c.id)} disabled={probing === c.id} className="btn btn-ghost btn-sm">
                  {probing === c.id ? "探测…" : "探测"}
                </button>
                <button onClick={() => { setEditing({ ...c }); setErr(null); }} className="btn btn-ghost btn-sm">编辑</button>
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
                    await remove(c.id);
                  }}
                  className="btn btn-danger-ghost btn-sm">
                  删除
                </button>
              </div>
            </div>
          );
        })}
      </div>

      {/* 新增 / 编辑表单：宽幅三列 */}
      {editing && (
        <div className="card fade-up mt-3 p-4">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-100">{editing.id ? "编辑集群" : "新增集群"}</span>
            {editing.id && <span className="mono text-[11px] text-[var(--text-faint)]">{editing.id}</span>}
          </div>
          <div className="grid grid-cols-1 gap-2.5 sm:grid-cols-2 xl:grid-cols-3">
            <div><label className={labelCls}>名称</label>
              <input className={inputCls} placeholder="如：生产集群" value={editing.name}
                onChange={(e) => setEditing({ ...editing, name: e.target.value })} /></div>
            <div><label className={labelCls}>Bootstrap（host:port，可逗号分隔多个）</label>
              <input className={inputCls} placeholder="192.168.1.10:9092"
                value={editing.bootstrap}
                onChange={(e) => setEditing({ ...editing, bootstrap: e.target.value })} /></div>
            <div><label className={labelCls}>Security Protocol</label>
              <select className={inputCls} value={editing.securityProtocol}
                onChange={(e) => setEditing({ ...editing, securityProtocol: e.target.value })}>
                <option value="PLAINTEXT">PLAINTEXT</option>
                <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
                <option value="SASL_SSL">SASL_SSL（未实现）</option>
              </select></div>
            {editing.securityProtocol !== "PLAINTEXT" && (
              <>
                <div><label className={labelCls}>SASL Mechanism</label>
                  <select className={inputCls} value={editing.saslMechanism}
                    onChange={(e) => setEditing({ ...editing, saslMechanism: e.target.value })}>
                    <option value="PLAIN">PLAIN</option>
                    <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                    <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                  </select></div>
                <div><label className={labelCls}>Username</label>
                  <input className={inputCls} value={editing.saslUsername}
                    onChange={(e) => setEditing({ ...editing, saslUsername: e.target.value })} /></div>
                <div><label className={labelCls}>Password</label>
                  <input type="password" className={inputCls} value={editing.saslPassword}
                    onChange={(e) => setEditing({ ...editing, saslPassword: e.target.value })} /></div>
              </>
            )}
          </div>
          {err && <div className="alert err mt-2.5">{err}</div>}
          <div className="mt-3 flex gap-2">
            <button onClick={submit} disabled={busy} className="btn btn-send !w-auto !px-6">
              {busy ? "保存…" : "保存并进入"}
            </button>
            <button onClick={() => setEditing(null)} className="btn btn-ghost !px-6">取消</button>
          </div>
        </div>
      )}

      <div className="mt-3 flex items-center gap-2 text-[11.5px] text-[var(--text-faint)]">
        <span>进入集群后可直接浏览数据：</span>
        <Link to="/topics" className="btn btn-ghost btn-sm">去 Topics 浏览 →</Link>
      </div>
    </div>
  );
}
