import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useConn, type ConnConfig } from "../conn";
import { PageHeader } from "../components/bits";
import { useAuth } from "../auth";

const inputCls = "field mono";
const labelCls = "lbl";

type Draft = {
  id?: string; name: string; bootstrap: string; securityProtocol: string;
  saslMechanism: string; saslUsername: string; saslPassword: string;
  monitoringEnabled: boolean;
  hasSaslPassword?: boolean;
  clearSaslPassword?: boolean;
};
const EMPTY: Draft = {
  name: "", bootstrap: "", securityProtocol: "PLAINTEXT",
  saslMechanism: "PLAIN", saslUsername: "", saslPassword: "",
  monitoringEnabled: true,
};

function ClusterEditor({
  draft, onChange, onSubmit, onCancel, busy, err, readonly,
}: {
  draft: Draft;
  onChange: (draft: Draft) => void;
  onSubmit: () => void;
  onCancel: () => void;
  busy: boolean;
  err: string | null;
  readonly: boolean;
}) {
  const editLocked = readonly && Boolean(draft.id);
  return (
    <div className="card cluster-editor fade-up">
      <div className="cluster-editor-heading">
        <span className="cluster-editor-title">{draft.id ? "编辑集群" : "新增集群"}</span>
      </div>
      <fieldset disabled={editLocked} className="cluster-editor-fields">
        <div><label className={labelCls} htmlFor="cluster-name">名称</label>
          <input id="cluster-name" className={inputCls} placeholder="例如：生产集群" value={draft.name}
            onChange={(e) => onChange({ ...draft, name: e.target.value })} /></div>
        <div><label className={labelCls} htmlFor="cluster-bootstrap">Bootstrap 地址</label>
          <input id="cluster-bootstrap" className={inputCls} placeholder="192.168.1.10:9092，多个地址用逗号分隔" value={draft.bootstrap}
            onChange={(e) => onChange({ ...draft, bootstrap: e.target.value })} /></div>
        <div><label className={labelCls} htmlFor="cluster-protocol">安全协议</label>
          <select id="cluster-protocol" className={inputCls} value={draft.securityProtocol}
            onChange={(e) => onChange({ ...draft, securityProtocol: e.target.value })}>
            <option value="PLAINTEXT">PLAINTEXT</option>
            <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
            <option value="SASL_SSL">SASL_SSL（未实现）</option>
          </select></div>
        {draft.securityProtocol !== "PLAINTEXT" && (
          <>
            <div><label className={labelCls} htmlFor="cluster-mechanism">认证机制</label>
              <select id="cluster-mechanism" className={inputCls} value={draft.saslMechanism}
                onChange={(e) => onChange({ ...draft, saslMechanism: e.target.value })}>
                <option value="PLAIN">PLAIN</option>
                <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
              </select></div>
            <div><label className={labelCls} htmlFor="cluster-username">用户名</label>
              <input id="cluster-username" className={inputCls} value={draft.saslUsername}
                onChange={(e) => onChange({ ...draft, saslUsername: e.target.value })} /></div>
            <div><label className={labelCls} htmlFor="cluster-password">密码</label>
              <input id="cluster-password" type="password" className={inputCls} value={draft.saslPassword} placeholder={draft.hasSaslPassword ? "已保存，留空保持不变" : "输入密码"}
                onChange={(e) => onChange({ ...draft, saslPassword: e.target.value })} /></div>
          </>
        )}
      </fieldset>
      {err && <div className="alert err mt-2.5">{err}</div>}
      <div className="cluster-editor-footer">
        <button type="button" role="switch" aria-checked={draft.monitoringEnabled} disabled={editLocked}
          onClick={() => onChange({ ...draft, monitoringEnabled: !draft.monitoringEnabled })}
          className={`cluster-collect-toggle ${draft.monitoringEnabled ? "is-on" : ""}`}>
          <span className="cluster-collect-track" aria-hidden="true"><span /></span>
          <span className="cluster-collect-copy"><strong>自动采集 Lag</strong><small>每分钟采集一次</small></span>
          <span className="cluster-collect-state">{draft.monitoringEnabled ? "已开启" : "已关闭"}</span>
        </button>
        <div className="cluster-editor-actions">
          <button onClick={onCancel} className="btn btn-ghost">取消</button>
          <button onClick={onSubmit} disabled={busy || editLocked} title={editLocked ? "只读模式下禁止编辑集群" : undefined} className="btn btn-primary">
            {busy ? "保存…" : draft.id ? "保存" : "保存并进入"}
          </button>
        </div>
      </div>
    </div>
  );
}

export function ClustersPage() {
  const { clusters, activeId, select, save, remove, probe, health, loading, refresh } = useConn();
  const { mode } = useAuth();
  const readonly = mode === "readonly";
  const navigate = useNavigate();
  const [editing, setEditing] = useState<Draft | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [probing, setProbing] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [actionError, setActionError] = useState<string | null>(null);
  const editorRef = useRef<HTMLDivElement>(null);
  const editingKey = editing ? (editing.id ?? "new") : null;

  const online = clusters.filter((c) => health[c.id]?.connected).length;
  const offline = clusters.filter((c) => health[c.id]?.connected === false).length;
  const visibleClusters = clusters.filter((c) =>
    `${c.name} ${c.bootstrap} ${c.securityProtocol} ${c.saslMechanism}`.toLowerCase().includes(query.trim().toLowerCase()));

  useEffect(() => {
    clusters.forEach((cluster) => { if (!health[cluster.id]) void probe(cluster.id); });
  }, [clusters, health, probe]);

  useEffect(() => {
    if (editingKey) editorRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [editingKey]);

  const submit = async () => {
    if (!editing || !editing.bootstrap.trim()) { setErr("bootstrap 不可为空"); return; }
    if (readonly && editing.id) { setErr("只读模式下禁止编辑集群"); return; }
    const isNew = !editing.id;
    setBusy(true);
    setErr(null);
    try {
      const saved = await save(editing as ConnConfig);
      setEditing(null);
      await probe(saved.id);
      if (isNew) {
        select(saved.id);
        navigate("/dashboard");
      }
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const openEdit = (c: ConnConfig) => {
    setQuery(""); setErr(null); setActionError(null);
    setEditing({ ...c, monitoringEnabled: c.monitoringEnabled ?? true });
  };

  const doProbe = async (id: string) => {
    setProbing(id);
    try { await probe(id); } finally { setProbing(null); }
  };

  const enterCluster = (id: string) => {
    select(id);
    navigate("/dashboard", { replace: true });
  };

  return (
    <div className="clusters-page">
      <PageHeader
        title="集群管理"
        right={
          <div className="clusters-header-actions">
            <button onClick={() => {
              setActionError(null);
              void refresh().catch((e) => setActionError(e instanceof Error ? e.message : String(e)));
            }} disabled={loading} className="btn btn-ghost clusters-toolbar-button">
              {loading ? "刷新中…" : "刷新"}
            </button>
            <button onClick={() => { setEditing({ ...EMPTY }); setErr(null); setActionError(null); }}
              className="btn btn-primary clusters-toolbar-button">
              新增集群
            </button>
          </div>
        }
      />

      {actionError && <div className="alert err mb-3">{actionError}</div>}

      {editing && !editing.id && (
        <div ref={editorRef} className="cluster-create-slot">
          <ClusterEditor draft={editing} onChange={setEditing} onSubmit={() => void submit()}
            onCancel={() => setEditing(null)} busy={busy} err={err} readonly={readonly} />
        </div>
      )}

      <div className="clusters-toolbar">
        <div className="clusters-summary" aria-label="集群状态汇总">
          <span><strong>{clusters.length}</strong> 个集群</span>
          <span className="clusters-summary-divider" />
          <span><i className="clusters-summary-dot online" /><strong>{online}</strong> 在线</span>
          {offline > 0 && <span><i className="clusters-summary-dot offline" /><strong>{offline}</strong> 离线</span>}
        </div>
        <label className="sr-only" htmlFor="cluster-search">搜索集群</label>
        <input id="cluster-search" value={query} onChange={(e) => setQuery(e.target.value)}
          className="field clusters-search" placeholder="搜索集群名称、地址或协议" />
        {query && <span className="clusters-search-count">匹配 {visibleClusters.length} 个</span>}
      </div>

      {loading && clusters.length === 0 && <div className="card p-6 text-center text-[12px] text-slate-500">正在读取纳管集群…</div>}

      {!loading && clusters.length === 0 && !editing && (
        <div className="card flex flex-col items-center p-10 text-center">
          <span className="mb-3 flex h-12 w-12 items-center justify-center rounded-2xl bg-sky-400/10 text-xl text-sky-300">▦</span>
          <div className="text-[15px] font-semibold text-[var(--text)]">还没有集群</div>
          <div className="mt-1 ui-note">添加 Bootstrap 地址后即可开始探测</div>
          <button onClick={() => { setEditing({ ...EMPTY }); setErr(null); }} className="btn btn-primary mt-4">添加第一个集群</button>
        </div>
      )}

      <div className="clusters-grid">
        {visibleClusters.map((c) => {
          const h = health[c.id];
          const active = c.id === activeId;
          const status = probing === c.id ? "探测中" : h?.connected ? "在线" : h?.connected === false ? "离线" : "待检测";
          const dot = probing === c.id ? "bg-sky-400" : h?.connected == null ? "bg-slate-500" : h.connected ? "bg-emerald-400" : "bg-rose-400";
          return (
            <div key={c.id} className="cluster-grid-item">
              <div className={`card cluster-row-card fade-up ${active ? "is-current" : ""}`}>
                <div className="cluster-card-head">
                  <div className="cluster-card-identity">
                    <span className="cluster-status-indicator">
                      {probing === c.id && <span className="absolute h-full w-full animate-ping rounded-full bg-sky-400 opacity-60" />}
                      <span className={`relative h-2.5 w-2.5 rounded-full ${dot}`} />
                    </span>
                    <div className="cluster-card-title-wrap">
                      <div className="cluster-card-title-line">
                        <h2 className="cluster-card-title" title={c.name}>{c.name}</h2>
                        <span className={`badge ${probing === c.id ? "run" : h?.connected ? "ok" : h?.connected === false ? "err" : "mute"}`}>{status}</span>
                        {active && <span className="badge run">当前集群</span>}
                      </div>
                      <div className="cluster-card-address mono" title={c.bootstrap}>{c.bootstrap}</div>
                    </div>
                  </div>
                  <button onClick={() => enterCluster(c.id)} className="btn btn-primary cluster-enter-button">进入工作空间</button>
                </div>
                <div className="cluster-card-overview">
                  <div className="cluster-card-stat">
                    <span>Broker</span><strong className="mono">{h?.brokerCount ?? (probing === c.id ? "…" : "—")}</strong>
                  </div>
                  <button type="button" onClick={() => { select(c.id); navigate("/topics"); }} className="cluster-card-stat cluster-card-stat-link" aria-label={`查看 ${c.name} 的主题`}>
                    <span>主题 <span aria-hidden="true">↗</span></span><strong className="mono">{h?.topicCount ?? (probing === c.id ? "…" : "—")}</strong>
                  </button>
                </div>
                <div className="cluster-card-details">
                  <div className="cluster-card-protocol">
                    <span className="badge vio">{c.securityProtocol}</span>
                    {c.securityProtocol !== "PLAINTEXT" && <span className="badge mute">{c.saslMechanism}</span>}
                  </div>
                  {h?.connected && h.brokers && h.brokers.length > 0 && (
                    <details className="cluster-brokers">
                      <summary className="cluster-brokers-label">Broker 节点 <span className="mono">{h.brokers.length}</span></summary>
                      <div className="cluster-brokers-list">
                        {h.brokers.map((broker) => (
                          <span key={broker.id} title={broker.rack ? `Rack: ${broker.rack}` : `${broker.host}:${broker.port}`} className="cluster-broker mono">
                            <b>#{broker.id}</b> {broker.host}:{broker.port}
                            {broker.id === h.controllerId && <em>Controller</em>}
                          </span>
                        ))}
                      </div>
                    </details>
                  )}
                </div>
                {h?.error && <div className="alert err cluster-card-error break-words" title={h.error}>探测失败：{h.error}</div>}
                <div className="cluster-card-actions">
                  <button onClick={() => void doProbe(c.id)} disabled={probing === c.id} className="btn btn-ghost cluster-action-button">
                    {probing === c.id ? "探测中…" : "探测"}
                  </button>
                  <button onClick={() => openEdit(c)} disabled={readonly} title={readonly ? "只读模式下禁止编辑集群" : undefined} className="btn btn-ghost cluster-action-button">编辑</button>
                  <button
                  onClick={async () => {
                    const ok = window.confirm("删除集群 " + c.name + "？");
                    if (!ok) return;
                    setActionError(null);
                    try { await remove(c.id); }
                    catch (e) { setActionError(e instanceof Error ? e.message : String(e)); }
                  }}
                    disabled={readonly} title={readonly ? "只读模式下禁止移除集群" : undefined} className="btn btn-danger-ghost cluster-action-button cluster-remove-button">
                    移除
                  </button>
                </div>
              </div>
              {editing?.id === c.id && (
                <div ref={editorRef} className="cluster-card-editor-slot">
                  <ClusterEditor draft={editing} onChange={setEditing} onSubmit={() => void submit()}
                    onCancel={() => setEditing(null)} busy={busy} err={err} readonly={readonly} />
                </div>
              )}
            </div>
          );
        })}
      </div>

      {!loading && clusters.length > 0 && visibleClusters.length === 0 && (
        <div className="card mt-3 p-8 text-center">
          <div className="text-[13px] font-semibold text-[var(--text)]">没有匹配的集群</div>
          <button onClick={() => setQuery("")} className="btn btn-ghost btn-sm mt-3">清除搜索</button>
        </div>
      )}

    </div>
  );
}
