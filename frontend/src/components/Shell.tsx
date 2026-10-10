import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useConn } from "../conn";
import { useAuth } from "../auth";
import { useLab } from "../lab";
import { reportConn } from "../App";

const GLOBAL_NAV: [string, string, string][] = [["集群总览", "/dashboard", "▦"]];
const CLUSTER_NAV: [string, string, string][] = [
  ["Topics", "/topics", "▤"], ["Producer", "/producer", "↗"], ["Consumer", "/consumer", "↓"],
  ["消费组", "/group", "◷"], ["压测", "/loadtest", "⌁"],
];
const TOOL_NAV: [string, string, string][] = [["协议检查", "/protocol", "⌘"], ["故障模拟", "/faults", "⚠"], ["设置", "/settings", "⚙"]];

function NavLink({ label, to, icon, active, disabled = false }: { label: string; to: string; icon: string; active: boolean; disabled?: boolean }) {
  const className = `shell-nav-link${active ? " is-active" : ""}${disabled ? " is-disabled" : ""}`;
  return disabled
    ? <div className={className} title="请先选择集群"><span className="shell-nav-icon">{icon}</span><span>{label}</span></div>
    : <Link to={to} title={label} className={className}><span className="shell-nav-icon">{icon}</span><span>{label}</span></Link>;
}

function ClusterSwitcher() {
  const { clusters, activeId, conn, select, health, probe } = useConn();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const selected = clusters.some((c) => c.id === activeId);
  const pick = (id: string) => {
    select(id);
    const cluster = clusters.find((c) => c.id === id);
    if (cluster) reportConn({ ok: null, bootstrap: cluster.bootstrap, detail: cluster.name });
    setOpen(false);
    navigate("/dashboard");
  };
  const healthState = selected ? health[conn.id]?.connected : null;
  const dot = healthState ? "connected" : healthState === false ? "offline" : "unknown";
  if (!clusters.length) return <button className="cluster-switch" onClick={() => navigate("/clusters")}>＋ 添加 Kafka 集群</button>;
  return <div className="cluster-switch-wrap">
    {open && <><button className="switch-dismiss" aria-label="关闭集群菜单" onClick={() => setOpen(false)} /><div className="switch-menu">
      <div className="switch-menu-title">切换工作集群</div>
      <div className="switch-menu-list">{clusters.map((cluster) => {
        const current = cluster.id === activeId;
        const connected = health[cluster.id]?.connected;
        return <button key={cluster.id} className={`switch-option${current ? " selected" : ""}`} onClick={() => pick(cluster.id)}>
          <i className={`status-dot ${connected ? "connected" : connected === false ? "offline" : "unknown"}`} />
          <span className="switch-option-copy"><b>{cluster.name}</b><small className="mono">{cluster.bootstrap}</small></span>{current && <span>✓</span>}
        </button>;
      })}</div>
      <button className="switch-manage" onClick={() => { setOpen(false); navigate("/clusters"); }}>管理所有集群 <span>→</span></button>
    </div></>}
    <div className="cluster-switch-row">
      <button className="cluster-switch" onClick={() => setOpen((value) => !value)}>
        <i className={`status-dot ${dot}`} /><span className="cluster-switch-copy"><small>当前集群</small><b>{selected ? conn.name : "选择 Kafka 集群"}</b></span><span className="switch-chevron">⌄</span>
      </button>
      {selected && <button className="probe-button" title="重新探测当前集群" onClick={() => probe(conn.id)}>↻</button>}
    </div>
  </div>;
}

function ModeSwitcher() {
  const { mode, switchMode } = useAuth();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const choose = async (next: "readonly" | "full") => {
    if (busy || mode === next) return;
    setBusy(true);
    setError("");
    try {
      await switchMode(next);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  return <div className="mode-switch-wrap">
    <div className={`mode-segment${busy ? " is-busy" : ""}`} role="group" aria-label="工作模式">
      <button type="button" className={`mode-segment-option${mode === "readonly" ? " is-active" : ""}`}
        aria-pressed={mode === "readonly"} disabled={busy || mode === "readonly"} onClick={() => void choose("readonly")}>
        <span className="mode-segment-dot" />只读巡检
      </button>
      <button type="button" className={`mode-segment-option${mode === "full" ? " is-active is-full" : ""}`}
        aria-pressed={mode === "full"} disabled={busy || mode === "full"} onClick={() => void choose("full")}>
        <span className="mode-segment-dot" />全部功能
      </button>
      {busy && <span className="mode-segment-progress" aria-label="切换中" />}
    </div>
    {error && <div className="mode-switch-error" role="alert">
      <span>{error}</span><button type="button" aria-label="关闭提示" onClick={() => setError("")}>×</button>
    </div>}
  </div>;
}

export function Shell({ children, status }: { children: ReactNode; status?: ReactNode }) {
  const location = useLocation();
  const navigate = useNavigate();
  const { clusters, activeId, conn, select, health } = useConn();
  const { unlocked, lock } = useLab();
  const { mode, logout } = useAuth();
  const [query, setQuery] = useState("");
  const [searchOpen, setSearchOpen] = useState(false);
  const searchInput = useRef<HTMLInputElement>(null);
  const readonly = mode === "readonly";
  const hasCluster = Boolean(activeId && clusters.some((cluster) => cluster.id === activeId));
  // 只读模式保留完整导航；写入操作在各页面禁用并由后端再次校验。
  const clusterNav = CLUSTER_NAV;
  const toolNav = TOOL_NAV;
  const isActive = (path: string) => location.pathname === path || (path === "/topics" && location.pathname.startsWith("/topics/"));
  const current = [...GLOBAL_NAV, ...clusterNav, ...toolNav].find(([, path]) => isActive(path));
  const pageTitle = current?.[0] ?? (location.pathname === "/clusters" ? "Kafka 集群" : "集群工作区");
  useEffect(() => {
    if (hasCluster) reportConn({ ok: health[conn.id]?.connected ?? null, bootstrap: conn.bootstrap, detail: conn.name });
  }, [hasCluster, conn.id, conn.bootstrap, conn.name, health[conn.id]?.connected]);
  const searchResults = useMemo(() => {
    const term = query.trim().toLowerCase();
    if (!term) return [] as { label: string; path: string; detail: string; clusterId?: string }[];
    const clusterMatches = clusters.filter((cluster) => `${cluster.name} ${cluster.bootstrap}`.toLowerCase().includes(term))
      .map((cluster) => ({ label: cluster.name, path: "/dashboard", detail: cluster.bootstrap, clusterId: cluster.id }));
    const pageMatches = [...GLOBAL_NAV, ...clusterNav, ...toolNav]
      .filter(([label, path]) => (label.toLowerCase().includes(term) || path.toLowerCase().includes(term)) && (hasCluster || path === "/clusters" || path === "/settings"))
      .map(([label, path]) => ({ label, path, detail: "功能页面" }));
    return [...clusterMatches, ...pageMatches].slice(0, 8);
  }, [query, clusters, clusterNav, toolNav, hasCluster]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") { event.preventDefault(); searchInput.current?.focus(); setSearchOpen(true); }
      if (event.key === "Escape") setSearchOpen(false);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  const openResult = (result: { path: string; clusterId?: string }) => {
    if (result.clusterId) {
      select(result.clusterId);
      const cluster = clusters.find((c) => c.id === result.clusterId);
      if (cluster) reportConn({ ok: null, bootstrap: cluster.bootstrap, detail: cluster.name });
    }
    setQuery(""); setSearchOpen(false); navigate(result.path);
  };
  const protocol = hasCluster ? conn.securityProtocol : null;

  return <div className="app-shell">
    <aside className="app-sidebar">
      <Link to="/dashboard" className="brand"><span className="brand-mark">K</span><span><b>Kafka Lab</b><small>CLUSTER OPERATIONS</small></span></Link>
      <div className="sidebar-workspace"><span className="eyebrow">工作空间</span><ClusterSwitcher /></div>
      <nav className="sidebar-nav">
        <div className="nav-section-label">概览</div>
        {GLOBAL_NAV.map(([label,path,icon]) => <NavLink key={path} label={label} to={path} icon={icon} active={isActive(path)} disabled={path === "/dashboard" && !hasCluster} />)}
        {hasCluster && <div className="nav-cluster-label"><i className={`status-dot ${health[conn.id]?.connected ? "connected" : health[conn.id]?.connected === false ? "offline" : "unknown"}`} />{conn.name}</div>}
        <div className="nav-section-label nav-section-spaced">数据工作台</div>
        {clusterNav.map(([label,path,icon]) => <NavLink key={path} label={label} to={path} icon={icon} active={isActive(path)} disabled={!hasCluster} />)}
        <div className="nav-section-label nav-section-spaced">工具</div>
        {toolNav.map(([label,path,icon]) => <NavLink key={path} label={label} to={path} icon={icon} active={isActive(path)} disabled={!hasCluster && path !== "/settings"} />)}
      </nav>
      <div className="sidebar-bottom">
        <button className="sidebar-utility" onClick={() => navigate("/clusters")}>▦ <span>所有集群</span></button>
        <div className="user-card"><span className="avatar">{readonly ? "R" : "A"}</span><span className="user-copy"><b>{readonly ? "只读用户" : "管理员"}</b><small>Kafka Lab</small></span><button title="退出登录" onClick={logout}>↗</button></div>
      </div>
    </aside>
    <div className="app-main">
      <header className="app-topbar">
        <div className="topbar-heading"><span className="topbar-kicker">KAFKA LAB</span><span className="topbar-divider">/</span><b>{pageTitle}</b>{hasCluster && <span className="topbar-cluster">{conn.name}</span>}</div>
        <div className="global-search-wrap"><span className="search-icon">⌕</span><input ref={searchInput} aria-label="全局搜索集群和功能" placeholder="搜索集群或功能…" value={query} onFocus={() => setSearchOpen(true)} onChange={(event) => { setQuery(event.target.value); setSearchOpen(true); }} onKeyDown={(event) => { if (event.key === "Enter" && searchResults[0]) openResult(searchResults[0]); }} /><kbd>Ctrl K</kbd>
          {searchOpen && query && <><button className="search-dismiss" aria-label="关闭搜索结果" onClick={() => setSearchOpen(false)} /><div className="search-results">{searchResults.length ? searchResults.map((item,index) => <button key={`${item.path}-${item.label}-${index}`} onClick={() => openResult(item)}><span className="search-result-icon">⌕</span><span><b>{item.label}</b><small>{item.detail}</small></span><span className="search-result-enter">↵</span></button>) : <div className="search-empty">没有匹配的集群或功能</div>}</div></>}
        </div>
        <div className="topbar-right">
          {protocol && <span className={`protocol-pill ${protocol === "PLAINTEXT" ? "plain" : "secure"}`}>{protocol}</span>}
          {status}
          <ModeSwitcher />
          <span className="topbar-avatar" title={readonly ? "只读用户" : "管理员"}>{readonly ? "R" : "A"}</span>
        </div>
      </header>
      <main className="app-content">
        {unlocked && <div className="experiment-banner"><span>实验模式已解锁</span><span>发送、压测、入组与故障操作可用</span><button onClick={lock}>结束实验</button></div>}
        {children}
      </main>
    </div>
  </div>;
}

export interface ConnState { ok: boolean | null; bootstrap: string; detail?: string; }
export function ConnBar({ conn }: { conn: ConnState }) {
  const text = conn.ok == null ? "检测中" : conn.ok ? "已连接" : "连接异常";
  return <span title={[conn.bootstrap, conn.detail].filter(Boolean).join(" · ")} className={`connection-pill ${conn.ok ? "online" : conn.ok === false ? "offline" : "pending"}`}><i />{text}{conn.detail && conn.ok && <span>{conn.detail}</span>}</span>;
}
export function Placeholder({ title, note }: { title: string; note: string }) {
  return <div className="card mx-auto max-w-xl p-8 text-center"><div className="text-[15px] font-semibold text-slate-100">{title}</div><div className="mt-2 text-[12.5px] leading-relaxed text-slate-400">{note}</div><Link to="/clusters" className="btn btn-info btn-sm mt-4">返回集群选择</Link></div>;
}
