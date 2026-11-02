import { useEffect, useState, type ReactNode } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useConn } from "../conn";
import { useAuth } from "../auth";
import { useLab } from "../lab";
import { reportConn } from "../App";

type ShellIconName = "monitoring" | "overview" | "topics" | "producer" | "consumer" | "groups" | "loadtest" |
  "sim-producer" | "sim-consumer" | "operations" | "clusters" | "refresh" | "logout" | "chevron" | "menu";

function ShellIcon({ name }: { name: ShellIconName }) {
  const paths: Record<ShellIconName, ReactNode> = {
    monitoring: <><path d="M3 12h4l2-5 4 10 2-5h6" /></>,
    overview: <><rect x="3" y="3" width="7" height="7" rx="1.3" /><rect x="14" y="3" width="7" height="7" rx="1.3" /><rect x="3" y="14" width="7" height="7" rx="1.3" /><rect x="14" y="14" width="7" height="7" rx="1.3" /></>,
    topics: <><rect x="4" y="4" width="16" height="16" rx="2" /><path d="M4 9h16M4 14h16" /></>,
    producer: <><path d="M5 19 19 5M10 5h9v9" /><path d="M5 8v4" /></>,
    consumer: <><path d="M12 4v15m-6-6 6 6 6-6" /></>,
    groups: <><circle cx="9" cy="8" r="3" /><path d="M3 19v-1a6 6 0 0 1 12 0v1H3ZM17 5a3 3 0 0 1 0 6m2 8h2v-1a5 5 0 0 0-4-4.9" /></>,
    loadtest: <path d="M3 12h4l2.5-6 4 12 2.5-6H21" />,
    "sim-producer": <><rect x="3" y="5" width="11" height="14" rx="2" /><path d="M6 9h5M6 12h5M18 8l3 4-3 4m-3-4h6" /></>,
    "sim-consumer": <><path d="M3 8h6m-3-3 3 3-3 3" /><rect x="10" y="5" width="11" height="14" rx="2" /><path d="M13 9h5m-5 3h5m-5 3h4" /></>,
    operations: <><path d="M5 4h14v16H5zM8 8h8M8 12h8M8 16h5" /></>,
    clusters: <><rect x="3" y="3" width="7" height="7" rx="1.3" /><rect x="14" y="3" width="7" height="7" rx="1.3" /><rect x="3" y="14" width="7" height="7" rx="1.3" /><rect x="14" y="14" width="7" height="7" rx="1.3" /></>,
    refresh: <><path d="M20 7v5h-5M4 17v-5h5" /><path d="M5.5 9A7 7 0 0 1 18 7l2 5M4 12l2 5a7 7 0 0 0 12.5-2" /></>,
    logout: <><path d="M10 4H5a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h5" /><path d="M14 8l4 4-4 4m-8-4h12" /></>,
    chevron: <path d="m5 9 7 7 7-7" />,
    menu: <><path d="M4 6h16M4 12h16M4 18h16" /></>,
  };
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{paths[name]}</svg>;
}

const GLOBAL_NAV: [string, string, ShellIconName][] = [["集群总览", "/dashboard", "overview"], ["监控与告警", "/monitoring", "monitoring"]];
const CLUSTER_NAV: [string, string, ShellIconName][] = [
  ["Topics", "/topics", "topics"], ["Producer", "/producer", "producer"], ["Consumer", "/consumer", "consumer"],
  ["消费组", "/group", "groups"],
];
const TOOL_NAV: [string, string, ShellIconName][] = [["生产端模拟", "/sim/producer", "sim-producer"], ["消费端模拟", "/sim/consumer", "sim-consumer"], ["压测", "/loadtest", "loadtest"], ["后台操作", "/operations", "operations"]];

function NavLink({ label, to, icon, active, disabled = false, onNavigate }: { label: string; to: string; icon: ShellIconName; active: boolean; disabled?: boolean; onNavigate?: () => void }) {
  const className = `shell-nav-link${active ? " is-active" : ""}${disabled ? " is-disabled" : ""}`;
  return disabled
    ? <div className={className} title="请先选择集群"><span className="shell-nav-icon"><ShellIcon name={icon} /></span><span>{label}</span></div>
    : <Link to={to} title={label} aria-current={active ? "page" : undefined} className={className} onClick={onNavigate}><span className="shell-nav-icon"><ShellIcon name={icon} /></span><span>{label}</span></Link>;
}

function ClusterSwitcher() {
  const { clusters, activeId, conn, select, health } = useConn();
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
      <button className="cluster-switch" aria-label={`当前集群：${selected ? conn.name : "未选择"}`} aria-expanded={open} onClick={() => setOpen((value) => !value)}>
        <i className={`status-dot ${dot}`} /><span className="cluster-switch-copy"><small>当前集群</small><b>{selected ? conn.name : "选择 Kafka 集群"}</b></span><span className={`switch-chevron${open ? " is-open" : ""}`}><ShellIcon name="chevron" /></span>
      </button>
      {selected && <button type="button" className="probe-button" aria-label="刷新当前页面" title="刷新当前页面" onClick={() => window.location.reload()}><ShellIcon name="refresh" /></button>}
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
        <span className="mode-segment-dot" />只读模式
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
  const { clusters, activeId, conn, health } = useConn();
  const { unlocked, lock } = useLab();
  const { username, logout } = useAuth();
  const displayName = username || "当前用户";
  const avatarText = displayName.slice(0, 1).toUpperCase();
  const hasCluster = Boolean(activeId && clusters.some((cluster) => cluster.id === activeId));
  const clusterNav = CLUSTER_NAV;
  const toolNav = TOOL_NAV;
  const isActive = (path: string) => location.pathname === path || (path === "/topics" && location.pathname.startsWith("/topics/"));
  const current = [...GLOBAL_NAV, ...clusterNav, ...toolNav].find(([, path]) => isActive(path));
  const onClustersPage = location.pathname === "/clusters";
  const onToolPage = location.pathname.startsWith("/sim/") || location.pathname === "/loadtest";
  const [navOpen, setNavOpen] = useState(false);
  useEffect(() => { setNavOpen(false); }, [location.pathname]);
  useEffect(() => {
    if (!navOpen) return;
    const closeOnEscape = (event: KeyboardEvent) => { if (event.key === "Escape") setNavOpen(false); };
    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [navOpen]);
  const pageTitle = current?.[0] ?? (onClustersPage ? "所有集群" : "集群工作区");
  useEffect(() => {
    if (hasCluster) reportConn({ ok: health[conn.id]?.connected ?? null, bootstrap: conn.bootstrap, detail: conn.name });
  }, [hasCluster, conn.id, conn.bootstrap, conn.name, health[conn.id]?.connected]);
  const protocol = hasCluster ? conn.securityProtocol : null;

  return <div className="app-shell">
    <a className="skip-link" href="#main-content">跳转到主内容</a>
    {navOpen && <button type="button" className="sidebar-mobile-scrim" aria-label="关闭导航" onClick={() => setNavOpen(false)} />}
    <aside className={`app-sidebar${navOpen ? " is-expanded" : ""}`}>
      <Link to="/dashboard" className="brand"><span className="brand-mark">K</span><span className="brand-lockup"><b className="brand-wordmark">KAFLAB</b><small>CLUSTER OPERATIONS</small></span></Link>
      <button type="button" className="shell-nav-toggle" aria-label={navOpen ? "收起导航" : "展开导航"} aria-expanded={navOpen} onClick={() => setNavOpen((current) => !current)}><ShellIcon name="menu" /></button>
      <div className="sidebar-workspace"><span className="eyebrow">工作空间</span><ClusterSwitcher /></div>
      <nav className="sidebar-nav">
        <div className="nav-section-label">概览</div>
        {GLOBAL_NAV.map(([label,path,icon]) => <NavLink key={path} label={label} to={path} icon={icon} active={isActive(path)} disabled={!hasCluster} onNavigate={() => setNavOpen(false)} />)}
        <div className="nav-section-label nav-section-spaced">数据工作台</div>
        {clusterNav.map(([label,path,icon]) => <NavLink key={path} label={label} to={path} icon={icon} active={isActive(path)} disabled={!hasCluster} onNavigate={() => setNavOpen(false)} />)}
        <div className="nav-section-label nav-section-spaced">工具</div>
        {toolNav.map(([label,path,icon]) => <NavLink key={path} label={label} to={path} icon={icon} active={isActive(path)} onNavigate={() => setNavOpen(false)} />)}
      </nav>
      <div className="sidebar-bottom">
        <Link to="/clusters" className={`sidebar-utility${onClustersPage ? " is-active" : ""}`} aria-current={onClustersPage ? "page" : undefined}><ShellIcon name="clusters" /><span>所有集群</span></Link>
        <div className="user-card"><span className="avatar">{avatarText}</span><span className="user-copy"><b title={displayName}>{displayName}</b><small>KAFLAB</small></span><button type="button" aria-label="退出登录" title="退出登录" onClick={logout}><ShellIcon name="logout" /></button></div>
      </div>
    </aside>
    <div className="app-main">
      <header className="app-topbar">
        <div className="topbar-heading" aria-label={onToolPage ? `工具，${pageTitle}` : hasCluster && !onClustersPage ? `工作空间，${conn.name}，${pageTitle}` : `工作空间，${pageTitle}`}>
          <span className="topbar-kicker">{onToolPage ? "工具" : "工作空间"}</span>
          <span className="topbar-divider" aria-hidden="true">›</span>
          {hasCluster && !onClustersPage && !onToolPage && <><span className="topbar-cluster" title={conn.name}>{conn.name}</span><span className="topbar-divider" aria-hidden="true">›</span></>}
          <b className={`topbar-page-title${/^[\x00-\x7F]+$/.test(pageTitle) ? " is-latin" : ""}`}>{pageTitle}</b>
        </div>
        <div className="topbar-right">
          {protocol && !onToolPage && <span className={`protocol-pill ${protocol === "PLAINTEXT" ? "plain" : "secure"}`}>{protocol}</span>}
          {!onToolPage && status}
          <ModeSwitcher />
          <span className="topbar-avatar" title={displayName}>{avatarText}</span>
        </div>
      </header>
      <main id="main-content" className="app-content" tabIndex={-1}>
        {unlocked && <div className="experiment-banner"><span>实验模式已解锁</span><span>发送、压测、入组与故障操作可用</span><button onClick={lock}>结束实验</button></div>}
        {children}
      </main>
    </div>
  </div>;
}

export interface ConnState { ok: boolean | null; bootstrap: string; detail?: string; }
export function ConnBar({ conn }: { conn: ConnState }) {
  const text = conn.ok == null ? "检测中" : conn.ok ? "已连接" : "连接异常";
  return <span title={[conn.bootstrap, conn.detail].filter(Boolean).join(" · ")} className={`connection-pill ${conn.ok ? "online" : conn.ok === false ? "offline" : "pending"}`}><i />{text}</span>;
}
export function Placeholder({ title, note }: { title: string; note: string }) {
  return <div className="card mx-auto max-w-xl p-8 text-center"><div className="text-[15px] font-semibold text-[var(--text)]">{title}</div><div className="mt-2 ui-note">{note}</div><Link to="/clusters" className="btn btn-info btn-sm mt-4">返回集群选择</Link></div>;
}
