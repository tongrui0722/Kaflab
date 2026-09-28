import type { ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
import { useConn } from "../conn";
import { useAuth } from "../auth";
import { useLab } from "../lab";
import { reportConn } from "../App";

const GLOBAL_NAV: [string, string, string][] = [
  ["概览", "/dashboard", "◈"],
  ["集群", "/clusters", "▦"],
];
const CLUSTER_NAV: [string, string, string][] = [
  ["Topics", "/topics", "☰"],
  ["Producer", "/producer", "➤"],
  ["Consumer", "/consumer", "⬇"],
  ["消费组", "/group", "◫"],
  ["压测", "/loadtest", "⬢"],
];
const TOOL_NAV: [string, string, string][] = [
  ["协议", "/protocol", "◔"],
  ["故障", "/faults", "⚠"],
  ["设置", "/settings", "⚙"],
];

function NavLink({ label, to, icon, active, indent }: { label: string; to: string; icon: string; active: boolean; indent?: boolean }) {
  return (
    <Link key={to} to={to} title={label}
      className={`group flex items-center gap-2.5 rounded-lg px-3 py-[7px] text-[12.5px] transition-all ${indent ? "ml-2 border-l border-[var(--border-soft)] !rounded-l-none" : ""} ${active ? "bg-[rgba(56,189,248,0.13)] font-semibold text-sky-200 shadow-[inset_0_0_0_1px_rgba(56,189,248,0.25)]" : "text-[var(--text-dim)] hover:bg-white/[0.04] hover:text-slate-100"}`}>
      <span className={`w-4 text-center text-[13px] ${active ? "text-sky-300" : "opacity-60 group-hover:opacity-100"}`}>{icon}</span>
      {label}
    </Link>
  );
}

export function Shell({ children, status }: { children: ReactNode; status?: ReactNode }) {
  const loc = useLocation();
  const { clusters, conn: active, select, health, probe } = useConn();
  const { unlocked, lock } = useLab();
  const { mode, logout } = useAuth();
  const readonly = mode === "readonly";
  const clusterNav = readonly ? CLUSTER_NAV.filter(([, to]) => to !== "/producer" && to !== "/loadtest") : CLUSTER_NAV;
  const toolNav = readonly ? TOOL_NAV.filter(([, to]) => to !== "/faults") : TOOL_NAV;
  const h = health[active.id];
  const dot = h?.connected == null ? "bg-slate-500" : h.connected ? "bg-emerald-400" : "bg-rose-400";
  const isActive = (to: string) => loc.pathname === to
    || (to === "/producer" && loc.pathname === "/")
    || (to === "/topics" && loc.pathname.startsWith("/topics/"));
  const cur = [...GLOBAL_NAV, ...clusterNav, ...toolNav].find(([, to]) => isActive(to));

  return (
    <div className="flex min-h-screen">
      {/* 侧边栏 */}
      <aside className="fixed inset-y-0 left-0 z-30 flex w-[196px] flex-col border-r border-[var(--border-soft)] bg-[var(--bg-soft)]">
        <Link to="/dashboard" className="flex items-center gap-2.5 px-4 pb-3 pt-4">
          <span className="flex h-8 w-8 items-center justify-center rounded-[9px] bg-gradient-to-br from-sky-400 to-cyan-600 text-[15px] font-black text-[#04121b] shadow-[0_6px_18px_-6px_rgba(56,189,248,0.7)]">K</span>
          <span className="leading-tight">
            <span className="block text-[13px] font-bold tracking-wide text-slate-100">Kafka Lab</span>
            <span className="block text-[10px] text-[var(--text-faint)]">Client · Trace · Lab</span>
          </span>
        </Link>
        <nav className="flex-1 space-y-0.5 overflow-y-auto px-2.5">
          {GLOBAL_NAV.map(([label, to, icon]) => (
            <NavLink key={to} label={label} to={to} icon={icon} active={isActive(to)} />
          ))}
          {/* 当前集群层级 */}
          <div className="px-1 pb-1 pt-3 text-[10.5px] font-semibold uppercase tracking-wider text-[var(--text-faint)]">
            <div className="flex items-center gap-1.5">
              <span className={`inline-block h-1.5 w-1.5 rounded-full ${dot}`} />
              <span className="min-w-0 flex-1 truncate normal-case" title={active.name}>{active.name}</span>
            </div>
          </div>
          {clusterNav.map(([label, to, icon]) => (
            <NavLink key={to} label={label} to={to} icon={icon} active={isActive(to)} indent />
          ))}
          <div className="px-3 pb-1 pt-3 text-[10.5px] font-semibold uppercase tracking-wider text-[var(--text-faint)]">
            工具
          </div>
          {toolNav.map(([label, to, icon]) => (
            <NavLink key={to} label={label} to={to} icon={icon} active={isActive(to)} />
          ))}
        </nav>
        {/* 模式与退出 */}
        <div className="flex items-center gap-1.5 px-3.5 pb-1 text-[10.5px]">
          <span className={`inline-block h-1.5 w-1.5 rounded-full ${readonly ? "bg-sky-400" : "bg-amber-300"}`} />
          <span className="text-[var(--text-faint)]">{readonly ? "只读巡检" : "全部功能"}</span>
          <button onClick={logout} className="ml-auto text-slate-500 hover:text-slate-200" title="退出登录">退出</button>
        </div>
        {/* 集群切换 */}
        <div className="border-t border-[var(--border-soft)] p-2.5">
          <div className="mb-1.5 flex items-center gap-1.5 px-1 text-[10.5px] font-semibold uppercase tracking-wider text-[var(--text-faint)]">
            <span className={`inline-block h-1.5 w-1.5 rounded-full ${dot} ${h?.connected == null ? "" : h.connected ? "" : ""}`} />
            集群
            <button onClick={() => probe(active.id)} className="ml-auto rounded px-1 text-[12px] hover:bg-white/10 hover:text-slate-200" title="探测当前集群">↻</button>
          </div>
          <select value={active.id} onChange={(e) => {
              select(e.target.value);
              const c = clusters.find((x) => x.id === e.target.value);
              if (c) reportConn({ ok: null, bootstrap: c.bootstrap, detail: c.name });
            }}
            className="field mono !h-[32px] !text-[11.5px]">
            {clusters.map((c) => (
              <option key={c.id} value={c.id}>{c.name}</option>
            ))}
          </select>
          <div className="mono mt-1 truncate px-1 text-[10px] text-[var(--text-faint)]" title={h?.error ?? h?.connected === false ? "未连接" : active.bootstrap}>
            {h?.connected ? `已连接${h?.brokerCount != null ? ` · ${h.brokerCount} nodes` : ""}` : h?.connected === false ? "未连接 · 点↻重测" : active.bootstrap}
          </div>
        </div>
      </aside>

      {/* 主区 */}
      <div className="min-w-0 flex-1 pl-[196px]">
        <header className="sticky top-0 z-20 border-b border-[var(--border-soft)] bg-[rgba(6,10,19,0.88)] backdrop-blur">
          <div className="mx-auto flex max-w-[1500px] items-center gap-3 px-4 py-2.5">
            <span className="text-[13px] font-semibold text-slate-200">{cur?.[0] ?? ""}</span>
            <span className="text-[11px] text-[var(--text-faint)]">/ {active.name}</span>
            <span className="ml-auto flex items-center gap-2">
              <span className="badge vio hidden sm:inline-flex">{active.securityProtocol}</span>
              {status}
            </span>
          </div>
        </header>
        <main className="mx-auto max-w-[1500px] px-4 py-4">
          {unlocked && (
            <div className="mb-3 flex items-center gap-2 rounded-lg border border-[rgba(251,191,36,0.35)] bg-[rgba(251,191,36,0.07)] px-3 py-1.5 text-[11.5px] text-amber-200/90">
              <span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-amber-300" />
              实验模式 · 写入已解锁（发送 / 压测 / 入组 / 提交 / 故障）
              <button onClick={lock} className="ml-auto rounded px-1.5 py-0.5 text-amber-200/80 hover:bg-white/10 hover:text-amber-100">
                退出锁定
              </button>
            </div>
          )}
          {children}</main>
      </div>
    </div>
  );
}

export interface ConnState {
  ok: boolean | null;
  bootstrap: string;
  detail?: string;
}

export function ConnBar({ conn }: { conn: ConnState }) {
  const dot = conn.ok == null ? "bg-slate-500" : conn.ok ? "bg-emerald-400" : "bg-rose-400";
  const text = conn.ok == null ? "检测中" : conn.ok ? "已连接" : "未连接";
  const tip = [conn.bootstrap, conn.detail].filter(Boolean).join(" · ");
  return (
    <span title={tip} className="inline-flex cursor-default items-center gap-1.5 rounded-full border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-1 text-[11px]">
      <span className={`inline-block h-1.5 w-1.5 rounded-full ${dot} ${conn.ok === false ? "pulsing" : ""}`} />
      <span className="font-semibold text-slate-200">{text}</span>
      {conn.detail && conn.ok && <span className="mono text-slate-500">{conn.detail}</span>}
    </span>
  );
}

export function Placeholder({ title, note }: { title: string; note: string }) {
  return (
    <div className="card mx-auto max-w-xl p-8 text-center">
      <div className="text-[15px] font-semibold text-slate-100">{title}</div>
      <div className="mt-2 text-[12.5px] leading-relaxed text-slate-400">{note}</div>
      <Link to="/producer" className="btn btn-info btn-sm mt-4">
        ← 返回 Producer
      </Link>
    </div>
  );
}
