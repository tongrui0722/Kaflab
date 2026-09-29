import { useEffect, useState } from "react";
import { useConn } from "../conn";
import { authHeaders } from "../auth";
import { PageHeader } from "../components/bits";
import type { Session } from "../types";

const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";

interface TopicRow { topic: string; partitions: number; replication: number; leaders: number[]; hasOffline: boolean }
interface GroupRow { groupId: string; state?: string; protocol?: string; members: number; totalLag?: number }

export function DashboardPage() {
  const { conn, clusters, select } = useConn();
  const ALL = "__all__";
  const [selId, setSelId] = useState<string>(conn.id);
  const [sessions, setSessions] = useState<Session[]>([]);
  const [topics, setTopics] = useState<TopicRow[]>([]);
  const [brokers, setBrokers] = useState<{ id?: number; host: string; port: number }[]>([]);
  const [groups, setGroups] = useState<GroupRow[]>([]);
  const [perCluster, setPerCluster] = useState<Record<string, { topics: number; brokers: number; groups: number; lag: number; err?: string }>>({});
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshSeq, setRefreshSeq] = useState(0);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);

  useEffect(() => { setSelId(conn.id); }, [conn.id]);
  const selCluster = clusters.find((c) => c.id === selId);
  const isAll = selId === ALL;

  useEffect(() => {
    let dead = false;
    (async () => {
      setLoading(true);
      setErr(null);
      try {
        const headers = authHeaders();
        const ss = await (await fetch(`${API}/api/sessions`, { headers })).json() as Session[];
        if (dead) return;
        setSessions(ss);
        if (isAll) {
          const rows = await Promise.all(clusters.map(async (c) => {
            try {
              const [t, g] = await Promise.all([
                (await fetch(`${API}/api/clusters/${c.id}/topics`, { headers })).json() as Promise<{ topics: TopicRow[]; brokers: typeof brokers }>,
                (await fetch(`${API}/api/clusters/${c.id}/groups`, { headers })).json() as Promise<{ groups: GroupRow[] }>,
              ]);
              return [c.id, { topics: t.topics.length, brokers: t.brokers.length, groups: g.groups.length, lag: g.groups.reduce((a, x) => a + (x.totalLag ?? 0), 0) }] as const;
            } catch (e) {
              return [c.id, { topics: 0, brokers: 0, groups: 0, lag: 0, err: e instanceof Error ? e.message : String(e) }] as const;
            }
          }));
          if (dead) return;
          setPerCluster(Object.fromEntries(rows));
          setTopics([]);
          setBrokers([]);
          setGroups([]);
        } else {
          const [t, g] = await Promise.all([
            (await fetch(`${API}/api/clusters/${selId}/topics`, { headers })).json() as Promise<{ topics: TopicRow[]; brokers: typeof brokers }>,
            (await fetch(`${API}/api/clusters/${selId}/groups`, { headers })).json() as Promise<{ groups: GroupRow[] }>,
          ]);
          if (dead) return;
          setTopics(t.topics);
          setBrokers(t.brokers);
          setGroups(g.groups);
        }
        setUpdatedAt(new Date());
      } catch (e) {
        if (!dead) setErr(e instanceof Error ? e.message : String(e));
      } finally {
        if (!dead) setLoading(false);
      }
    })();
    return () => { dead = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selId, clusters, refreshSeq]);

  const done = sessions.filter((s) => s.status === "success" || s.status === "error");
  const ok = sessions.filter((s) => s.status === "success").length;
  const fails = sessions.filter((s) => s.status === "error");
  const durs = sessions.map((s) => s.durationMs).filter((d): d is number => d != null);
  const avg = durs.length ? Math.round(durs.reduce((a, b) => a + b, 0) / durs.length) : null;
  const offlineTopics = topics.filter((t) => t.hasOffline);
  const topTopics = [...topics].sort((a, b) => b.partitions - a.partitions);
  const recentFails = fails.slice(0, 5);
  const recent = sessions.slice(0, 10);
  const ops = ["Producer", "Consumer", "Group", "Rebalance"] as const;
  const opCount = (op: string) => sessions.filter((s) => s.operation === op).length;

  const go = (hash: string) => { window.location.hash = hash; };
  const pick = (id: string) => {
    setSelId(id);
    if (id !== ALL) select(id);
  };

  const allTotals = Object.values(perCluster).reduce(
    (a, c) => ({ brokers: a.brokers + c.brokers, topics: a.topics + c.topics, groups: a.groups + c.groups, lag: a.lag + c.lag }),
    { brokers: 0, topics: 0, groups: 0, lag: 0 });

  const reachableClusters = Object.values(perCluster).filter((c) => !c.err).length;
  const stats: { k: string; v: React.ReactNode; sub?: string; tone?: string }[] = isAll ? [
    {
      k: "集群", v: <span className="text-[20px] font-bold text-slate-100">{clusters.length}</span>,
      sub: `${reachableClusters}/${clusters.length} 可达`, tone: "sky",
    },
    {
      k: "Brokers", v: <span className="text-[20px] font-bold text-emerald-300">{allTotals.brokers}</span>,
      sub: "全集群合计", tone: "emerald",
    },
    {
      k: "Topics", v: <span className="text-[20px] font-bold text-slate-100">{allTotals.topics}</span>,
      sub: "全集群合计", tone: "blue",
    },
    {
      k: "消费组", v: <span className="text-[20px] font-bold text-slate-100">{allTotals.groups}</span>,
      sub: allTotals.lag > 0 ? `总 LAG ${allTotals.lag}` : "无积压", tone: allTotals.lag > 0 ? "amber" : "emerald",
    },
    {
      k: "成功率", v: <span className="text-[20px] font-bold text-slate-100">{done.length ? `${Math.round((ok / done.length) * 100)}%` : "—"}</span>,
      sub: `${ok}/${done.length} 成功`, tone: "emerald",
    },
    {
      k: "失败", v: <span className={`text-[20px] font-bold ${fails.length ? "text-rose-300" : "text-slate-500"}`}>{fails.length}</span>,
      sub: fails.length ? "见下方明细" : "零失败", tone: fails.length ? "rose" : "slate",
    },
  ] : [
    {
      k: "集群", v: brokers.length
        ? <span className="text-[20px] font-bold text-emerald-300">{brokers.length} <span className="text-[12px] font-semibold">brokers</span></span>
        : loading ? <span className="text-[14px] text-slate-500">同步中</span>
        : <span className="text-[14px] text-rose-300">不可达</span>,
      sub: selCluster?.name ?? conn.name,
      tone: loading ? "sky" : brokers.length ? "emerald" : "rose",
    },
    {
      k: "Topics", v: <span className="text-[20px] font-bold text-slate-100">{topics.length}</span>,
      sub: offlineTopics.length ? `${offlineTopics.length} 个离线分区 ⚠` : "全部健康", tone: loading ? "sky" : offlineTopics.length ? "rose" : "blue",
    },
    {
      k: "消费组", v: <span className="text-[20px] font-bold text-slate-100">{groups.length}</span>,
      sub: groups.length ? `${groups.filter((g) => (g.state ?? "") !== "Empty").length} 活跃` : "暂无", tone: "violet",
    },
    {
      k: "成功率", v: <span className="text-[20px] font-bold text-slate-100">{done.length ? `${Math.round((ok / done.length) * 100)}%` : "—"}</span>,
      sub: `${ok}/${done.length} 成功`, tone: "emerald",
    },
    {
      k: "平均耗时", v: <span className="text-[20px] font-bold text-slate-100">{avg != null ? `${avg}` : "—"}<span className="text-[12px] font-semibold text-slate-400"> ms</span></span>,
      sub: `${durs.length} 次统计`, tone: "sky",
    },
    {
      k: "失败", v: <span className={`text-[20px] font-bold ${fails.length ? "text-rose-300" : "text-slate-500"}`}>{fails.length}</span>,
      sub: fails.length ? "见下方明细" : "零失败", tone: fails.length ? "rose" : "slate",
    },
  ];

  return (
    <div>
      <PageHeader
        title="概览"
        desc={isAll ? "全部集群 · 健康与数据总览" : `${selCluster?.name ?? conn.name} · ${selCluster?.bootstrap ?? conn.bootstrap} · 单集群视图`}
        right={
          <>
            <button onClick={() => go("#/producer")} className="btn btn-send btn-sm !w-auto">去发送</button>
            <button onClick={() => go("#/consumer")} className="btn btn-fetch btn-sm !w-auto">去拉取</button>
            <button onClick={() => go("#/topics")} className="btn btn-ghost btn-sm">Topics</button>
            <button onClick={() => setRefreshSeq((n) => n + 1)} disabled={loading}
              className="btn btn-ghost btn-sm" title="重新读取 Kafka 实时数据">
              {loading ? "同步中…" : "↻ 刷新"}
            </button>
            {updatedAt && <span className="hidden text-[10.5px] text-[var(--text-faint)] lg:inline">更新于 {updatedAt.toLocaleTimeString("zh-CN", { hour12: false })}</span>}
          </>
        }
      />

      {/* 集群切换 */}
      <div className="mb-3 flex flex-wrap items-center gap-1.5">
        <button onClick={() => pick(ALL)}
          className={`rounded-lg px-2.5 py-1 text-[12px] transition-colors ${isAll ? "bg-[rgba(56,189,248,0.15)] font-semibold text-sky-200" : "text-slate-400 hover:bg-white/[0.04] hover:text-slate-200"}`}>
          全部集群
        </button>
        {clusters.map((c) => {
          const pc = perCluster[c.id];
          const bad = isAll && pc?.err;
          return (
            <button key={c.id} onClick={() => pick(c.id)} title={bad ? pc.err : c.bootstrap}
              className={`flex items-center gap-1.5 rounded-lg px-2.5 py-1 text-[12px] transition-colors ${!isAll && c.id === selId ? "bg-[rgba(56,189,248,0.15)] font-semibold text-sky-200" : "text-slate-400 hover:bg-white/[0.04] hover:text-slate-200"}`}>
              <span className={`inline-block h-1.5 w-1.5 rounded-full ${bad ? "bg-rose-400" : c.id === conn.id ? "bg-emerald-400" : "bg-slate-500"}`} />
              {c.name}
            </button>
          );
        })}
      </div>

      {err && <div className="alert err mb-3">加载失败：{err}</div>}

      {/* 指标 */}
      <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-3 xl:grid-cols-6">
        {stats.map((s) => (
          <div key={s.k} className={`metric-card metric-${s.tone ?? "slate"} fade-up`}>
            <div className="metric-label">{s.k}</div>
            <div className="mono mt-1.5 min-h-[30px]">
              {loading ? <span className="inline-block h-6 w-16 animate-pulse rounded bg-white/[0.07]" /> : s.v}
            </div>
            <div className="mt-1 min-h-[17px] truncate text-[11px] text-[var(--text-faint)]">
              {loading ? "正在同步实时数据…" : s.sub}
            </div>
          </div>
        ))}
      </div>

      {isAll ? (
        <div className="card mt-3 p-3.5">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-100">各集群</span>
            <span className="badge mute">{clusters.length} 个</span>
            <span className="ml-auto text-[11px] text-[var(--text-faint)]">点卡片进入该集群视图</span>
          </div>
          {loading && Object.keys(perCluster).length === 0 ? (
            <div className="py-4 text-center text-[12px] text-slate-500">探测中…</div>
          ) : (
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
              {clusters.map((c) => {
                const pc = perCluster[c.id];
                return (
                  <button key={c.id} onClick={() => pick(c.id)}
                    className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-3 py-2.5 text-left transition-colors hover:border-sky-500/40">
                    <div className="flex items-center gap-2">
                      <span className={`inline-block h-2 w-2 shrink-0 rounded-full ${pc?.err ? "bg-rose-400" : pc ? "bg-emerald-400" : "bg-slate-500"}`} />
                      <span className="mono min-w-0 flex-1 truncate text-[12.5px] font-semibold text-slate-100">{c.name}</span>
                      {c.id === conn.id && <span className="badge run shrink-0">当前</span>}
                    </div>
                    {loading && !pc ? (
                      <div className="mt-1 text-[11px] text-slate-500">正在检查连接…</div>
                    ) : pc?.err ? (
                      <div className="mono mt-1 truncate text-[11px] text-rose-300/80" title={pc.err}>不可达</div>
                    ) : pc ? (
                      <div className="mono mt-1.5 flex gap-3 text-[11px] text-[var(--text-faint)]">
                        <span><b className="text-slate-200">{pc.brokers}</b> brokers</span>
                        <span><b className="text-slate-200">{pc.topics}</b> topics</span>
                        <span><b className="text-slate-200">{pc.groups}</b> 消费组</span>
                        {pc.lag > 0 && <span className="text-amber-300">LAG {pc.lag}</span>}
                      </div>
                    ) : (
                      <div className="mono mt-1 text-[11px] text-slate-600">探测中…</div>
                    )}
                  </button>
                );
              })}
            </div>
          )}
        </div>
      ) : (
      <>
      {/* Brokers */}
      <div className="card mt-3 p-3.5">
        <div className="card-t">
          <span className="text-[13px] font-bold text-slate-100">Brokers</span>
          <span className="badge mute">{brokers.length} nodes</span>
          <span className="ml-auto text-[11px] text-[var(--text-faint)]">来自真实 Metadata · 涉及 topics 数按 Leaders 归属统计</span>
        </div>
        {loading ? (
          <div className="py-4 text-center text-[12px] text-slate-500">正在读取集群 Metadata…</div>
        ) : brokers.length === 0 ? (
          <div className="py-4 text-center text-[12px] text-slate-500">集群不可达。</div>
        ) : (
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
            {brokers.map((b) => {
              const involved = topics.filter((t) => t.leaders.includes(b.id ?? -1)).length;
              return (
                <div key={b.id ?? `${b.host}:${b.port}`} className="flex items-center gap-3 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-3 py-2.5">
                  <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-[rgba(56,189,248,0.12)] text-[13px] font-bold text-sky-300">#{b.id}</span>
                  <div className="min-w-0">
                    <div className="mono truncate text-[12px] text-slate-200">{b.host}:{b.port}</div>
                    <div className="mono text-[11px] text-[var(--text-faint)]">{involved} topics involved</div>
                  </div>
                  <span className="badge ok ml-auto">up</span>
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div className="layout-grid layout-2eq mt-3">
        {/* 热门 Topics */}
        <div className="card flex min-h-0 flex-col p-3.5">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-100">Topics</span>
            <span className="badge mute">{topics.length}</span>
            <button onClick={() => go("#/topics")} className="btn btn-ghost btn-sm ml-auto">全部 →</button>
          </div>
          {loading ? <div className="py-4 text-center text-[12px] text-slate-500">正在同步 Topic…</div>
            : topTopics.length === 0 && <div className="py-4 text-center text-[12px] text-slate-500">暂无 Topic。</div>}
          <div className="scroll-box max-h-[300px] min-h-[120px] flex-1 space-y-1 overflow-y-auto overscroll-contain pr-1">
            {!loading && topTopics.map((t) => (
              <button key={t.topic} onClick={() => go("#/topics")}
                className="flex w-full items-center gap-2 rounded-lg px-2 py-1.5 text-left transition-colors hover:bg-white/[0.03]">
                <span className="mono min-w-0 flex-1 truncate text-[12px] text-slate-200">{t.topic}</span>
                <span className="mono shrink-0 text-[11px] text-[var(--text-faint)]">{t.partitions}P · RF{t.replication}</span>
                {t.hasOffline ? <span className="badge err shrink-0">OFFLINE</span> : <span className="badge ok shrink-0">OK</span>}
              </button>
            ))}
          </div>
        </div>

        {/* 消费组 */}
        <div className="card flex min-h-0 flex-col p-3.5">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-100">消费组</span>
            <span className="badge mute">{groups.length}</span>
            <button onClick={() => go("#/group")} className="btn btn-ghost btn-sm ml-auto">管理 →</button>
          </div>
          {!loading && groups.length === 0 && (
            <div className="py-4 text-center text-[12px] text-slate-500">
              暂无消费组（演示组用完即离组）。去 Group 页 Join 一次就会出现。
            </div>
          )}
          {loading ? <div className="py-4 text-center text-[12px] text-slate-500">正在同步消费组…</div> : groups.length > 0 && (
          <div className="scroll-box max-h-[300px] min-h-[120px] flex-1 space-y-1 overflow-y-auto overscroll-contain pr-1">
            {groups.slice(0, 30).map((g) => (
              <button key={g.groupId} onClick={() => go("#/group")}
                className="flex w-full items-center gap-2 rounded-lg px-2 py-1.5 text-left transition-colors hover:bg-white/[0.03]">
                <span className="mono min-w-0 flex-1 truncate text-[12px] text-slate-200" title={g.groupId}>{g.groupId}</span>
                {(g.totalLag ?? 0) > 0
                  ? <span className="badge warn shrink-0">LAG {g.totalLag}</span>
                  : <span className="badge ok shrink-0">0</span>}
                <span className="mono shrink-0 text-[11px] text-[var(--text-faint)]">{g.members} 成员</span>
                <span className={`badge shrink-0 ${(g.state ?? "") === "Empty" ? "mute" : "run"}`}>{g.state ?? "?"}</span>
              </button>
            ))}
          </div>
          )}
          {/* 操作分布 */}
          <div className="mt-3 border-t border-[var(--border-soft)] pt-2.5">
            <div className="card-title mb-1.5">操作分布（Sessions）</div>
            <div className="flex flex-wrap gap-1.5">
              {ops.map((o) => (
                <span key={o} className="badge mute !text-[11px] !py-1">{o} × {opCount(o)}</span>
              ))}
            </div>
          </div>
        </div>
      </div>
      </>
      )}

      {/* 失败 + 最近 */}
      <div className="layout-grid layout-2eq mt-3">
        <div className="card flex min-h-0 flex-col p-3.5">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-100">最近失败</span>
            {fails.length > 0 && <span className="badge err">{fails.length}</span>}
          </div>
          {recentFails.length === 0 && <div className="py-4 text-center text-[12px] text-slate-500">零失败，链路健康。</div>}
          <div className="scroll-box max-h-[320px] min-h-[120px] flex-1 space-y-1 overflow-y-auto overscroll-contain pr-1">
            {recentFails.map((s) => (
              <div key={s.id} className="mono rounded-lg border border-[rgba(251,113,133,0.25)] bg-[rgba(251,113,133,0.05)] px-2.5 py-1.5 text-[11px]">
                <div className="flex items-center gap-2">
                  <span className="text-[var(--text-faint)]">{new Date(s.startTime).toLocaleTimeString("zh-CN", { hour12: false })}</span>
                  <span className="truncate text-slate-300">{s.operation} {s.topic}</span>
                </div>
                <div className="mt-0.5 truncate text-rose-300/90">{s.error}</div>
              </div>
            ))}
          </div>
        </div>
        <div className="card flex min-h-0 flex-col p-3.5">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-100">最近操作</span>
            <span className="badge mute">{sessions.length}</span>
          </div>
          {recent.length === 0 && <div className="py-4 text-center text-[12px] text-slate-500">暂无 Session。去 Producer 发送第一条真实消息。</div>}
          <div className="scroll-box max-h-[320px] min-h-[120px] flex-1 space-y-[3px] overflow-y-auto overscroll-contain pr-1">
            {recent.map((s) => (
              <div key={s.id} className="mono flex items-center gap-2.5 rounded-md px-2 py-1 text-[11px] text-slate-300 transition-colors hover:bg-white/[0.03]">
                <span className="shrink-0 text-[var(--text-faint)]">{new Date(s.startTime).toLocaleTimeString("zh-CN", { hour12: false })}</span>
                <span className="shrink-0 text-slate-400">{s.operation}</span>
                <span className="min-w-0 flex-1 truncate">{s.topic}{s.partition != null ? ` P${s.partition}` : ""}{s.offset != null ? ` @${s.offset}` : ""}</span>
                <span className={`shrink-0 font-bold ${s.status === "success" ? "text-emerald-300" : s.status === "error" ? "text-rose-300" : "text-sky-300"}`}>
                  {s.status === "success" ? "✓" : s.status === "error" ? "✕" : "…"}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
