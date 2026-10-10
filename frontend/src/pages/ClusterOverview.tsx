import { useCallback, useEffect, useMemo, useState } from "react";
import { useConn } from "../conn";
import { authHeaders } from "../auth";
import { GroupStateBadge, PageHeader } from "../components/bits";

const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";

type Summary = {
  brokerCount: number; topicCount: number; partitionCount: number;
  offlineTopics?: number; underReplicatedTopics?: number;
  offlinePartitionCount?: number; underReplicatedPartitionCount?: number; topicsWithIssues?: number;
  groupCount: number; memberCount?: number; totalLag: number | null;
  lagState?: "complete" | "partial" | "unavailable"; lagUnknownGroups?: number; lagUnknownPartitions?: number;
};
type Group = { groupId: string; state?: string; memberCount?: number; members?: number; totalLag?: number | null;
  uncommitted?: number; error?: string };
type Topic = { topic: string; partitions: number; replication: number; leaders: number[]; hasOffline: boolean;
  hasUnderReplicated?: boolean; offlinePartitions?: number; underReplicatedPartitions?: number };
type Broker = { id?: number; host: string; port: number; leaders?: number; replicas?: number; isr?: number };
type Overview = { summary: Summary; brokers: Broker[]; brokerStats?: Broker[]; lagTop: Group[]; groups: Group[];
  topics: Topic[]; collector: { running: boolean; last_ok: number | null; last_error: string | null };
  collectedAt: number; errors: string[]; lagNote: string };
type Pt = { series: string; value: number; ts: number };

const COLORS = ["#4b88d7", "#1b9b83", "#c48123", "#cc6a9b", "#7669cf", "#d65267", "#27a6b8", "#6d9e42"];
const LIVE_LAG_RETENTION_SECONDS = 3 * 24 * 60 * 60;
const LIVE_LAG_CACHE_LIMIT = 5000;

function readLiveLagCache(clusterId: string): Pt[] {
  try {
    const raw = localStorage.getItem(`kcl:lag-history:${clusterId}`);
    if (!raw) return [];
    const cutoff = Math.floor(Date.now() / 1000) - LIVE_LAG_RETENTION_SECONDS;
    const points = JSON.parse(raw) as Pt[];
    if (!Array.isArray(points)) return [];
    return points.filter((point) => typeof point?.series === "string" &&
      Number.isFinite(point.value) && Number.isFinite(point.ts) && point.ts >= cutoff)
      .slice(-LIVE_LAG_CACHE_LIMIT);
  } catch {
    return [];
  }
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { headers: authHeaders() });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body?.detail || `请求失败 (${response.status})`);
  return body as T;
}

function timeLabel(ts: number) {
  return new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false, hour: "2-digit", minute: "2-digit" });
}

function niceMax(value: number) {
  if (!Number.isFinite(value) || value <= 0) return 1;
  const power = 10 ** Math.floor(Math.log10(value));
  const fraction = value / power;
  const step = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return step * power;
}

function TrendChart({ title, description, unit, pts, hours, seriesName, emptyText }: {
  title: string; description?: string; unit: string; pts: Pt[]; hours: number;
  seriesName: string; emptyText: string;
}) {
  const points = useMemo(() => [...pts].sort((a, b) => a.ts - b.ts), [pts]);
  const now = Math.floor(Date.now() / 1000);
  const x0 = now - hours * 3600;
  const x1 = now;
  const maxValue = Math.max(0, ...points.map((p) => p.value));
  const yMax = niceMax(maxValue * 1.12);
  const w = 900, h = 310, left = 62, right = 18, top = 22, bottom = 42;
  const plotW = w - left - right, plotH = h - top - bottom;
  const x = (ts: number) => left + ((ts - x0) / Math.max(1, x1 - x0)) * plotW;
  const y = (value: number) => top + plotH - (Math.max(0, value) / yMax) * plotH;
  const gapLimit = Math.min(1800, Math.max(90, hours * 3600 / 30));
  const segments: Pt[][] = [];
  for (const point of points) {
    const last = segments[segments.length - 1];
    if (!last || point.ts - last[last.length - 1].ts > gapLimit) segments.push([point]);
    else last.push(point);
  }
  const stats = points.length ? {
    latest: points[points.length - 1].value,
    peak: maxValue,
    average: points.reduce((sum, p) => sum + p.value, 0) / points.length,
    change: points[points.length - 1].value - points[0].value,
  } : null;
  const yTicks = [0, 1, 2, 3, 4].map((i) => yMax * i / 4);
  const xTicks = [0, 1, 2, 3, 4].map((i) => x0 + (x1 - x0) * i / 4);

  return (
    <section className="card flex min-w-0 flex-col p-4">
      <div className="flex flex-wrap items-start gap-x-4 gap-y-2">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="text-[13px] font-bold text-slate-800">{title}</span>
            <span className="badge mute">{unit}</span>
          </div>
          {description && <p className="mt-1 text-[11px] text-[var(--text-faint)]">{description}</p>}
        </div>
        {stats && <div className="flex flex-wrap gap-x-4 gap-y-1 text-[11px]">
          <span className="text-slate-400">当前 <b className="mono ml-1 text-slate-800">{stats.latest.toLocaleString("zh-CN", { maximumFractionDigits: 1 })}</b></span>
          <span className="text-slate-400">峰值 <b className="mono ml-1 text-slate-800">{stats.peak.toLocaleString("zh-CN", { maximumFractionDigits: 1 })}</b></span>
          <span className="text-slate-400">均值 <b className="mono ml-1 text-slate-800">{stats.average.toLocaleString("zh-CN", { maximumFractionDigits: 1 })}</b></span>
          <span className="text-slate-400">变化 <b className={`mono ml-1 ${stats.change > 0 ? "text-amber-700" : stats.change < 0 ? "text-emerald-700" : "text-slate-800"}`}>
            {stats.change > 0 ? "+" : ""}{stats.change.toLocaleString("zh-CN", { maximumFractionDigits: 1 })}</b></span>
        </div>}
      </div>
      {points.length === 0 ? (
        <div className="mt-4 flex min-h-[190px] flex-col items-center justify-center rounded-xl border border-dashed border-[var(--border-soft)] bg-[var(--panel-3)] px-5 text-center">
          <div className="text-[13px] font-semibold text-slate-600">暂无历史采样</div>
          <div className="mt-1 max-w-lg text-[11px] leading-5 text-[var(--text-faint)]">{emptyText}</div>
        </div>
      ) : (
        <div className="mt-3 overflow-hidden rounded-xl border border-[var(--border-soft)] bg-[var(--panel-3)]">
          <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" className="block h-[260px] w-full" role="img" aria-label={`${title}趋势图`}>
            {yTicks.map((tick, i) => <g key={`y-${i}`}>
              <line x1={left} x2={w - right} y1={y(tick)} y2={y(tick)} stroke="#d7e0ea" strokeDasharray={i === 0 ? "0" : "3 5"} />
              <text x={left - 10} y={y(tick) + 4} fill="#71849a" fontSize="11" textAnchor="end">{tick.toLocaleString("zh-CN", { maximumFractionDigits: 1 })}</text>
            </g>)}
            {xTicks.map((tick, i) => <g key={`x-${i}`}>
              <line x1={x(tick)} x2={x(tick)} y1={top} y2={top + plotH} stroke="#e1e7ef" strokeDasharray="2 6" />
              <text x={x(tick)} y={h - 13} fill="#71849a" fontSize="11" textAnchor={i === 0 ? "start" : i === 4 ? "end" : "middle"}>{timeLabel(tick)}</text>
            </g>)}
            {segments.map((segment, index) => <g key={`seg-${index}`}>
              {segment.length > 1 && <path d={`${segment.map((p, i) => `${i ? "L" : "M"}${x(p.ts).toFixed(1)},${y(p.value).toFixed(1)}`).join(" ")} L${x(segment[segment.length - 1].ts).toFixed(1)},${y(0).toFixed(1)} L${x(segment[0].ts).toFixed(1)},${y(0).toFixed(1)} Z`}
                fill={COLORS[0]} fillOpacity="0.12" stroke="none" />}
              {segment.length > 1 && <path d={segment.map((p, i) => `${i ? "L" : "M"}${x(p.ts).toFixed(1)},${y(p.value).toFixed(1)}`).join(" ")}
                fill="none" stroke={COLORS[0]} strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />}
              {segment.map((p, i) => <circle key={`${p.ts}-${i}`} cx={x(p.ts)} cy={y(p.value)} r={points.length > 80 ? 2 : 3.5}
                fill={COLORS[0]} stroke="#f4f7fb" strokeWidth="1.5">
                <title>{new Date(p.ts * 1000).toLocaleString("zh-CN")} · {seriesName}：{p.value.toLocaleString("zh-CN", { maximumFractionDigits: 2 })} {unit}</title>
              </circle>)}
            </g>)}
          </svg>
        </div>
      )}
      <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-[10.5px] text-[var(--text-faint)]">
        <span>{seriesName} · {points.length} 个采样点</span>
        {points.length > 0 && <span>{timeLabel(points[0].ts)} 至 {timeLabel(points[points.length - 1].ts)}</span>}
      </div>
    </section>
  );
}

function StateLabel({ state }: { state?: string }) {
  return <GroupStateBadge state={state} />;
}

function SummaryCard({ title, value, detail, tone = "blue" }: { title: string; value: string | number; detail: string; tone?: string }) {
  return <div className={`metric-card metric-${tone} min-w-0`}>
    <div className="flex items-center gap-2">
      <span className="neu-chip" aria-hidden="true" />
      <div className="metric-label">{title}</div>
    </div>
    <div className="mono mt-1.5 truncate text-[21px] font-bold text-slate-800" title={String(value)}>{value}</div>
    <div className="mt-1 truncate text-[11px] text-[var(--text-faint)]" title={detail}>{detail}</div>
  </div>;
}

function HealthRing({ pct }: { pct: number | null }) {
  if (pct == null) return null;
  const r = 15.5, c = 2 * Math.PI * r;
  const v = Math.max(0, Math.min(100, pct));
  const color = v >= 99 ? "#1b9b83" : v >= 90 ? "#4c8fe8" : "#c48123";
  return (
    <span className="flex items-center gap-1.5" title={`健康分区占比 ${v.toFixed(1)}%`}>
      <svg width="40" height="40" viewBox="0 0 40 40" role="img" aria-label={`健康分区占比 ${Math.round(v)}%`}>
        <circle cx="20" cy="20" r={r} fill="none" stroke="#e2e9f1" strokeWidth="5" />
        <circle cx="20" cy="20" r={r} fill="none" stroke={color} strokeWidth="5"
          strokeLinecap="round" strokeDasharray={`${(v / 100) * c} ${c}`}
          transform="rotate(-90 20 20)" />
        <text x="20" y="23.5" textAnchor="middle" fontSize="10" fontWeight="700" fill="#1d2d43">{Math.round(v)}</text>
      </svg>
    </span>
  );
}

export function ClusterOverviewPage() {
  const { conn } = useConn();
  const [ov, setOv] = useState<Overview | null>(null);
  const [lag, setLag] = useState<Pt[]>([]);
  const [liveLag, setLiveLag] = useState<Pt[]>([]);
  const [hours, setHours] = useState(1);
  const [refresh, setRefresh] = useState(30);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  const [selectedLagSeries, setSelectedLagSeries] = useState("__total__");
  const [showInternalTopics, setShowInternalTopics] = useState(false);

  useEffect(() => {
    setOv(null);
    setLag([]);
    setLiveLag(readLiveLagCache(conn.id));
    setUpdatedAt(null);
  }, [conn.id]);

  const load = useCallback(async () => {
    if (!conn.id) return;
    setBusy(true);
    setErr(null);
    try {
      const q = `hours=${hours}`;
      // Overview also records samples. Fetch it first so the trend query cannot
      // race ahead of the write and miss the newest point on an empty database.
      const snapshot = await getJson<Overview>(`${API}/api/clusters/${conn.id}/overview`);
      setOv(snapshot);
      const sampleTs = snapshot.collectedAt ? Math.floor(snapshot.collectedAt / 1000) : Math.floor(Date.now() / 1000);
      const current: Pt[] = (snapshot.groups ?? [])
        .filter((group) => !group.error && typeof group.totalLag === "number")
        .map((group) => ({ series: group.groupId, value: group.totalLag as number, ts: sampleTs }));
      if (current.length) {
        const total = current.reduce((sum, point) => sum + point.value, 0);
        current.push({ series: "__total__", value: total, ts: sampleTs });
        setLiveLag((previous) => {
          const merged = new Map<string, Pt>();
          [...previous.filter((point) => point.ts >= sampleTs - LIVE_LAG_RETENTION_SECONDS), ...current].forEach((point) => {
            merged.set(`${point.series}:${point.ts}`, point);
          });
          const points = [...merged.values()].sort((a, b) => a.ts - b.ts).slice(-LIVE_LAG_CACHE_LIMIT);
          try {
            localStorage.setItem(`kcl:lag-history:${conn.id}`, JSON.stringify(points));
          } catch {
            // Browser storage can be disabled or full; server history remains the source of record.
          }
          return points;
        });
      }
      setUpdatedAt(snapshot.collectedAt ? new Date(snapshot.collectedAt) : new Date());
      // Keep the live overview useful even if the persisted history endpoint is
      // temporarily stale or unavailable. Session samples continue to render.
      try {
        const trend = await getJson<{ points?: Pt[] }>(`${API}/api/clusters/${conn.id}/trends?metric=group_lag&${q}`);
        setLag(trend.points ?? []);
      } catch {
        setLag([]);
      }
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }, [conn.id, hours]);

  const chartLag = useMemo(() => {
    const cutoff = Math.floor(Date.now() / 1000) - hours * 3600;
    const merged = new Map<string, Pt>();
    // Persisted points win on duplicate timestamps; current overview samples
    // fill gaps and provide live data while the collector history catches up.
    [...liveLag.filter((point) => point.ts >= cutoff), ...lag.filter((point) => point.ts >= cutoff)]
      .forEach((point) => merged.set(`${point.series}:${point.ts}`, point));
    return [...merged.values()].sort((a, b) => a.ts - b.ts);
  }, [lag, liveLag, hours]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!refresh) return;
    const timer = window.setInterval(() => { if (document.visibilityState === "visible") void load(); }, refresh * 1000);
    return () => window.clearInterval(timer);
  }, [load, refresh]);

  const summary = ov?.summary;
  const lagOptions = useMemo(() => {
    const available = new Set(chartLag.map((point) => point.series));
    const options: { value: string; label: string }[] = [];
    if (available.has("__total__")) options.push({ value: "__total__", label: "集群总 Lag" });
    const top = (ov?.lagTop ?? []).filter((g) => available.has(g.groupId));
    top.forEach((g) => options.push({ value: g.groupId, label: g.groupId }));
    const added = new Set(options.map((option) => option.value));
    [...available].filter((id) => id !== "__total__" && !added.has(id)).slice(0, 8)
      .forEach((id) => options.push({ value: id, label: id }));
    return options;
  }, [chartLag, ov?.lagTop]);
  const activeSeries = lagOptions.some((option) => option.value === selectedLagSeries)
    ? selectedLagSeries : lagOptions[0]?.value ?? "__total__";
  const selectedGroup = ov?.groups.find((group) => group.groupId === activeSeries);
  const chartPoints = chartLag.filter((point) => point.series === activeSeries);
  const allGroups = useMemo(() => {
    const list = (ov?.groups?.length ? ov.groups : ov?.lagTop ?? []).slice();
    const members = (g: Group) => g.memberCount ?? g.members ?? 0;
    const lagOf = (g: Group) => (typeof g.totalLag === "number" ? g.totalLag : -1);
    return list.sort((a, b) => {
      const onlineA = members(a) > 0 ? 0 : 1;
      const onlineB = members(b) > 0 ? 0 : 1;
      if (onlineA !== onlineB) return onlineA - onlineB;
      return lagOf(b) - lagOf(a);
    });
  }, [ov?.groups, ov?.lagTop]);
  const brokerStats = ov?.brokerStats?.length ? ov.brokerStats : ov?.brokers ?? [];
  const hasBrokerDistribution = Boolean(ov?.brokerStats?.length);
  const maxLeaders = Math.max(1, ...brokerStats.map((broker) => broker.leaders ?? 0));
  const issues = ov?.topics.filter((topic) => (topic.offlinePartitions ?? (topic.hasOffline ? 1 : 0)) > 0 ||
    (topic.underReplicatedPartitions ?? (topic.hasUnderReplicated ? 1 : 0)) > 0) ?? [];
  const lagText = summary?.totalLag == null ? "未知" : summary.lagState === "partial"
    ? `~${summary.totalLag.toLocaleString("zh-CN")}` : summary.totalLag.toLocaleString("zh-CN");
  const offlineTopicCount = summary?.offlineTopics ?? ov?.topics.filter((topic) => topic.hasOffline).length ?? 0;
  const underReplicatedTopicCount = summary?.underReplicatedTopics ?? ov?.topics.filter((topic) => topic.hasUnderReplicated).length ?? 0;
  const offlineCountForHealth = summary?.offlinePartitionCount ?? offlineTopicCount;
  const underReplicatedCountForHealth = summary?.underReplicatedPartitionCount ?? underReplicatedTopicCount;
  const healthText = !ov ? "加载中" : offlineCountForHealth + underReplicatedCountForHealth > 0
    ? "需要关注" : ov.errors?.length ? "数据不完整" : "正常";
  const healthTone = healthText === "正常" ? "emerald" : healthText === "加载中" ? "blue" : "rose";
  const groupStates = useMemo(() => {
    const canonical: Record<string, string> = {
      stable: "Stable", empty: "Empty", dead: "Dead",
      preparingrebalancing: "PreparingRebalancing",
      completingrebalancing: "CompletingRebalancing",
    };
    const counts = new Map<string, number>();
    (ov?.groups ?? []).forEach((group) => {
      const raw = group.state || "unknown";
      const key = canonical[raw.toLowerCase()] ?? raw;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    });
    return [...counts.entries()].sort((a, b) => b[1] - a[1]);
  }, [ov?.groups]);
  const healthyPct = useMemo(() => {
    const total = summary?.partitionCount ?? 0;
    if (!ov || total <= 0) return null;
    const bad = (summary?.offlinePartitionCount ?? 0)
      + (summary?.underReplicatedPartitionCount ?? 0);
    return Math.max(0, Math.min(100, ((total - bad) / total) * 100));
  }, [ov, summary]);
  const topicTop = useMemo(() => [...(ov?.topics ?? [])]
    .filter((topic) => showInternalTopics || !topic.topic.startsWith("_"))
    .sort((a, b) => b.partitions - a.partitions || a.topic.localeCompare(b.topic)),
    [ov?.topics, showInternalTopics]);
  const internalTopicCount = ov?.topics.filter((topic) => topic.topic.startsWith("_")).length ?? 0;

  return <div className="space-y-3">
    <PageHeader title={`集群概览 · ${conn.name}`} desc={conn.bootstrap} right={
      <div className="flex flex-wrap items-center justify-end gap-2">
        <span className={`badge ${healthTone === "emerald" ? "ok" : healthTone === "rose" ? "err" : "mute"}`}>{healthText}</span>
        <span className="hidden text-[10.5px] text-[var(--text-faint)] lg:inline">{updatedAt ? `快照 ${updatedAt.toLocaleTimeString("zh-CN", { hour12: false })}` : "尚无快照"}</span>
        <select aria-label="图表时间范围" className="field !h-[32px] !w-auto" value={hours} onChange={(event) => setHours(Number(event.target.value))}>
          <option value={0.25}>15 分钟</option><option value={1}>1 小时</option><option value={6}>6 小时</option><option value={24}>24 小时</option>
        </select>
        <select aria-label="自动刷新间隔" className="field !h-[32px] !w-auto" value={refresh} onChange={(event) => setRefresh(Number(event.target.value))}>
          <option value={0}>手动刷新</option><option value={30}>30 秒</option><option value={60}>1 分钟</option><option value={120}>2 分钟</option>
        </select>
        <button onClick={() => void load()} disabled={busy} className="btn btn-ghost btn-sm">{busy ? "更新中…" : "刷新"}</button>
      </div>
    } />

    {err && <div className="alert err">概览加载失败：{err}<button onClick={() => void load()} className="ml-2 underline">重试</button></div>}
    {(ov?.errors ?? []).map((message, index) => <div key={`${index}-${message}`} className="alert warn">部分数据未获取：{message}</div>)}

    <section className="grid grid-cols-2 gap-2.5 md:grid-cols-3 2xl:grid-cols-6">
      <SummaryCard title="Broker" value={summary?.brokerCount ?? "…"} detail="元数据中发现的节点" tone="emerald" />
      <SummaryCard title="Topic" value={summary?.topicCount ?? "…"} detail="当前集群主题数" tone="blue" />
      <SummaryCard title="分区" value={summary?.partitionCount ?? "…"} detail="当前元数据分区总数" tone="blue" />
      <SummaryCard title="消费组" value={summary?.groupCount ?? "…"} detail={summary
        ? `${summary.memberCount ?? 0} 个成员 · ${groupStates.slice(0, 2).map(([state, count]) => `${state} ${count}`)
          .join(" · ") || "暂无状态"}`
        : "等待集群快照"} tone="sky" />
      <SummaryCard title="总 Lag" value={lagText} detail={summary?.lagState === "partial"
        ? `${summary.lagUnknownGroups ?? 0} 组 / ${summary.lagUnknownPartitions ?? 0} 分区数据不完整`
        : summary?.lagState === "unavailable" ? "消费组数据不可用" : "近似值 · 已提交位点口径"}
        tone={summary?.lagState === "complete" && (summary.totalLag ?? 0) === 0 ? "emerald" : summary?.lagState === "unavailable" ? "mute" : "amber"} />
      <SummaryCard title="分区健康" value={healthText} detail={summary?.offlinePartitionCount != null || summary?.underReplicatedPartitionCount != null
        ? `${summary.offlinePartitionCount ?? 0} 离线分区 · ${summary.underReplicatedPartitionCount ?? 0} 欠复制分区`
        : `${offlineTopicCount} 个离线主题 · ${underReplicatedTopicCount} 个欠复制主题（旧接口口径）`} tone={healthTone} />
    </section>

    <section className="grid min-w-0 grid-cols-1 gap-3 2xl:grid-cols-[minmax(0,1.55fr)_minmax(300px,0.85fr)]">
      <div className="min-w-0">
        <div className="mb-2 flex flex-wrap items-center justify-between gap-2 px-1">
          <div><h2 className="text-[13px] font-bold text-slate-800">消费组 Lag 趋势</h2></div>
          <select aria-label="Lag 趋势序列" title={activeSeries} className="field !h-[32px] !w-auto min-w-[160px] max-w-full" value={activeSeries}
            onChange={(event) => setSelectedLagSeries(event.target.value)} disabled={!lagOptions.length}>
            {lagOptions.length ? lagOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)
              : <option value="__total__">等待采样</option>}
          </select>
        </div>
        <TrendChart title={activeSeries === "__total__" ? "集群已知总 Lag" : "消费组 Lag"}
          unit="条" pts={chartPoints} hours={hours}
          seriesName={activeSeries === "__total__" ? "集群总 Lag" : activeSeries}
          description="每次概览刷新都会记录当前值；页面打开期间持续补充实时采样。"
          emptyText="暂无完整采样点。自动刷新会持续记录；消费组无提交位点时不绘制合计。" />
      </div>

      <section className="card flex min-w-0 flex-col p-4">
        <div className="flex items-start justify-between gap-3">
          <div><h2 className="text-[14px] font-bold text-slate-800">Broker 分区分布</h2></div>
          <span className="badge mute">{brokerStats.length} 节点</span>
        </div>
        {!ov ? <div className="py-12 text-center text-[12px] text-slate-500">等待集群快照…</div>
          : brokerStats.length === 0 ? <div className="py-12 text-center text-[12px] text-slate-500">Broker 元数据不可用。</div>
          : !hasBrokerDistribution ? <div className="mt-4 space-y-2">
            <div className="rounded-lg border border-dashed border-[var(--border-soft)] px-3 py-3 text-[11px] text-[var(--text-faint)]">
              暂无分区分布统计，仅列出已发现的 Broker。
            </div>
            {brokerStats.map((broker) => <div key={broker.id ?? `${broker.host}:${broker.port}`} className="flex items-center gap-2 rounded-lg bg-[var(--panel-3)] px-3 py-2 text-[11px]">
              <span className="mono text-slate-600">#{broker.id ?? "?"}</span><span className="mono min-w-0 flex-1 truncate text-[var(--text-faint)]">{broker.host}:{broker.port}</span>
              <span className="badge mute">已发现</span>
            </div>)}
          </div>
          : <div className="scroll-box mt-5 max-h-[var(--ov-box)] min-h-0 flex-1 space-y-4 overflow-y-auto overscroll-contain pr-1">
            {brokerStats.map((broker, index) => <div key={broker.id ?? `${broker.host}:${broker.port}`}>
              <div className="flex items-center gap-2 text-[11px]">
                <span className="mono w-12 shrink-0 text-slate-600">#{broker.id ?? "?"}</span>
                <span className="mono min-w-0 flex-1 truncate text-[var(--text-faint)]" title={`${broker.host}:${broker.port}`}>{broker.host}:{broker.port}</span>
                <b className="mono text-slate-800">{broker.leaders ?? 0} Leader</b>
              </div>
              <div className="mt-1.5 h-2 overflow-hidden rounded-full bg-slate-900/[0.08]">
                <div className="h-full rounded-full" style={{ width: `${Math.max(1, ((broker.leaders ?? 0) / maxLeaders) * 100)}%`, background: COLORS[index % COLORS.length] }} />
              </div>
              <div className="mt-1 flex gap-3 text-[10px] text-[var(--text-faint)]">
                <span>副本 {broker.replicas ?? "—"}</span><span>ISR {broker.isr ?? "—"}</span>
                <span className="ml-auto">元数据可见</span>
              </div>
            </div>)}
          </div>}
      </section>
    </section>

    <section className="grid min-w-0 grid-cols-1 gap-3 xl:grid-cols-2">
      <section className="card flex min-w-0 flex-col p-4">
        <div className="flex items-center justify-between gap-3">
          <div><h2 className="text-[14px] font-bold text-slate-800">Consumer Group Lag Top</h2></div>
          <span className="badge mute">{allGroups.length}</span>
        </div>
        {!ov ? <div className="py-10 text-center text-[12px] text-slate-500">等待集群快照…</div>
          : allGroups.length === 0 ? <div className="py-10 text-center text-[12px] text-slate-500">当前集群还没有消费组。</div>
          : <div className="scroll-box mt-3 max-h-[var(--ov-box)] min-h-0 flex-1 overflow-auto overscroll-contain">
            <table className="w-full min-w-[520px] text-left text-[11px]">
              <thead className="sticky top-0 z-10 bg-[var(--panel-2)] text-[var(--text-faint)]"><tr className="border-b border-[var(--border-soft)]">
                <th className="px-2 py-2 font-medium">消费组</th><th className="px-2 py-2 font-medium">状态</th>
                <th className="px-2 py-2 text-right font-medium">成员</th><th className="px-2 py-2 text-right font-medium">Lag</th></tr></thead>
              <tbody>{allGroups.map((group) => <tr key={group.groupId} className="border-b border-[var(--border-soft)]/60 last:border-0">
                <td className="max-w-[220px] truncate px-2 py-2.5 font-medium text-slate-700" title={group.groupId}>{group.groupId}</td>
                <td className="px-2 py-2.5"><StateLabel state={group.state} /></td>
                <td className="mono px-2 py-2.5 text-right text-slate-600">{group.memberCount ?? group.members ?? 0}</td>
                <td className="mono px-2 py-2.5 text-right font-semibold text-slate-800">{group.totalLag?.toLocaleString("zh-CN") ?? "未知"}</td>
              </tr>)}</tbody>
            </table>
          </div>}
      </section>

      <section className="card flex min-w-0 flex-col p-4">
        <div className="flex items-center justify-between gap-3">
          <div><h2 className="text-[14px] font-bold text-slate-800">分区异常</h2></div>
          <div className="flex items-center gap-2">
            <HealthRing pct={healthyPct} />
            <span className={`badge ${issues.length ? "err" : "ok"}`}>{issues.length ? `${issues.length} 个主题` : "无异常"}</span>
          </div>
        </div>
        {!ov ? <div className="py-10 text-center text-[12px] text-slate-500">等待集群快照…</div>
          : issues.length === 0 ? <div className="mt-3 flex min-h-0 flex-1 flex-col items-center justify-center rounded-xl border border-dashed border-[var(--border-soft)] bg-[var(--panel-3)] py-10 text-center">
            <div className="text-[13px] font-semibold text-emerald-700">当前未发现分区异常</div>
            <div className="mt-1 text-[11px] text-[var(--text-faint)]">基于 Leader、Replica 和 ISR 信息。</div>
          </div>
          : <div className="scroll-box mt-3 max-h-[var(--ov-box)] min-h-0 flex-1 space-y-1 overflow-y-auto overscroll-contain pr-1">
            {issues.slice(0, 30).map((topic) => <div key={topic.topic} className="flex flex-wrap items-center gap-2 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-1.5">
              <span className="mono min-w-0 flex-1 truncate text-[12px] text-slate-700" title={topic.topic}>{topic.topic}</span>
              <span className="text-[10.5px] text-[var(--text-faint)]">{topic.partitions} 分区</span>
              {(topic.offlinePartitions ?? (topic.hasOffline ? 1 : 0)) > 0 && <span className="badge err">离线 {(topic.offlinePartitions ?? 1)}</span>}
              {(topic.underReplicatedPartitions ?? (topic.hasUnderReplicated ? 1 : 0)) > 0 && <span className="badge warn">欠复制 {(topic.underReplicatedPartitions ?? 1)}</span>}
            </div>)}
            {issues.length > 30 && <div className="px-3 py-2 text-[10.5px] text-[var(--text-faint)]">另有 {issues.length - 30} 个异常主题未展开。</div>}
          </div>}
      </section>
    </section>
    <section className="card min-w-0 p-4">
      <div className="flex items-center justify-between gap-3">
        <div><h2 className="text-[14px] font-bold text-slate-800">Topic 分区规模</h2></div>
        <div className="flex items-center gap-2">
          {internalTopicCount > 0 && <button className="btn btn-ghost btn-sm" onClick={() => setShowInternalTopics((value) => !value)}>
            {showInternalTopics ? "隐藏系统 Topic" : `显示系统 Topic (${internalTopicCount})`}
          </button>}
          <span className="badge mute">{ov?.summary.topicCount ?? 0} Topics</span>
        </div>
      </div>
      {!ov ? <div className="py-8 text-center text-[12px] text-slate-500">等待集群快照…</div>
        : topicTop.length === 0 ? <div className="py-8 text-center text-[12px] text-slate-500">暂无 Topic 元数据。</div>
        : <div className="scroll-box mt-3 grid max-h-[var(--ov-box)] grid-cols-1 gap-x-6 gap-y-3 overflow-y-auto overscroll-contain pr-1 md:grid-cols-2">
          {topicTop.map((topic) => <div key={topic.topic} className="min-w-0">
            <div className="flex items-center gap-2 text-[11px]">
              <span className="mono min-w-0 flex-1 truncate text-slate-700" title={topic.topic}>{topic.topic}</span>
              <span className="mono shrink-0 font-semibold text-slate-800">{topic.partitions} 分区</span>
            </div>
            <div className="mt-1.5 h-1.5 overflow-hidden rounded-full bg-slate-900/[0.08]">
              <div className="h-full rounded-full bg-[#4c8fe8]" style={{ width: `${Math.max(2, topic.partitions / Math.max(1, topicTop[0]?.partitions ?? 1) * 100)}%` }} />
            </div>
            <div className="mt-1 flex flex-wrap gap-x-3 text-[10px] text-[var(--text-faint)]">
              <span>RF {topic.replication || "未知"}</span><span>{topic.leaders.length} 个 Leader Broker</span>
              {(topic.offlinePartitions ?? 0) > 0 && <span className="text-rose-600">离线 {topic.offlinePartitions}</span>}
              {(topic.underReplicatedPartitions ?? 0) > 0 && <span className="text-amber-700">欠复制 {topic.underReplicatedPartitions}</span>}
            </div>
          </div>)}
        </div>}
    </section>
    <div className="px-1 text-[10.5px] text-[var(--text-faint)]">
      {busy ? "正在更新" : updatedAt ? `最近更新 ${updatedAt.toLocaleTimeString("zh-CN", { hour12: false })}` : ""}
      {selectedGroup?.uncommitted ? ` · 当前组 ${selectedGroup.uncommitted} 个分区位点未知` : ""}
    </div>
  </div>;
}
