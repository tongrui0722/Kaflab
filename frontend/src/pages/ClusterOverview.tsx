import { apiFetch, apiWebSocket } from "../api/http";
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useConn } from "../conn";
import { authHeaders } from "../auth";
import { GroupStateBadge } from "../components/bits";
import { BrokerDistribution, type Broker } from "../components/BrokerDistribution";
import { normalizeLagPoints } from "../lagTrend";
import { Link } from "react-router-dom";

const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;

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
type BrokerNode = { id: number; status: "online" | "offline"; host?: string | null; port?: number | null };
type Overview = { summary: Summary; brokers: Broker[]; brokerStats?: Broker[]; brokerNodes?: BrokerNode[]; controllerId?: number | null; lagTop: Group[]; groups: Group[];
  topics: Topic[]; collector: { running: boolean; last_ok: number | null; last_error: string | null };
  collectedAt: number; errors: string[]; lagNote: string };
type Pt = { series: string; value: number; ts: number };

async function getJson<T>(url: string): Promise<T> {
  const response = await apiFetch(url, { headers: authHeaders() });
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
  const step = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 2.5 ? 2.5
    : fraction <= 5 ? 5 : fraction <= 7.5 ? 7.5 : 10;
  return step * power;
}

function smoothLine(points: Pt[], x: (ts: number) => number, y: (value: number) => number) {
  if (!points.length) return "";
  if (points.length === 1) return `M${(x(points[0].ts) - 5).toFixed(1)},${y(points[0].value).toFixed(1)} L${(x(points[0].ts) + 5).toFixed(1)},${y(points[0].value).toFixed(1)}`;
  const xs = points.map((point) => x(point.ts));
  const ys = points.map((point) => y(point.value));
  const slopes = xs.slice(1).map((end, index) => (ys[index + 1] - ys[index]) / Math.max(1, end - xs[index]));
  const tangents = points.map((_, index) => {
    if (index === 0) return slopes[0];
    if (index === points.length - 1) return slopes[slopes.length - 1];
    const before = slopes[index - 1], after = slopes[index];
    if (before * after <= 0) return 0;
    return Math.sign(before) * Math.min(Math.abs((before + after) / 2), 3 * Math.abs(before), 3 * Math.abs(after));
  });
  return points.slice(1).reduce((path, _, index) => {
    const dx = (xs[index + 1] - xs[index]) / 3;
    return `${path} C${(xs[index] + dx).toFixed(1)},${(ys[index] + tangents[index] * dx).toFixed(1)} ${(xs[index + 1] - dx).toFixed(1)},${(ys[index + 1] - tangents[index + 1] * dx).toFixed(1)} ${xs[index + 1].toFixed(1)},${ys[index + 1].toFixed(1)}`;
  }, `M${xs[0].toFixed(1)},${ys[0].toFixed(1)}`);
}

function LagSeriesPicker({ options, value, onChange }: {
  options: { value: string; label: string }[]; value: string; onChange: (value: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const list = useRef<HTMLDivElement>(null);
  const selected = options.find((option) => option.value === value);

  useEffect(() => {
    if (!open) return;
    const dismiss = (event: PointerEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
    const escape = (event: KeyboardEvent) => {
      if (event.key === "Escape") { setOpen(false); trigger.current?.focus(); }
    };
    document.addEventListener("pointerdown", dismiss);
    document.addEventListener("keydown", escape);
    return () => { document.removeEventListener("pointerdown", dismiss); document.removeEventListener("keydown", escape); };
  }, [open]);

  return <div className="lag-series-picker" ref={root}>
    <button ref={trigger} type="button" className="lag-series-trigger" disabled={!options.length}
      aria-label={`监控对象：${selected?.label ?? "等待采样"}`} aria-haspopup="menu" aria-expanded={open}
      onClick={() => setOpen((current) => !current)}
      onKeyDown={(event) => {
        if (event.key === "ArrowDown") {
          event.preventDefault(); setOpen(true);
          requestAnimationFrame(() => list.current?.querySelector("button")?.focus());
        }
      }}>
      <span className="lag-series-trigger-copy" title={selected?.label}>{selected?.label ?? "等待采样"}</span>
      <svg className={`lag-series-chevron${open ? " is-open" : ""}`} viewBox="0 0 16 16" fill="none" aria-hidden="true">
        <path d="m4 6 4 4 4-4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    </button>
    {open && <div ref={list} className="lag-series-menu" role="menu" aria-label="选择 Lag 监控对象">
      {options.map((option) => <button key={option.value} type="button" role="menuitemradio" aria-checked={option.value === value}
        className={`lag-series-option${option.value === value ? " is-selected" : ""}`}
        onClick={() => { onChange(option.value); setOpen(false); trigger.current?.focus(); }}
        onKeyDown={(event) => {
          if (event.key !== "ArrowDown" && event.key !== "ArrowUp") return;
          event.preventDefault();
          const items = [...(list.current?.querySelectorAll("button") ?? [])];
          const index = items.indexOf(event.currentTarget);
          items[(index + (event.key === "ArrowDown" ? 1 : -1) + items.length) % items.length]?.focus();
        }}>
        <span className="lag-series-option-copy" title={option.label}>{option.label}</span>
        {option.value === value && <span className="lag-series-check" aria-hidden="true">✓</span>}
      </button>)}
    </div>}
  </div>;
}

function TrendChart({ pts, hours, seriesName, emptyText, action, warning }: {
  pts: Pt[]; hours: number; seriesName: string; emptyText: string;
  action: ReactNode; warning?: string;
}) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const points = useMemo(() => [...pts].sort((a, b) => a.ts - b.ts), [pts]);
  useEffect(() => { setHoveredIndex(null); }, [hours, seriesName]);
  const now = Math.floor(Date.now() / 1000);
  const first = points[0], last = points[points.length - 1];
  const observedSpan = first && last ? last.ts - first.ts : 0;
  const focusOnSamples = Boolean(points.length && observedSpan < hours * 3600 * .35 &&
    now - last.ts <= Math.max(120, hours * 3600 * .2));
  const domainPad = Math.max(60, observedSpan * .08);
  const x0 = focusOnSamples ? first.ts - domainPad : now - hours * 3600;
  const x1 = focusOnSamples ? last.ts + domainPad : now;
  const maxValue = Math.max(0, ...points.map((p) => p.value));
  const yMax = maxValue === 0 ? 4 : niceMax(maxValue * 1.08);
  const w = 900, h = 310, left = 68, right = 24, top = 28, bottom = 46;
  const plotW = w - left - right, plotH = h - top - bottom;
  const x = (ts: number) => left + ((ts - x0) / Math.max(1, x1 - x0)) * plotW;
  const y = (value: number) => top + plotH - (Math.max(0, value) / yMax) * plotH;
  const intervals = points.slice(1).map((point, index) => point.ts - points[index].ts).filter((gap) => gap > 0).sort((a, b) => a - b);
  const medianInterval = intervals[Math.floor(intervals.length / 2)] ?? 30;
  const gapLimit = Math.min(1800, Math.max(90, medianInterval * 3));
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
  const hovered = hoveredIndex == null ? null : points[hoveredIndex];
  const number = (value: number) => value.toLocaleString("zh-CN", { maximumFractionDigits: 1 });

  return (
    <section className="card lag-trend-card overview-panel">
      <div className="overview-panel-head">
        <h2 className="overview-panel-title">消费组 Lag 趋势</h2>
        {action}
      </div>
      {stats && <div className="lag-trend-stats">
          <span>最新 <b className="mono">{number(stats.latest)}</b></span>
          <span>峰值 <b className="mono">{number(stats.peak)}</b></span>
          <span>均值 <b className="mono">{number(stats.average)}</b></span>
          <span>变化 <b className="mono">{stats.change > 0 ? "+" : ""}{number(stats.change)}</b></span>
      </div>}
      {warning && <span className="lag-trend-warning">{warning}</span>}
      {points.length === 0 ? (
        <div className="lag-trend-empty">
          <div className="text-[13px] font-semibold text-slate-600">暂无历史采样</div>
          <div className="mt-1 max-w-lg text-[12px] leading-5 text-[var(--text-faint)]">{emptyText}</div>
        </div>
      ) : (
        <div className="lag-chart-surface">
          <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" className="lag-chart-svg" role="img" tabIndex={0}
            aria-label={`${seriesName}趋势图，鼠标悬停或使用左右方向键查看采样值`}
            onMouseMove={(event) => {
              const bounds = event.currentTarget.getBoundingClientRect();
              const px = (event.clientX - bounds.left) / bounds.width * w;
              let nearest = 0;
              for (let index = 1; index < points.length; index++) {
                if (Math.abs(x(points[index].ts) - px) < Math.abs(x(points[nearest].ts) - px)) nearest = index;
              }
              if (nearest !== hoveredIndex) setHoveredIndex(nearest);
            }} onMouseLeave={() => setHoveredIndex(null)}
            onFocus={() => setHoveredIndex((current) => current ?? points.length - 1)}
            onBlur={() => setHoveredIndex(null)}
            onKeyDown={(event) => {
              if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
              event.preventDefault();
              setHoveredIndex((current) => Math.max(0, Math.min(points.length - 1,
                (current ?? points.length - 1) + (event.key === "ArrowLeft" ? -1 : 1))));
            }}>
            {yTicks.map((tick, i) => <g key={`y-${i}`}>
              <line x1={left} x2={w - right} y1={y(tick)} y2={y(tick)} stroke="#d5e1ee" strokeDasharray={i === 0 ? "0" : "4 7"} />
              <text x={left - 12} y={y(tick) + 4} fill="#637b96" fontSize="11" textAnchor="end">{number(tick)}</text>
            </g>)}
            {xTicks.map((tick, i) => <g key={`x-${i}`}>
              <line x1={x(tick)} x2={x(tick)} y1={top} y2={top + plotH} stroke="#e5edf5" strokeDasharray="2 8" />
              <text x={x(tick)} y={h - 13} fill="#637b96" fontSize="11" textAnchor={i === 0 ? "start" : i === 4 ? "end" : "middle"}>{timeLabel(tick)}</text>
            </g>)}
            {segments.map((segment, index) => <path key={`seg-${index}`} d={smoothLine(segment, x, y)}
              fill="none" stroke="#4c8fe8" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />)}
            {hovered && <line x1={x(hovered.ts)} x2={x(hovered.ts)} y1={top} y2={top + plotH} stroke="#5e86c9" strokeWidth="1.2" strokeDasharray="4 5" />}
          </svg>
          {hovered && <div className="lag-chart-tooltip" style={{ left: `${Math.min(85, Math.max(15, x(hovered.ts) / w * 100))}%` }}>
            <span>{new Date(hovered.ts * 1000).toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false })}</span>
            <strong className="mono">{number(hovered.value)} <small>条</small></strong>
          </div>}
        </div>
      )}
    </section>
  );
}

function StateLabel({ state }: { state?: string }) {
  return <GroupStateBadge state={state} />;
}

function SummaryCard({ title, value, tone = "blue" }: { title: string; value: string | number; tone?: string }) {
  return <div className={`metric-card metric-${tone} overview-summary-card min-w-0`}>
    <div className="overview-summary-heading">
      <span className="neu-chip" aria-hidden="true" />
      <div className="metric-label">{title}</div>
    </div>
    <div className="overview-summary-value" title={String(value)}>{value}</div>
  </div>;
}

type HealthSlice = { label: string; value: number; tone: "good" | "warn" | "bad" };

function HealthDonut({ title, unit, centerLabel, slices, footer }: {
  title: string; unit: string; centerLabel: string; slices: HealthSlice[]; footer?: ReactNode;
}) {
  const total = slices.reduce((sum, slice) => sum + slice.value, 0);
  const healthy = slices[0]?.value ?? 0;
  const percentage = total > 0 ? Math.round(healthy / total * 100) : null;
  let offset = 0;

  return <div className={`overview-health-chart${slices.some((slice) => slice.tone !== "good" && slice.value > 0) ? " has-issue" : ""}`}>
    <div className="overview-health-chart-head">
      <h3>{title}</h3>
      <span>{total.toLocaleString("zh-CN")} {unit}</span>
    </div>
    <div className="overview-health-chart-body">
      <div className="overview-health-ring" role="img"
        aria-label={`${title}：${slices.map((slice) => `${slice.label} ${slice.value}`).join("，")}`}>
        <svg viewBox="0 0 104 104" aria-hidden="true">
          <circle className="overview-health-ring-track" cx="52" cy="52" r="39" fill="none" strokeWidth="11" />
          {total > 0 && slices.map((slice) => {
            const length = slice.value / total * 100;
            const start = offset;
            offset += length;
            return slice.value > 0 && <circle key={slice.label} className={`overview-health-ring-segment is-${slice.tone}`}
              cx="52" cy="52" r="39" fill="none" strokeWidth="11" pathLength="100"
              strokeDasharray={`${length} ${100 - length}`} strokeDashoffset={-start}
              transform="rotate(-90 52 52)" />;
          })}
        </svg>
        <div className="overview-health-ring-center">
          <strong>{percentage == null ? "—" : `${percentage}%`}</strong>
          <small>{centerLabel}</small>
        </div>
      </div>
      <div className="overview-health-legend">
        {slices.map((slice) => <div className="overview-health-legend-row" key={slice.label}>
          <span className={`overview-health-legend-dot is-${slice.tone}`} aria-hidden="true" />
          <span className="overview-health-legend-label">{slice.label}</span>
          <strong className="mono">{slice.value.toLocaleString("zh-CN")}</strong>
        </div>)}
      </div>
    </div>
    {footer && <div className="overview-health-chart-foot">{footer}</div>}
  </div>;
}

export function ClusterOverviewPage() {
  const { conn } = useConn();
  const [ov, setOv] = useState<Overview | null>(null);
  const [lag, setLag] = useState<Pt[]>([]);
  const [lagError, setLagError] = useState<string | null>(null);
  const [hours, setHours] = useState(1);
  const [refresh, setRefresh] = useState(30);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  const [selectedLagSeries, setSelectedLagSeries] = useState("__total__");
  const [showInternalTopics, setShowInternalTopics] = useState(false);
  const [showIssueDetails, setShowIssueDetails] = useState(false);
  const issueTrigger = useRef<HTMLButtonElement>(null);
  const issueClose = useRef<HTMLButtonElement>(null);
  const issueDrawer = useRef<HTMLElement>(null);

  useEffect(() => {
    setOv(null);
    setLag([]);
    setLagError(null);
    setUpdatedAt(null);
    setShowIssueDetails(false);
  }, [conn.id]);

  useEffect(() => {
    if (!showIssueDetails) return;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    issueClose.current?.focus();
    const onDrawerKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setShowIssueDetails(false);
        issueTrigger.current?.focus();
      } else if (event.key === "Tab") {
        const controls = issueDrawer.current?.querySelectorAll<HTMLElement>('button:not([disabled]), a[href], [tabindex="0"]');
        if (!controls?.length) return;
        const first = controls[0], last = controls[controls.length - 1];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
      }
    };
    document.addEventListener("keydown", onDrawerKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener("keydown", onDrawerKeyDown);
    };
  }, [showIssueDetails]);

  const load = useCallback(async () => {
    if (!conn.id) return;
    setBusy(true);
    setErr(null);
    try {
      // History is sampled by the backend even while nobody is logged in.
      // Fetch it independently so a slow overview snapshot cannot hide the chart.
      await Promise.allSettled([
        getJson<{ points?: Pt[] }>(`${API}/api/clusters/${conn.id}/trends?metric=group_lag&hours=${hours}`)
          .then((trend) => { setLag(trend.points ?? []); setLagError(null); })
          .catch((reason) => setLagError(reason instanceof Error ? reason.message : String(reason))),
        getJson<Overview>(`${API}/api/clusters/${conn.id}/overview`)
          .then((snapshot) => {
            setOv(snapshot);
            setUpdatedAt(snapshot.collectedAt ? new Date(snapshot.collectedAt) : new Date());
          })
          .catch((reason) => setErr(reason instanceof Error ? reason.message : String(reason))),
      ]);
    } finally {
      setBusy(false);
    }
  }, [conn.id, hours]);

  const chartLag = useMemo(() => {
    const cutoff = Math.floor(Date.now() / 1000) - hours * 3600;
    return normalizeLagPoints(lag, [], cutoff);
  }, [lag, hours]);

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
  const brokerHealth = useMemo(() => {
    const nodes = ov?.brokerNodes ?? [];
    const online = nodes.filter((node) => node.status === "online").length;
    return [
      { label: "在线", value: online, tone: "good" as const },
      { label: "离线", value: nodes.length - online, tone: "bad" as const },
    ];
  }, [ov?.brokerNodes]);
  const offlineNodes = ov?.brokerNodes?.filter((node) => node.status === "offline") ?? [];
  const topicHealth = useMemo(() => {
    const topics = ov?.topics ?? [];
    const offline = topics.filter((topic) => (topic.offlinePartitions ?? (topic.hasOffline ? 1 : 0)) > 0).length;
    const underReplicated = topics.filter((topic) =>
      (topic.offlinePartitions ?? (topic.hasOffline ? 1 : 0)) === 0 &&
      (topic.underReplicatedPartitions ?? (topic.hasUnderReplicated ? 1 : 0)) > 0).length;
    return [
      { label: "正常", value: topics.length - offline - underReplicated, tone: "good" as const },
      { label: "仅欠复制", value: underReplicated, tone: "warn" as const },
      { label: "离线", value: offline, tone: "bad" as const },
    ];
  }, [ov?.topics]);
  const issues = ov?.topics.filter((topic) => (topic.offlinePartitions ?? (topic.hasOffline ? 1 : 0)) > 0 ||
    (topic.underReplicatedPartitions ?? (topic.hasUnderReplicated ? 1 : 0)) > 0) ?? [];
  const issueCount = issues.length + offlineNodes.length + (ov?.errors?.length ?? 0);
  const hasHealthIssues = issueCount > 0;
  const lagText = summary?.totalLag == null ? "未知" : summary.lagState === "partial"
    ? `~${summary.totalLag.toLocaleString("zh-CN")}` : summary.totalLag.toLocaleString("zh-CN");
  const offlineTopicCount = summary?.offlineTopics ?? ov?.topics.filter((topic) => topic.hasOffline).length ?? 0;
  const underReplicatedTopicCount = summary?.underReplicatedTopics ?? ov?.topics.filter((topic) => topic.hasUnderReplicated).length ?? 0;
  const offlineCountForHealth = summary?.offlinePartitionCount ?? offlineTopicCount;
  const underReplicatedCountForHealth = summary?.underReplicatedPartitionCount ?? underReplicatedTopicCount;
  const healthText = !ov ? "加载中" : offlineNodes.length + offlineCountForHealth + underReplicatedCountForHealth > 0
    ? "需要关注" : ov.errors?.length ? "数据不完整" : "正常";
  const healthTone = healthText === "正常" ? "emerald" : healthText === "加载中" ? "blue" : "rose";
  const topicTop = useMemo(() => [...(ov?.topics ?? [])]
    .filter((topic) => showInternalTopics || !topic.topic.startsWith("_"))
    .sort((a, b) => b.partitions - a.partitions || a.topic.localeCompare(b.topic)),
    [ov?.topics, showInternalTopics]);
  const internalTopicCount = ov?.topics.filter((topic) => topic.topic.startsWith("_")).length ?? 0;

  return <div className="overview-page">
    <header className="overview-hero">
      <div className="overview-hero-copy">
        <h1><span>集群概览</span><span className="overview-hero-name" title={conn.name}>{conn.name}</span></h1>
        <p className="overview-hero-address mono" title={conn.bootstrap}>{conn.bootstrap}</p>
      </div>
      <div className="overview-header-controls">
        <span className={`badge ${healthTone === "emerald" ? "ok" : healthTone === "rose" ? "err" : "mute"}`}>{healthText}</span>
        <span className="overview-snapshot-time">{updatedAt ? `更新于 ${updatedAt.toLocaleTimeString("zh-CN", { hour12: false })}` : "尚无快照"}</span>
        <select aria-label="图表时间范围" className="field !h-[34px] !w-auto" value={hours} onChange={(event) => setHours(Number(event.target.value))}>
          <option value={0.25}>15 分钟</option><option value={1}>1 小时</option><option value={6}>6 小时</option><option value={24}>24 小时</option>
        </select>
        <select aria-label="自动刷新间隔" className="field !h-[34px] !w-auto" value={refresh} onChange={(event) => setRefresh(Number(event.target.value))}>
          <option value={0}>手动刷新</option><option value={30}>30 秒</option><option value={60}>1 分钟</option><option value={120}>2 分钟</option>
        </select>
        <button onClick={() => void load()} disabled={busy} className="btn btn-ghost btn-sm">{busy ? "更新中…" : "刷新"}</button>
      </div>
    </header>

    {err && <div className="alert err">概览加载失败：{err}<button onClick={() => void load()} className="ml-2 underline">重试</button></div>}
    <section className="overview-metrics" aria-label="集群关键指标">
      <SummaryCard title="集群健康" value={healthText} tone={healthTone} />
      <SummaryCard title="总 Lag" value={lagText}
        tone={summary?.lagState === "complete" && (summary.totalLag ?? 0) === 0 ? "emerald" : summary?.lagState === "unavailable" ? "mute" : "amber"} />
      <SummaryCard title="在线 Broker" value={summary?.brokerCount ?? "…"} tone="emerald" />
      <SummaryCard title="Topic" value={summary?.topicCount ?? "…"} tone="blue" />
      <SummaryCard title="分区" value={summary?.partitionCount ?? "…"} tone="blue" />
      <SummaryCard title="消费组" value={summary?.groupCount ?? "…"} tone="sky" />
    </section>

    <section className="overview-primary-grid">
      <TrendChart pts={chartPoints} hours={hours}
        seriesName={activeSeries === "__total__" ? "集群总 Lag" : activeSeries}
        action={<LagSeriesPicker options={lagOptions} value={activeSeries} onChange={setSelectedLagSeries} />}
        warning={lagError ? `历史 Lag 读取失败：${lagError}` : selectedGroup?.uncommitted
          ? `${selectedGroup.uncommitted} 个分区位点未知` : ov?.collector.last_error ?? undefined}
        emptyText="暂无可用采样" />

      <BrokerDistribution brokers={brokerStats} controllerId={ov?.controllerId} hasDistribution={hasBrokerDistribution} loading={!ov} />
    </section>

    <section className="overview-secondary-grid">
      <section className="card overview-panel overview-secondary-panel">
        <div className="overview-panel-head">
          <h2 className="overview-panel-title">消费组 Lag</h2>
          <span className="badge mute">{allGroups.length} 组</span>
        </div>
        {!ov ? <div className="overview-empty">等待集群快照…</div>
          : allGroups.length === 0 ? <div className="overview-empty">暂无消费组</div>
          : <div className="overview-table-wrap scroll-box">
            <table className="overview-table">
              <thead><tr>
                <th>消费组</th><th>状态</th><th className="is-number">成员</th><th className="is-number">Lag</th></tr></thead>
              <tbody>{allGroups.map((group) => <tr key={group.groupId}>
                <td className="overview-group-name" title={group.groupId}>{group.groupId}</td>
                <td><StateLabel state={group.state} /></td>
                <td className="is-number mono">{group.memberCount ?? group.members ?? 0}</td>
                <td className="is-number mono overview-lag-value">{group.totalLag?.toLocaleString("zh-CN") ?? "未知"}</td>
              </tr>)}</tbody>
            </table>
          </div>}
      </section>

      <section className={`card overview-panel overview-secondary-panel overview-health-panel${hasHealthIssues ? " has-issue" : ""}`}>
        <div className="overview-panel-head">
          <h2 className="overview-panel-title">节点与 Topic 健康</h2>
          <div className="overview-health-actions">
            <span className={`badge ${hasHealthIssues ? "err" : !ov?.brokerNodes?.length ? "mute" : "ok"}`}>
              {hasHealthIssues ? "需要关注" : !ov?.brokerNodes?.length ? "数据待获取" : "正常"}
            </span>
            <button ref={issueTrigger} type="button" className={`btn btn-sm overview-issues-button ${hasHealthIssues ? "btn-warn" : "btn-ghost"}`}
              disabled={!hasHealthIssues} aria-haspopup="dialog" aria-label={`查看异常明细，${issueCount} 项`}
              onClick={() => setShowIssueDetails(true)}>异常 <span className="mono">{issueCount}</span></button>
          </div>
        </div>
        {!ov ? <div className="overview-empty">等待集群快照…</div>
          : <>
            <div className="overview-health-grid">
              <HealthDonut title="Broker 节点" unit="节点" centerLabel="在线节点" slices={brokerHealth}
                footer={<div className="overview-health-nodes scroll-box" aria-label="Broker 节点状态">
                  {!ov.brokerNodes?.length && <span className="ui-note">节点数据待获取</span>}
                  {(ov.brokerNodes ?? []).map((node) => <span key={node.id}
                    className={`overview-health-node is-${node.status}`}
                    title={node.host ? `${node.host}${node.port ? `:${node.port}` : ""}` : `Broker #${node.id}`}>
                    <i aria-hidden="true" /><b className="mono">#{node.id}</b> {node.status === "online" ? "在线" : "离线"}
                  </span>)}
                </div>} />
              <HealthDonut title="Topic 状态" unit="Topic" centerLabel="正常 Topic" slices={topicHealth}
                footer={<div className="overview-health-partitions" aria-label="分区健康统计">
                  <div><span>总分区</span><strong className="mono">{summary?.partitionCount ?? 0}</strong></div>
                  <div><span>离线</span><strong className="mono">{summary?.offlinePartitionCount ?? ov.topics.reduce((sum, topic) => sum + (topic.offlinePartitions ?? 0), 0)}</strong></div>
                  <div><span>欠复制</span><strong className="mono">{summary?.underReplicatedPartitionCount ?? ov.topics.reduce((sum, topic) => sum + (topic.underReplicatedPartitions ?? 0), 0)}</strong></div>
                </div>} />
            </div>
          </>}
      </section>
    </section>
    {showIssueDetails && <div className="overview-issue-overlay">
      <button type="button" className="overview-issue-scrim" aria-label="关闭异常明细" onClick={() => { setShowIssueDetails(false); issueTrigger.current?.focus(); }} />
      <aside ref={issueDrawer} className="overview-issue-drawer" role="dialog" aria-modal="true" aria-labelledby="overview-issue-title">
        <header className="overview-issue-drawer-head">
          <div><p>HEALTH / DETAILS</p><h2 id="overview-issue-title">异常明细 <span className="mono">{issueCount}</span></h2></div>
          <button ref={issueClose} type="button" className="overview-issue-close" aria-label="关闭异常明细"
            onClick={() => { setShowIssueDetails(false); issueTrigger.current?.focus(); }}>×</button>
        </header>
        <div className="overview-issue-drawer-body scroll-box">
          {offlineNodes.length > 0 && <section className="overview-issue-section">
            <h3>离线 Broker <span className="mono">{offlineNodes.length}</span></h3>
            <div className="overview-issue-list">{offlineNodes.map((node) => <div key={`broker-${node.id}`} className="overview-issue-item">
              <div className="overview-issue-copy"><strong className="mono">Broker #{node.id}</strong>
                {node.host && <small className="mono">{node.host}{node.port ? `:${node.port}` : ""}</small>}</div>
              <span className="badge err">离线</span>
            </div>)}</div>
          </section>}
          {issues.length > 0 && <section className="overview-issue-section">
            <h3>异常 Topic <span className="mono">{issues.length}</span></h3>
            <div className="overview-issue-list">{issues.map((topic) => <div key={topic.topic} className="overview-issue-item">
              <div className="overview-issue-copy"><Link to={`/topics/${encodeURIComponent(topic.topic)}`} onClick={() => setShowIssueDetails(false)}
                className="overview-issue-name mono" title={topic.topic}>{topic.topic}</Link>
                <small>{topic.partitions} 个分区</small></div>
              <div className="overview-issue-tags">
                {(topic.offlinePartitions ?? (topic.hasOffline ? 1 : 0)) > 0 && <span className="badge err">离线分区 {topic.offlinePartitions ?? 1}</span>}
                {(topic.underReplicatedPartitions ?? (topic.hasUnderReplicated ? 1 : 0)) > 0 && <span className="badge warn">欠复制 {topic.underReplicatedPartitions ?? 1}</span>}
              </div>
            </div>)}</div>
          </section>}
          {(ov?.errors?.length ?? 0) > 0 && <section className="overview-issue-section">
            <h3>采集异常 <span className="mono">{ov?.errors.length}</span></h3>
            <div className="overview-issue-list">{ov?.errors.map((message, index) => <div key={`${index}-${message}`} className="overview-issue-item overview-issue-message">{message}</div>)}</div>
          </section>}
        </div>
        <footer className="overview-issue-drawer-foot">快照更新时间 {updatedAt?.toLocaleTimeString("zh-CN", { hour12: false }) ?? "—"}</footer>
      </aside>
    </div>}
    <section className="card overview-panel overview-topic-panel">
      <div className="overview-panel-head">
        <h2 className="overview-panel-title">Topic 分区规模</h2>
        <div className="overview-panel-actions">
          {internalTopicCount > 0 && <button className="btn btn-ghost btn-sm" onClick={() => setShowInternalTopics((value) => !value)}>
            {showInternalTopics ? "隐藏系统 Topic" : `显示系统 Topic (${internalTopicCount})`}
          </button>}
          <span className="badge mute">{topicTop.length} 个</span>
          <Link to="/topics" className="btn btn-ghost btn-sm">查看全部</Link>
        </div>
      </div>
      {!ov ? <div className="overview-empty">等待集群快照…</div>
        : topicTop.length === 0 ? <div className="overview-empty">暂无 Topic 元数据</div>
        : <div className="overview-topic-grid scroll-box">
          {topicTop.slice(0, 8).map((topic) => <div key={topic.topic} className="overview-topic-item">
            <div className="overview-topic-row">
              <span className="overview-topic-name mono" title={topic.topic}>{topic.topic}</span>
              <span className="overview-topic-count"><b className="mono">{topic.partitions}</b><small>分区</small></span>
            </div>
            <div className="overview-bar-track overview-topic-bar" aria-hidden="true">
              <div className="overview-bar-fill" style={{ width: `${topic.partitions > 0 ? Math.max(2, topic.partitions / Math.max(1, topicTop[0]?.partitions ?? 1) * 100) : 0}%` }} />
            </div>
            <div className="overview-topic-meta">
              <span>RF {topic.replication || "未知"}</span><span>{topic.leaders.length} 个 Leader Broker</span>
              {(topic.offlinePartitions ?? 0) > 0 && <span className="text-rose-600">离线 {topic.offlinePartitions}</span>}
              {(topic.underReplicatedPartitions ?? 0) > 0 && <span className="text-amber-700">欠复制 {topic.underReplicatedPartitions}</span>}
            </div>
          </div>)}
        </div>}
    </section>
  </div>;
}
