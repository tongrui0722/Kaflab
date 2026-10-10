import { useEffect, useMemo, useState } from "react";
import { useConn } from "../conn";
import { useAuth, authHeaders } from "../auth";
import { PageHeader } from "../components/bits";

const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";
const PAGE_SIZES = [10, 20, 50];

interface TopicRow {
  topic: string;
  partitions: number;
  replication: number;
  leaders: number[];
  hasOffline: boolean;
  hasUnderReplicated?: boolean;
}
type HealthFilter = "all" | "healthy" | "offline" | "under-replicated";
type SortKey = "topic" | "partitions" | "replication" | "health";

export function TopicsPage() {
  const { conn } = useConn();
  const { mode } = useAuth();
  const readonly = mode === "readonly";
  const [rows, setRows] = useState<TopicRow[]>([]);
  const [brokerCount, setBrokerCount] = useState(3);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [healthFilter, setHealthFilter] = useState<HealthFilter>("all");
  const [sortKey, setSortKey] = useState<SortKey>("topic");
  const [sortAsc, setSortAsc] = useState(true);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [showCreate, setShowCreate] = useState(false);
  const [cName, setCName] = useState("");
  const [cParts, setCParts] = useState(3);
  const [cRf, setCRf] = useState(1);
  const [cRetentionMs, setCRetentionMs] = useState("");
  const [cCleanupPolicy, setCCleanupPolicy] = useState("");
  const [cRetentionMb, setCRetentionMb] = useState("");
  const [cConfigs, setCConfigs] = useState("");
  const [cBusy, setCBusy] = useState(false);
  const [cErr, setCErr] = useState<string | null>(null);
  const [cSuccess, setCSuccess] = useState<string | null>(null);

  const load = async (cid: string): Promise<TopicRow[] | null> => {
    setLoading(true);
    setErr(null);
    try {
      const r = await fetch(`${API}/api/clusters/${cid}/topics`, { headers: authHeaders() });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const d = await r.json() as { topics: TopicRow[]; brokers: { id?: number }[] };
      setRows(d.topics);
      if (d.brokers) {
        setBrokerCount(d.brokers.length);
        setCRf((v) => Math.min(v, Math.max(d.brokers.length, 1)));
      }
      return d.topics;
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      return null;
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { void load(conn.id); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [conn.id]);
  useEffect(() => { setPage(1); }, [q, healthFilter, pageSize, conn.id]);

  const createTopic = async () => {
    if (cBusy) return;
    const name = cName.trim();
    if (!name) { setCErr("请输入 Topic 名称"); return; }
    if (!Number.isInteger(cParts) || cParts < 1) { setCErr("分区数必须为 ≥1 的整数"); return; }
    if (!Number.isInteger(cRf) || cRf < 1 || cRf > brokerCount) {
      setCErr(`副本数须为 1 到 ${brokerCount} 之间的整数`);
      return;
    }
    const retentionMb = cRetentionMb.trim() ? Number(cRetentionMb) : null;
    if (retentionMb !== null && (!Number.isSafeInteger(retentionMb) || retentionMb < 1)) {
      setCErr("保留容量必须是大于 0 的整数 MB");
      return;
    }
    let configs: Record<string, string> = {};
    if (cRetentionMs) configs["retention.ms"] = cRetentionMs;
    if (cCleanupPolicy) configs["cleanup.policy"] = cCleanupPolicy;
    if (retentionMb !== null) configs["retention.bytes"] = String(retentionMb * 1024 * 1024);
    if (cConfigs.trim()) {
      try {
        const parsed = JSON.parse(cConfigs) as unknown;
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("bad");
        if (Object.entries(parsed as Record<string, unknown>).some(([k, v]) => !k.trim() || typeof v !== "string")) {
          throw new Error("bad-config-value");
        }
        const advanced = parsed as Record<string, string>;
        const duplicates = Object.keys(advanced).filter((key) => key in configs);
        if (duplicates.length) {
          setCErr(`配置项重复：${duplicates.join(", ")}。请在常用配置或高级 JSON 中保留一处。`);
          return;
        }
        configs = { ...configs, ...advanced };
      } catch {
        setCErr("Topic 配置必须为 JSON 对象，如 {\"retention.ms\": \"604800000\"}");
        return;
      }
    }
    setCBusy(true);
    setCErr(null);
    setCSuccess(null);
    let responseStatus: number | null = null;
    try {
      const r = await fetch(`${API}/api/clusters/${conn.id}/topics`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ name, partitions: cParts, replicationFactor: cRf, configs }),
      });
      if (!r.ok) { responseStatus = r.status; throw new Error(await r.text()); }
      setShowCreate(false);
      setCName("");
      setCRetentionMs("");
      setCCleanupPolicy("");
      setCRetentionMb("");
      setCConfigs("");
      setCSuccess(`Topic「${name}」创建成功`);
      await load(conn.id);
    } catch (e) {
      const raw = e instanceof Error ? e.message : String(e);
      if (responseStatus !== null && responseStatus < 500) {
        try {
          const j = JSON.parse(raw) as { detail?: string | { msg?: string }[] };
          if (typeof j.detail === "string") setCErr(j.detail);
          else if (Array.isArray(j.detail)) setCErr(j.detail[0]?.msg ?? "请求参数无效");
          else setCErr(raw);
        } catch { setCErr(raw); }
        return;
      }
      const refreshed = await load(conn.id);
      if (refreshed?.some((row) => row.topic === name)) {
        setShowCreate(false);
        setCName("");
        setCRetentionMs("");
        setCCleanupPolicy("");
        setCRetentionMb("");
        setCConfigs("");
        setCSuccess(`Topic「${name}」已在 Kafka 集群中确认存在`);
        setCErr(null);
        return;
      }
      try {
        const j = JSON.parse(raw) as { detail?: string | { msg?: string }[] };
        if (typeof j.detail === "string") setCErr(j.detail);
        else if (Array.isArray(j.detail)) {
          const msg = j.detail[0]?.msg ?? "请求参数无效";
          setCErr(msg.includes("greater than or equal to 1") ? "分区数和副本数必须至少为 1" : msg);
        } else setCErr(raw);
      } catch {
        setCErr(raw.replace(/^\{"detail":"|"}$/g, "").replace(/\\"/g, '"'));
      }
    } finally {
      setCBusy(false);
    }
  };

  const filtered = useMemo(() => {
    const search = q.trim().toLocaleLowerCase();
    const matches = rows.filter((row) => {
      if (search && !row.topic.toLocaleLowerCase().includes(search)) return false;
      if (healthFilter === "healthy" && (row.hasOffline || row.hasUnderReplicated)) return false;
      if (healthFilter === "offline" && !row.hasOffline) return false;
      if (healthFilter === "under-replicated" && !row.hasUnderReplicated) return false;
      return true;
    });
    const healthRank = (row: TopicRow) => row.hasOffline ? 2 : row.hasUnderReplicated ? 1 : 0;
    matches.sort((a, b) => {
      const value = sortKey === "topic" ? a.topic.localeCompare(b.topic)
        : sortKey === "partitions" ? a.partitions - b.partitions
          : sortKey === "replication" ? a.replication - b.replication
            : healthRank(a) - healthRank(b);
      return sortAsc ? value : -value;
    });
    return matches;
  }, [rows, q, healthFilter, sortKey, sortAsc]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const pageRows = filtered.slice((page - 1) * pageSize, page * pageSize);
  const offlineCount = rows.filter((row) => row.hasOffline).length;
  const underReplicatedCount = rows.filter((row) => row.hasUnderReplicated).length;
  const partitionCount = rows.reduce((sum, row) => sum + row.partitions, 0);

  const sort = (key: SortKey) => {
    if (sortKey === key) setSortAsc((asc) => !asc);
    else { setSortKey(key); setSortAsc(key === "topic"); }
  };
  const healthLabel = (row: TopicRow) => row.hasOffline ? "离线" : row.hasUnderReplicated ? "ISR 不足" : "健康";
  const healthClass = (row: TopicRow) => row.hasOffline ? "err" : row.hasUnderReplicated ? "warn" : "ok";

  return (
    <div>
      <PageHeader title="Topics" desc={`${conn.name} · Topic、分区与副本健康状态`}
        right={
          <>
            <button onClick={() => void load(conn.id)} disabled={loading} className="btn btn-ghost btn-sm">
              {loading ? "刷新中…" : "↻ 刷新"}
            </button>
            <button onClick={() => { if (readonly) return; setShowCreate((v) => !v); setCErr(null); setCSuccess(null); }}
              disabled={readonly} title={readonly ? "只读巡检模式下禁止创建 Topic" : undefined}
              className="btn btn-primary btn-sm">{showCreate ? "收起表单" : "+ 新建 Topic"}</button>
          </>
        } />

      {cSuccess && <div className="alert ok mb-3">{cSuccess}</div>}
      {err && <div className="alert err mb-3 flex items-center gap-3">加载失败：{err}
        <button onClick={() => void load(conn.id)} className="btn btn-ghost btn-sm ml-auto">重试</button>
      </div>}

      <div className="mb-3 grid grid-cols-2 gap-2.5 sm:grid-cols-4">
        {[
          ["Topics", rows.length, "当前集群总数", "sky"],
          ["分区", partitionCount, "全部 Topic 分区", "violet"],
          ["离线 Topic", offlineCount, offlineCount ? "存在无 Leader 分区" : "全部有 Leader", offlineCount ? "rose" : "emerald"],
          ["ISR 不足", underReplicatedCount, underReplicatedCount ? "副本未全部同步" : "副本同步正常", underReplicatedCount ? "amber" : "blue"],
        ].map(([label, value, sub, tone]) => (
          <div key={String(label)} className={`metric-card metric-${tone}`}>
            <div className="metric-label">{label}</div>
            <div className="mono mt-1.5 text-[20px] font-bold text-slate-800">{loading ? "…" : value}</div>
            <div className="mt-1 text-[10.5px] text-[var(--text-faint)]">{sub}</div>
          </div>
        ))}
      </div>

      {showCreate && (
        <div className="card mb-3 p-4">
          <div className="card-t">
            <span className="text-[13px] font-bold text-slate-800">创建 Topic</span>
            <span className="text-[11px] text-[var(--text-faint)]">将在 {conn.name} 上创建</span>
          </div>
          <fieldset disabled={readonly} className="m-0 min-w-0 border-0 p-0">
          <div className="grid grid-cols-1 items-end gap-3 sm:grid-cols-2 xl:grid-cols-[minmax(220px,1.4fr)_minmax(120px,.6fr)_minmax(160px,.8fr)_auto]">
            <div><label className="lbl">Topic 名称</label>
              <input className="field mono" value={cName} placeholder="如 order-events"
                onChange={(e) => setCName(e.target.value)} /></div>
            <div><label className="lbl">分区数</label>
              <input type="number" min={1} max={1000} className="field mono" value={cParts}
                onChange={(e) => setCParts(Math.min(1000, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
            <div><label className="lbl">副本数（当前 {brokerCount} 台 Broker）</label>
              <input type="number" min={1} max={Math.max(brokerCount, 1)} className="field mono" value={cRf}
                onChange={(e) => setCRf(Math.min(Math.max(brokerCount, 1), Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
            <button onClick={() => void createTopic()} disabled={readonly || cBusy || loading} title={readonly ? "只读巡检模式下禁止创建 Topic" : undefined} className="btn btn-primary">
              {cBusy ? "创建中…" : "创建 Topic"}
            </button>
            <div className="sm:col-span-2 xl:col-span-full">
              <div className="mb-2 text-[11px] font-semibold text-slate-600">常用配置</div>
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                <div><label className="lbl">消息保留时间</label>
                  <select className="field" value={cRetentionMs} onChange={(e) => setCRetentionMs(e.target.value)}>
                    <option value="">沿用 Broker 默认</option>
                    <option value="86400000">1 天</option>
                    <option value="259200000">3 天</option>
                    <option value="604800000">7 天</option>
                    <option value="2592000000">30 天</option>
                    <option value="7776000000">90 天</option>
                    <option value="-1">永久保留</option>
                  </select>
                </div>
                <div><label className="lbl">清理策略</label>
                  <select className="field" value={cCleanupPolicy} onChange={(e) => setCCleanupPolicy(e.target.value)}>
                    <option value="">沿用 Broker 默认</option>
                    <option value="delete">按保留策略删除（delete）</option>
                    <option value="compact">按 Key 压缩（compact）</option>
                    <option value="compact,delete">压缩并按期删除</option>
                  </select>
                </div>
                <div><label className="lbl">容量上限（MB，可选）</label>
                  <input type="number" min={1} step={1} className="field mono" value={cRetentionMb}
                    placeholder="留空表示沿用默认值" onChange={(e) => setCRetentionMb(e.target.value)} />
                </div>
              </div>
              <details className="mt-3 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-3">
                <summary className="cursor-pointer text-[11px] font-semibold text-slate-600">高级配置（JSON，可选）</summary>
                <div className="mt-2">
                  <textarea className="field mono min-h-[72px] py-2" value={cConfigs}
                    placeholder={'{"max.message.bytes": "1048576"}'}
                    onChange={(e) => setCConfigs(e.target.value)} />
                  <div className="mt-1 text-[10.5px] text-[var(--text-faint)]">配置值使用字符串；与上方常用配置重复的键会提示错误。</div>
                </div>
              </details>
            </div>
          </div>
          </fieldset>
          {cErr && <div className="alert err mt-3">{cErr}</div>}
        </div>
      )}

      <section className="card overflow-hidden">
        <div className="flex flex-wrap items-center gap-3 border-b border-[var(--border-soft)] p-3.5">
          <div className="min-w-[150px]">
            <div className="text-[13px] font-bold text-slate-800">Topic 列表</div>
            <div className="mt-0.5 text-[10.5px] text-[var(--text-faint)]">{filtered.length} / {rows.length} 个 Topic</div>
          </div>
          <label className="sr-only" htmlFor="topic-search">搜索 Topic</label>
          <input id="topic-search" placeholder="搜索 Topic 名称…" value={q}
            onChange={(e) => setQ(e.target.value)} className="field mono !h-[34px] min-w-[180px] flex-1 xl:max-w-sm" />
          <select aria-label="按健康状态筛选" value={healthFilter} onChange={(e) => setHealthFilter(e.target.value as HealthFilter)}
            className="field !h-[34px] !w-auto min-w-[145px]">
            <option value="all">全部状态</option>
            <option value="healthy">健康</option>
            <option value="offline">离线</option>
            <option value="under-replicated">ISR 不足</option>
          </select>
          {(healthFilter !== "all" || q) && <button onClick={() => { setQ(""); setHealthFilter("all"); }} className="btn btn-ghost btn-sm">清除</button>}
        </div>

        {loading && rows.length === 0 ? (
          <div className="py-14 text-center text-[12px] text-slate-500">正在读取 Kafka Metadata…</div>
        ) : err ? (
          <div className="py-10 text-center text-[12px] text-slate-500">暂时无法加载 Topic 列表。</div>
        ) : filtered.length === 0 ? (
          <div className="py-14 text-center">
            <div className="text-[13px] font-semibold text-slate-700">{rows.length ? "没有匹配的 Topic" : "当前集群还没有 Topic"}</div>
            <div className="mt-1 text-[11px] text-[var(--text-faint)]">{rows.length ? "试试调整搜索词或健康状态筛选。" : "可以通过右上角创建第一个 Topic。"}</div>
          </div>
        ) : (
          <>
            <div className="tbl-wrap max-h-[min(68vh,760px)] !rounded-none !border-0">
              <table className="tbl mono min-w-[760px] tabular-nums">
                <thead className="sticky top-0 z-10 bg-[var(--panel-2)]">
                  <tr>
                    {([["topic", "Topic"], ["partitions", "分区"], ["replication", "RF"], ["health", "状态"]] as [SortKey, string][]).map(([key, label]) => (
                      <th key={key} className={key === "topic" ? "" : "!text-center"}>
                        <button onClick={() => sort(key)} className="inline-flex items-center gap-1 hover:text-slate-700">
                          {label}<span className="text-sky-700">{sortKey === key ? sortAsc ? "↑" : "↓" : "↕"}</span>
                        </button>
                      </th>
                    ))}
                    <th>Leader Brokers</th>
                    <th className="!text-right">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {pageRows.map((row) => (
                    <tr key={row.topic} onClick={() => { window.location.hash = `#/topics/${encodeURIComponent(row.topic)}`; }}
                      className="cursor-pointer transition-colors hover:bg-slate-900/[0.04]">
                      <td className="max-w-[420px] truncate font-semibold text-slate-800" title={row.topic}>{row.topic}</td>
                      <td className="!text-center text-slate-600">{row.partitions}</td>
                      <td className="!text-center text-slate-600">{row.replication}</td>
                      <td className="!text-center"><span className={`badge ${healthClass(row)}`}>{healthLabel(row)}</span></td>
                      <td>
                        <div className="flex flex-wrap gap-1">
                          {row.leaders.length ? row.leaders.map((leader) => <span key={leader} className="badge mute">#{leader}</span>)
                            : <span className="text-[var(--text-faint)]">无 Leader</span>}
                        </div>
                      </td>
                      <td className="!text-right"><span className="text-[11px] text-sky-700">查看详情 →</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="flex flex-wrap items-center justify-between gap-3 border-t border-[var(--border-soft)] px-3.5 py-3">
              <span className="text-[11px] text-[var(--text-faint)]">
                显示 {(page - 1) * pageSize + 1}–{Math.min(page * pageSize, filtered.length)} / {filtered.length}
              </span>
              <div className="flex items-center gap-2">
                <label className="text-[11px] text-[var(--text-faint)]" htmlFor="topic-page-size">每页</label>
                <select id="topic-page-size" value={pageSize} onChange={(e) => setPageSize(Number(e.target.value))}
                  className="field !h-[30px] !w-[72px] !px-2 text-center">
                  {PAGE_SIZES.map((size) => <option key={size} value={size}>{size}</option>)}
                </select>
                <button onClick={() => setPage((n) => Math.max(1, n - 1))} disabled={page <= 1} className="btn btn-ghost btn-sm">上一页</button>
                <span className="mono min-w-[58px] text-center text-[11px] text-slate-600">{page} / {totalPages}</span>
                <button onClick={() => setPage((n) => Math.min(totalPages, n + 1))} disabled={page >= totalPages} className="btn btn-ghost btn-sm">下一页</button>
              </div>
            </div>
          </>
        )}
      </section>
    </div>
  );
}
