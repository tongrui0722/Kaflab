import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { useConn } from "../conn";
import { useAuth, authHeaders } from "../auth";
import { PageHeader } from "../components/bits";
import { TopicMessages } from "./TopicMessages";
import { TopicConfigPanel } from "./TopicConfig";

const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";

interface PartRow {
  partition: number; leader: number; replicas: number[]; isr: number[]; error: number;
}

type Tab = "overview" | "partitions" | "config" | "messages";

export function TopicDetailPage() {
  const { name } = useParams<{ name: string }>();
  const topic = decodeURIComponent(name ?? "");
  const { conn } = useConn();
  const { mode } = useAuth();
  const readonly = mode === "readonly";
  const [tab, setTab] = useState<Tab>("overview");
  const [detail, setDetail] = useState<PartRow[] | null>(null);
  const [brokers, setBrokers] = useState<{ id?: number; host: string; port: number }[]>([]);
  const [detailLoading, setDetailLoading] = useState(false);
  const [fetchErr, setFetchErr] = useState<string | null>(null);
  const [confirmDel, setConfirmDel] = useState(false);
  const [delBusy, setDelBusy] = useState(false);
  const [expTarget, setExpTarget] = useState(0);
  const [expBusy, setExpBusy] = useState(false);
  const [expMsg, setExpMsg] = useState<string | null>(null);
  const [rfTarget, setRfTarget] = useState(0);
  const [rfBusy, setRfBusy] = useState(false);
  const [rfMsg, setRfMsg] = useState<string | null>(null);
  const [partitionQuery, setPartitionQuery] = useState("");
  const [partitionHealth, setPartitionHealth] = useState<"all" | "healthy" | "offline" | "under-replicated">("all");

  const loadDetail = async () => {
    setDetailLoading(true);
    setFetchErr(null);
    try {
      const r = await fetch(`${API}/api/clusters/${conn.id}/topics/${encodeURIComponent(topic)}`);
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const d = await r.json() as { partitions: PartRow[]; brokers: typeof brokers };
      setDetail(d.partitions);
      setBrokers(d.brokers ?? []);
      setExpTarget(d.partitions.length);
      setRfTarget(Math.max(...d.partitions.map((p) => p.replicas.length), 1));
    } catch (e) {
      setDetail(null);
      setFetchErr(e instanceof Error ? e.message : String(e));
    } finally {
      setDetailLoading(false);
    }
  };

  useEffect(() => {
    setDetail(null);
    setTab("overview");
    setConfirmDel(false);
    loadDetail();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [conn.id, topic]);

  const openInConsumer = () => {
    try {
      localStorage.setItem("kcl-consumer-prefill",
        JSON.stringify({ topic, partition: 0, maxMessages: 10 }));
    } catch { /* ignore */ }
    window.location.hash = "#/consumer";
  };

  const deleteTopic = async () => {
    setDelBusy(true);
    try {
      const r = await fetch(`${API}/api/clusters/${conn.id}/topics/${encodeURIComponent(topic)}`, {
        method: "DELETE", headers: { ...authHeaders() },
      });
      if (!r.ok) throw new Error(await r.text());
      window.location.hash = "#/topics";
    } catch (e) {
      setFetchErr(e instanceof Error ? e.message : String(e));
    } finally {
      setDelBusy(false);
    }
  };

  const expandPartitions = async () => {
    if (!detail) return;
    if (expTarget <= detail.length) { setExpMsg(`目标须大于当前 ${detail.length}`); return; }
    setExpBusy(true);
    setExpMsg(null);
    try {
      const r = await fetch(`${API}/api/clusters/${conn.id}/topics/${encodeURIComponent(topic)}/partitions`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ count: expTarget }),
      });
      if (!r.ok) throw new Error(await r.text());
      setExpMsg(`已扩容到 ${expTarget} 分区`);
      await loadDetail();
    } catch (e) {
      setExpMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setExpBusy(false);
    }
  };

  const applyReplicas = async () => {
    if (!detail) return;
    const curRf = Math.max(...detail.map((p) => p.replicas.length), 1);
    if (rfTarget < 1) { setRfMsg("副本数至少为 1"); return; }
    if (rfTarget === curRf) { setRfMsg("与当前相同，无需调整"); return; }
    setRfBusy(true);
    setRfMsg(null);
    try {
      const r = await fetch(`${API}/api/clusters/${conn.id}/topics/${encodeURIComponent(topic)}/reassignment`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ replication: rfTarget }),
      });
      if (!r.ok) throw new Error(await r.text());
      const d = await r.json() as { note?: string };
      setRfMsg(d.note ?? "已提交，后台搬迁中");
      await loadDetail();
    } catch (e) {
      setRfMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setRfBusy(false);
    }
  };

  const rf = detail ? Math.max(...detail.map((p) => p.replicas.length), 1) : 0;
  const offlinePartitions = detail?.filter((p) => p.leader < 0).length ?? 0;
  const underReplicatedPartitions = detail?.filter((p) => p.isr.length < p.replicas.length).length ?? 0;
  const minIsr = detail?.length ? Math.min(...detail.map((p) => p.isr.length)) : 0;
  const leaderDistribution = detail
    ? [...new Set(detail.map((p) => p.leader))].sort((a, b) => a - b)
      .map((leader) => ({ leader, count: detail.filter((p) => p.leader === leader).length }))
    : [];
  const visiblePartitions = (detail ?? []).filter((p) => {
    const search = partitionQuery.trim();
    const matchesQuery = !search || String(p.partition).includes(search)
      || String(p.leader).includes(search) || p.replicas.join(",").includes(search);
    const offline = p.leader < 0;
    const underReplicated = p.isr.length < p.replicas.length;
    const matchesHealth = partitionHealth === "all"
      || (partitionHealth === "healthy" && !offline && !underReplicated)
      || (partitionHealth === "offline" && offline)
      || (partitionHealth === "under-replicated" && !offline && underReplicated);
    return matchesQuery && matchesHealth;
  });

  return (
    <div>
      <PageHeader
        title={topic}
        desc={`${conn.name} · ${detail ? `${detail.length} 分区 · RF ${rf}` : "加载中"}`}
        right={
          <>
            <button onClick={() => { window.location.hash = "#/topics"; }} className="btn btn-ghost btn-sm">← Topics</button>
            {!confirmDel && (
              <button onClick={() => { if (!readonly) setConfirmDel(true); }} disabled={readonly}
                title={readonly ? "只读巡检模式下禁止删除 Topic" : undefined} className="btn btn-danger-ghost btn-sm">删除</button>
            )}
            {confirmDel && (
              <>
                <span className="badge err">确认删除 Topic？</span>
                <button onClick={deleteTopic} disabled={readonly || delBusy} title={readonly ? "只读巡检模式下禁止删除 Topic" : undefined} className="btn btn-danger-ghost btn-sm">
                  {delBusy ? "删除中…" : "确认删除"}
                </button>
                <button onClick={() => setConfirmDel(false)} disabled={delBusy} className="btn btn-ghost btn-sm">取消</button>
              </>
            )}
            <button onClick={() => void loadDetail()} disabled={detailLoading} className="btn btn-ghost btn-sm">
              {detailLoading ? "刷新中…" : "↻ 刷新"}
            </button>
          </>
        }
      />
      {fetchErr && <div className="alert err mb-3 flex items-center gap-3">{fetchErr}
        <button onClick={() => void loadDetail()} className="btn btn-ghost btn-sm ml-auto">重试</button>
      </div>}
      <div className="card overflow-hidden">
        <div role="tablist" aria-label="Topic 详情页面" className="flex gap-1 overflow-x-auto border-b border-[var(--border-soft)] px-3 pt-2 text-[12.5px] sm:px-4">
          {(["overview", "partitions", "config", "messages"] as const).map((t) => (
            <button key={t} role="tab" aria-selected={tab === t} onClick={() => setTab(t)}
              className={`relative shrink-0 rounded-t-lg px-3 pb-3 pt-2 transition-colors ${tab === t ? "font-semibold text-sky-700" : "text-[var(--text-faint)] hover:bg-slate-900/[0.04] hover:text-slate-600"}`}>
              {t === "overview" ? "概览" : t === "partitions" ? "分区" : t === "config" ? "配置" : "消息"}
              {t === "partitions" && detail && <span className="badge mute ml-1.5">{detail.length}</span>}
              {tab === t && <span className="absolute inset-x-0 -bottom-px h-[2px] rounded-full bg-sky-400" />}
            </button>
          ))}
        </div>
        <div role="tabpanel" className="p-3 sm:p-4">
          {tab === "overview" && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-2.5 xl:grid-cols-4">
                {[
                  { label: "分区", value: detail?.length ?? "—", note: "Topic 分区总数", tone: "sky" },
                  { label: "副本因子", value: detail ? rf : "—", note: `${brokers.length} 台 Broker`, tone: "violet" },
                  { label: "离线分区", value: detail ? offlinePartitions : "—", note: offlinePartitions ? "没有可用 Leader" : "全部有 Leader", tone: offlinePartitions ? "rose" : "emerald" },
                  { label: "ISR 不足", value: detail ? underReplicatedPartitions : "—", note: underReplicatedPartitions ? "副本尚未全部同步" : "副本同步正常", tone: underReplicatedPartitions ? "amber" : "blue" },
                ].map((item) => (
                  <div key={item.label} className={`metric-card metric-${item.tone}`}>
                    <div className="metric-label">{item.label}</div>
                    <div className="mono mt-1.5 text-[20px] font-bold text-slate-800">{detailLoading ? "…" : item.value}</div>
                    <div className="mt-1 truncate text-[10.5px] text-[var(--text-faint)]">{item.note}</div>
                  </div>
                ))}
              </div>
              <div className="grid gap-3 xl:grid-cols-2">
                <div className="rounded-xl border border-[var(--border-soft)] bg-[var(--panel-3)] p-4">
                  <div className="mb-3 flex items-center gap-2">
                    <span className="card-title">Broker 与 Leader 分布</span>
                    <span className={`badge ${offlinePartitions || underReplicatedPartitions ? "warn" : "ok"}`}>
                      {offlinePartitions ? "存在离线分区" : underReplicatedPartitions ? "副本同步中" : "健康"}
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {brokers.map((broker) => {
                      const leaders = leaderDistribution.find((item) => item.leader === broker.id)?.count ?? 0;
                      return <div key={broker.id} className="flex min-w-[170px] flex-1 items-center gap-2 rounded-lg border border-[var(--border-soft)] px-3 py-2.5">
                        <span className="badge run">#{broker.id}</span>
                        <span className="mono min-w-0 flex-1 truncate text-[11px] text-slate-600">{broker.host}:{broker.port}</span>
                        <span className="text-[10px] text-[var(--text-faint)]">{leaders} Leaders</span>
                      </div>;
                    })}
                    {brokers.length === 0 && <div className="py-3 text-[11px] text-[var(--text-faint)]">暂无 Broker 元数据。</div>}
                  </div>
                </div>
                <div className="rounded-xl border border-[var(--border-soft)] bg-[var(--panel-3)] p-4">
                  <div className="card-title mb-3">消息快捷入口</div>
                  <div className="flex flex-wrap gap-2">
                    <button onClick={openInConsumer} className="btn btn-info btn-sm flex-1">在 Consumer 页读取 P0 最新消息 →</button>
                    <button onClick={() => {
                      try { localStorage.setItem("kcl-producer-prefill", JSON.stringify({ topic })); } catch { /* ignore */ }
                      window.location.hash = "#/producer";
                    }} className="btn btn-ghost btn-sm">前往 Producer →</button>
                  </div>
                </div>
              </div>
              {detail && (
                <div className="grid gap-3 xl:grid-cols-2">
                  <div className="rounded-xl border border-[var(--border-soft)] bg-[var(--panel-3)] p-4">
                    <div className="text-[12px] font-semibold text-slate-700">分区扩容</div>
                    <div className="mb-3 mt-1 text-[10.5px] text-[var(--text-faint)]">只能增加分区数，扩容后不可缩减。</div>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="mono text-[11px] text-slate-400">{detail.length} →</span>
                      <input aria-label="目标分区数" type="number" min={detail.length + 1} max={1000} value={expTarget} disabled={readonly}
                        onChange={(e) => setExpTarget(Math.floor(Number(e.target.value) || 0))} className="field mono nospin !h-[32px] !w-24 text-center" />
                      <button onClick={expandPartitions} disabled={readonly || expBusy || expTarget <= detail.length} title={readonly ? "只读巡检模式下禁止扩容分区" : undefined} className="btn btn-ghost btn-sm">{expBusy ? "提交中…" : "扩容分区"}</button>
                    </div>
                    {expMsg && <div className={`alert mt-3 ${expMsg.includes("失败") || expMsg.includes("须") ? "err" : "info"}`}>{expMsg}</div>}
                  </div>
                  <div className="rounded-xl border border-[var(--border-soft)] bg-[var(--panel-3)] p-4">
                    <div className="text-[12px] font-semibold text-slate-700">副本调整</div>
                    <div className="mb-3 mt-1 text-[10.5px] text-[var(--text-faint)]">目标副本数不能超过 Broker 数（{brokers.length}）。</div>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="mono text-[11px] text-slate-400">RF {rf} →</span>
                      <input aria-label="目标副本数" type="number" min={1} max={Math.max(brokers.length, 1)} value={rfTarget} disabled={readonly}
                        onChange={(e) => setRfTarget(Math.floor(Number(e.target.value) || 0))} className="field mono nospin !h-[32px] !w-24 text-center" />
                      <button onClick={applyReplicas} disabled={readonly || rfBusy || rfTarget < 1 || rfTarget > brokers.length || rfTarget === rf} title={readonly ? "只读巡检模式下禁止调整副本" : undefined} className="btn btn-ghost btn-sm">{rfBusy ? "提交中…" : "应用副本调整"}</button>
                    </div>
                    {rfMsg && <div className={`alert mt-3 ${rfMsg.toLowerCase().includes("failed") || rfMsg.includes("失败") ? "err" : "info"}`}>{rfMsg}</div>}
                  </div>
                </div>
              )}
            </div>
          )}
          {tab === "partitions" && (
            detail ? (
              <div className="space-y-3">
                <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
                  <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-3"><div className="text-[10px] text-[var(--text-faint)]">全部分区</div><div className="mono mt-1 text-[16px] font-semibold text-slate-800">{detail.length}</div></div>
                  <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-3"><div className="text-[10px] text-[var(--text-faint)]">离线分区</div><div className={`mono mt-1 text-[16px] font-semibold ${offlinePartitions ? "text-rose-600" : "text-emerald-700"}`}>{offlinePartitions}</div></div>
                  <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-3"><div className="text-[10px] text-[var(--text-faint)]">ISR 不足</div><div className={`mono mt-1 text-[16px] font-semibold ${underReplicatedPartitions ? "text-amber-700" : "text-emerald-700"}`}>{underReplicatedPartitions}</div></div>
                  <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-3"><div className="text-[10px] text-[var(--text-faint)]">最小 ISR</div><div className="mono mt-1 text-[16px] font-semibold text-slate-800">{minIsr}</div></div>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <input value={partitionQuery} onChange={(e) => setPartitionQuery(e.target.value)} className="field mono !h-[34px] min-w-[180px] flex-1 sm:max-w-xs" placeholder="搜索分区、Leader 或 Broker ID…" aria-label="搜索分区" />
                  <select value={partitionHealth} onChange={(e) => setPartitionHealth(e.target.value as typeof partitionHealth)} className="field !h-[34px] !w-auto min-w-[145px]" aria-label="分区健康筛选">
                    <option value="all">全部分区</option><option value="healthy">健康</option><option value="offline">离线</option><option value="under-replicated">ISR 不足</option>
                  </select>
                  <span className="text-[10.5px] text-[var(--text-faint)]">{visiblePartitions.length} / {detail.length} 个分区</span>
                </div>
                <div className="scroll-box max-h-[min(65vh,680px)] overflow-auto overscroll-contain rounded-lg border border-[var(--border-soft)]">
                  <table className="tbl mono min-w-[680px] tabular-nums">
                    <thead className="sticky top-0 z-10 bg-[var(--panel-2)]"><tr>
                      <th className="!text-center">分区</th><th className="!text-center">Leader</th><th>Replicas</th><th>ISR</th><th className="!text-center">健康</th>
                    </tr></thead>
                    <tbody>
                      {visiblePartitions.map((p) => {
                        const offline = p.leader < 0;
                        const underReplicated = p.isr.length < p.replicas.length;
                        return <tr key={p.partition} className="transition-colors hover:bg-slate-900/[0.04]">
                          <td className="!text-center text-slate-600">{p.partition}</td>
                          <td className="!text-center">{offline ? <span className="badge err">无 Leader</span> : <span className="badge run">#{p.leader}</span>}</td>
                          <td className="text-slate-400">{p.replicas.map((id) => <span key={id} className="badge mute mr-1">#{id}</span>)}</td>
                          <td className="text-slate-400">{p.isr.map((id) => <span key={id} className="badge mute mr-1">#{id}</span>)}</td>
                          <td className="!text-center">{offline ? <span className="badge err">离线</span> : underReplicated ? <span className="badge warn">ISR 不足</span> : <span className="badge ok">健康</span>}</td>
                        </tr>;
                      })}
                      {visiblePartitions.length === 0 && <tr><td colSpan={5} className="py-8 text-center text-[11px] text-[var(--text-faint)]">没有匹配的分区</td></tr>}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : (
              <div className="py-8 text-center text-[12px] text-slate-500">分区加载中…</div>
            )
          )}
          {tab === "config" && (
            <TopicConfigPanel key={`${conn.id}/${topic}`} cid={conn.id} topic={topic} readonly={readonly} />
          )}
          {tab === "messages" && (
            detail ? (
              <div className="space-y-3">
                <div className="flex flex-wrap items-end justify-between gap-2">
                  <div><div className="text-[13px] font-bold text-slate-800">消息浏览器</div><div className="mt-1 text-[10.5px] text-[var(--text-faint)]">按分区、Offset 或时间定位；只读读取，不修改消息。</div></div>
                  <span className="badge mute">{detail.length} 个分区</span>
                </div>
                <TopicMessages cid={conn.id} topic={topic} partitions={detail.map((p) => p.partition)} />
              </div>
            ) : (
              <div className="py-8 text-center text-[12px] text-slate-500">分区加载中…</div>
            )
          )}
        </div>
      </div>
    </div>
  );
}
