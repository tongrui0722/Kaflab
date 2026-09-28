import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { useConn } from "../conn";
import { useAuth, authHeaders } from "../auth";
import { KV, PageHeader } from "../components/bits";
import { TopicMessages } from "./TopicMessages";

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
  const [fetchErr, setFetchErr] = useState<string | null>(null);
  const [confirmDel, setConfirmDel] = useState(false);
  const [delBusy, setDelBusy] = useState(false);
  const [expTarget, setExpTarget] = useState(0);
  const [expBusy, setExpBusy] = useState(false);
  const [expMsg, setExpMsg] = useState<string | null>(null);
  const [rfTarget, setRfTarget] = useState(0);
  const [rfBusy, setRfBusy] = useState(false);
  const [rfMsg, setRfMsg] = useState<string | null>(null);

  const loadDetail = async () => {
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

  return (
    <div>
      <PageHeader
        title={topic}
        desc={`${conn.name} · ${detail ? `${detail.length} 分区 · RF ${rf}` : "加载中"}`}
        right={
          <>
            <button onClick={() => { window.location.hash = "#/topics"; }} className="btn btn-ghost btn-sm">← Topics</button>
            {!readonly && !confirmDel && (
              <button onClick={() => setConfirmDel(true)} className="btn btn-danger-ghost btn-sm">删除</button>
            )}
            {!readonly && confirmDel && (
              <button onClick={deleteTopic} disabled={delBusy} className="btn btn-danger-ghost btn-sm">
                {delBusy ? "删除中…" : `确认删除 ${topic}？`}
              </button>
            )}
          </>
        }
      />
      {fetchErr && <div className="alert warn mb-3">{fetchErr}</div>}
      <div className="card px-3 pt-2">
        <div className="flex gap-4 border-b border-[var(--border-soft)] text-[12.5px]">
          {(["overview", "partitions", "config", "messages"] as const).map((t) => (
            <button key={t} onClick={() => setTab(t)}
              className={`relative pb-2.5 transition-colors ${tab === t ? "font-semibold text-sky-300" : "text-[var(--text-faint)] hover:text-slate-300"}`}>
              {t === "overview" ? "Overview" : t === "partitions" ? `Partitions${detail ? ` (${detail.length})` : ""}` : t === "config" ? "Configuration" : "Messages"}
              {tab === t && <span className="absolute inset-x-0 -bottom-px h-[2px] rounded-full bg-sky-400" />}
            </button>
          ))}
        </div>
        <div className="py-3">
          {tab === "overview" && (
            <div className="grid gap-2.5 xl:grid-cols-2">
              <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-3">
                <div className="card-title mb-2">快捷操作</div>
                <div className="flex gap-1.5">
                  <button onClick={openInConsumer} className="btn btn-info btn-sm flex-1">
                    在 Consumer 页读 P0 最新 10 条 →
                  </button>
                  <button
                    onClick={() => {
                      try { localStorage.setItem("kcl-producer-prefill", JSON.stringify({ topic })); } catch { /* ignore */ }
                      window.location.hash = "#/producer";
                    }}
                    className="btn btn-ghost btn-sm shrink-0">
                    去发送 →
                  </button>
                </div>
                <div className="mt-2">
                  <KV k="Brokers" v={brokers.map((b) => `#${b.id} ${b.host}:${b.port}`).join(" · ")} mono />
                </div>
              </div>
              {!readonly && detail && (
                <div className="divide-y divide-[var(--border-soft)] rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-3 py-1">
                  <div className="flex h-10 items-center gap-2">
                    <span className="w-16 shrink-0 text-[11.5px] text-slate-500">分区扩容</span>
                    <span className="mono rounded bg-white/[0.04] px-1.5 py-0.5 text-[11px] text-slate-400">{detail.length} →</span>
                    <input type="number" min={detail.length + 1} max={1000} value={expTarget}
                      onChange={(e) => setExpTarget(Math.floor(Number(e.target.value) || 0))}
                      className="field mono nospin !h-[28px] !w-20 !px-1.5 text-center" />
                    <button onClick={expandPartitions} disabled={expBusy} className="btn btn-ghost btn-sm !min-h-[28px]">扩容</button>
                    {expMsg && <span className="mono min-w-0 flex-1 truncate text-right text-[11px] text-slate-500" title={expMsg}>{expMsg}</span>}
                  </div>
                  <div className="flex h-10 items-center gap-2">
                    <span className="w-16 shrink-0 text-[11.5px] text-slate-500">副本调整</span>
                    <span className="mono rounded bg-white/[0.04] px-1.5 py-0.5 text-[11px] text-slate-400">RF {rf} →</span>
                    <input type="number" min={1} max={Math.max(brokers.length, 1)} value={rfTarget}
                      onChange={(e) => setRfTarget(Math.floor(Number(e.target.value) || 0))}
                      className="field mono nospin !h-[28px] !w-20 !px-1.5 text-center" />
                    <button onClick={applyReplicas} disabled={rfBusy} className="btn btn-ghost btn-sm !min-h-[28px]">应用</button>
                    {rfMsg && <span className="mono min-w-0 flex-1 truncate text-right text-[11px] text-slate-500" title={rfMsg}>{rfMsg}</span>}
                  </div>
                </div>
              )}
            </div>
          )}
          {tab === "partitions" && (
            detail ? (
              <div className="scroll-box max-h-[560px] overflow-y-auto overscroll-contain rounded-lg border border-[var(--border-soft)]">
                <table className="tbl mono tabular-nums">
                  <thead className="sticky top-0 bg-[var(--panel-2)]"><tr>
                    <th className="!text-center">P</th>
                    <th className="!text-center">Leader</th>
                    <th>Replicas</th>
                    <th>ISR</th>
                  </tr></thead>
                  <tbody>
                    {detail.map((p) => (
                      <tr key={p.partition} className="transition-colors hover:bg-white/[0.025]">
                        <td className="!text-center text-slate-300">{p.partition}</td>
                        <td className="!text-center"><span className="badge run">{p.leader}</span></td>
                        <td className="text-slate-400">{p.replicas.join(", ")}</td>
                        <td className="text-slate-400">{p.isr.join(", ")}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="py-8 text-center text-[12px] text-slate-500">分区加载中…</div>
            )
          )}
          {tab === "config" && (
            detail ? (
              <div className="grid max-w-3xl gap-1">
                <KV k="分区数" v={String(detail.length)} mono />
                <KV k="副本因子" v={String(rf)} mono />
                <KV k="最小 ISR" v={String(Math.min(...detail.map((p) => p.isr.length), 0))} mono />
                <KV k="Leader 分布" v={[...new Set(detail.map((p) => p.leader))].map((l) => `#${l} × ${detail.filter((p) => p.leader === l).length}`).join(" · ")} mono />
                <KV k="离线分区" v={detail.some((p) => p.leader < 0) ? "存在" : "无"} mono />
                <div className="pt-1 text-[11px] leading-relaxed text-slate-600">
                  broker 侧 Topic 配置项（retention / cleanup.policy 等）需 DescribeConfigs API，当前协议库暂未支持。
                </div>
              </div>
            ) : (
              <div className="py-8 text-center text-[12px] text-slate-500">加载中…</div>
            )
          )}
          {tab === "messages" && (
            detail ? (
              <TopicMessages cid={conn.id} topic={topic} partitions={detail.map((p) => p.partition)} />
            ) : (
              <div className="py-8 text-center text-[12px] text-slate-500">分区加载中…</div>
            )
          )}
        </div>
      </div>
    </div>
  );
}
