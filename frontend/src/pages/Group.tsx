import { useCallback, useEffect, useRef, useState } from "react";
import { DEFAULT_SECURITY } from "../api/producer";
import { useConn } from "../conn";
import { useAuth, authHeaders } from "../auth";
import { useLab } from "../lab";
import { PageHeader } from "../components/bits";

const inputCls = "field mono";
const labelCls = "lbl";
const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";

interface Member {
  memberId: string; clientId: string; clientHost: string;
  assignment?: { topic: string; partitions: number[] }[];
}
interface GroupRow {
  groupId: string; state?: string | null; protocolType?: string | null;
  protocol?: string | null; memberCount?: number; members?: Member[];
  coordinator?: { id?: number; host: string; port: number };
  totalLag?: number; scopedPartitions?: number; uncommitted?: number;
  topics?: string[]; error?: string;
}
interface LagRow {
  topic: string; partition: number; leader: number;
  end: number | null; committed: number | null; lag: number | null;
}

export function GroupPage() {
  const { conn } = useConn();
  const { mode } = useAuth();
  const readonly = mode === "readonly";
  const { requireLab } = useLab();
  const [form, setForm] = useState({
    bootstrap: "192.168.127.129:19092", groupId: "test-group", topics: "test-topic",
    ...DEFAULT_SECURITY, requestTimeoutMs: 10000,
  });
  const [rows, setRows] = useState<GroupRow[]>([]);
  const [sel, setSel] = useState<string | null>(null);
  const [lag, setLag] = useState<{ totalLag: number; uncommitted: number; rows: LagRow[] } | null>(null);
  const [joinOut, setJoinOut] = useState<Record<string, unknown> | null>(null);
  const [rebalOut, setRebalOut] = useState<{
    generations: Record<string, number | null>; rebalanced: boolean; note: string;
    steps: { step: string; generation?: number | null; memberId?: string; errorCode?: number | null; errorName?: string; note?: string }[];
  } | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [auto, setAuto] = useState(false);
  const [filter, setFilter] = useState("");
  const [fetchedAt, setFetchedAt] = useState<number | null>(null);

  const secBody = () => ({
    ...form, clientId: "kafka-sim-01",
    bootstrap: conn.bootstrap,
    securityProtocol: conn.securityProtocol,
    saslMechanism: conn.saslMechanism,
    saslUsername: conn.saslUsername,
    saslPassword: conn.saslPassword,
  });

  const loadOverview = useCallback(async (silent = false) => {
    if (!silent) { setLoading(true); setBusy("overview"); }
    try {
      const r = await fetch(`${API}/api/clusters/${conn.id}/groups/overview`);
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const d = await r.json() as { groups: GroupRow[] };
      setRows(d.groups);
      setFetchedAt(Date.now());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      if (!silent) { setLoading(false); setBusy(null); }
    }
  }, [conn.id]);

  useEffect(() => { loadOverview(); }, [loadOverview]);

  const overviewRef = useRef(loadOverview);
  useEffect(() => { overviewRef.current = loadOverview; });
  useEffect(() => {
    if (!auto) return;
    const t = setInterval(() => { overviewRef.current(true); }, 8000);
    return () => clearInterval(t);
  }, [auto]);

  const loadLag = async (gid: string) => {
    setBusy("lag");
    setError(null);
    try {
      const ts = form.topics.split(",").map((t) => t.trim()).filter(Boolean).join(",");
      const r = await fetch(`${API}/api/clusters/${conn.id}/groups/${encodeURIComponent(gid)}/lag?topics=${encodeURIComponent(ts)}`);
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      setLag(await r.json());
    } catch (e) {
      setLag(null);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  };

  const pick = (gid: string) => {
    setSel(gid);
    setForm((f) => ({ ...f, groupId: gid }));
    setLag(null);
    loadLag(gid);
  };

  const joinDemo = async () => {
    setLoading(true); setBusy("join"); setError(null);
    try {
      const r = await fetch(`${API}/api/groups/join`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ ...secBody(), topics: form.topics.split(",").map((t) => t.trim()).filter(Boolean) }),
      });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      setJoinOut(await r.json());
      await loadOverview(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setLoading(false); setBusy(null); }
  };

  const rebalanceDemo = async () => {
    setLoading(true); setBusy("rebalance"); setError(null); setRebalOut(null);
    try {
      const r = await fetch(`${API}/api/groups/rebalance`, {
        method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ ...secBody(), topics: form.topics.split(",").map((t) => t.trim()).filter(Boolean) }),
      });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      setRebalOut(await r.json());
      await loadOverview(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setLoading(false); setBusy(null); }
  };

  const shown = rows.filter((r) => r.groupId.includes(filter.trim()));
  const selRow = rows.find((r) => r.groupId === sel) ?? null;
  const activeCount = rows.filter((r) => (r.state ?? "") !== "Empty" && !r.error).length;
  const totalLagAll = rows.reduce((a, r) => a + (r.totalLag ?? 0), 0);

  return (
    <div>
      <PageHeader
        title="Consumer Group"
        desc="状态、成员与积压，点一行看明细"
        right={
          <>
            {busy && <span className="badge run pulsing">{busy === "overview" ? "拉取中…" : busy === "lag" ? "LAG 查询中…" : busy === "join" ? "入组演示中…" : "Rebalance 演示中（约20s）…"}</span>}
            {fetchedAt && <span className="badge mute mono">{new Date(fetchedAt).toLocaleTimeString("zh-CN", { hour12: false })}</span>}
            <button onClick={() => setAuto((v) => !v)} className={`btn btn-sm ${auto ? "btn-warn" : "btn-ghost"}`}>
              {auto ? "自动刷新 · 8s" : "自动刷新关"}
            </button>
            <button onClick={() => loadOverview()} disabled={loading} className="btn btn-primary btn-sm">
              刷新
            </button>
          </>
        }
      />

      {error && <div className="alert err mb-3">{error}</div>}

      {/* 全量一览 */}
      <div className="card p-3.5">
        <div className="card-t">
          <span className="text-[13px] font-bold text-slate-100">消费组一览</span>
          <span className="badge mute">{rows.length} 组</span>
          <span className="badge ok">{activeCount} 活跃</span>
          {totalLagAll > 0 && <span className="badge warn">总 LAG {totalLagAll}</span>}
          <input placeholder="过滤 Group ID" value={filter} onChange={(e) => setFilter(e.target.value)}
            className="field mono ml-auto !h-[30px] !w-44 !text-[11.5px]" />
        </div>
        {loading && rows.length === 0 && <div className="py-6 text-center text-[12px] text-slate-500">ListGroups → DescribeGroups 拉取中…</div>}
        {!loading && rows.length === 0 && (
          <div className="py-8 text-center text-[12px] text-slate-500">
            该集群暂无消费组。去下方「入组演示」Join 一次就会出现。
          </div>
        )}
        {rows.length > 0 && (
          <div className="tbl-wrap">
            <table className="tbl">
              <thead><tr>
                <th>Group ID</th><th>State</th><th>成员</th><th>消费者 ID / 连接</th>
                <th>订阅 Topics</th><th>LAG</th><th>Coordinator</th><th></th>
              </tr></thead>
              <tbody>
                {shown.map((r) => {
                  const isSel = r.groupId === sel;
                  return (
                    <tr key={r.groupId} onClick={() => pick(r.groupId)}
                      className={`cursor-pointer ${isSel ? "!bg-[rgba(56,189,248,0.09)]" : ""}`}>
                      <td className="mono max-w-[200px] truncate font-semibold text-slate-100">{r.groupId}</td>
                      <td>
                        {r.error ? <span className="badge err">查询失败</span>
                          : (r.state ?? "") === "Empty"
                            ? <span className="badge mute">Empty</span>
                            : <span className="badge run">{r.state ?? "?"}</span>}
                      </td>
                      <td>
                        <span className={`badge ${(r.memberCount ?? 0) > 0 ? "ok" : "mute"}`}>
                          {r.memberCount ?? 0}
                        </span>
                      </td>
                      <td className="mono text-[11px] text-slate-400">
                        {(r.members ?? []).length === 0
                          ? <span className="text-slate-600">无活跃成员</span>
                          : (r.members ?? []).map((m) => (
                            <div key={m.memberId} className="truncate">
                              {m.clientId || "—"} <span className="text-slate-600">@</span> {m.clientHost || "—"}
                            </div>
                          ))}
                      </td>
                      <td className="mono max-w-[160px] truncate text-[11px] text-slate-400">
                        {(r.topics ?? []).length === 0 ? <span className="text-slate-600">—</span> : (r.topics ?? []).join(", ")}
                      </td>
                      <td>
                        {r.error ? <span className="text-slate-600">—</span>
                          : (r.totalLag ?? 0) > 0
                            ? <span className="badge warn">{r.totalLag}</span>
                            : (r.scopedPartitions ?? 0) === 0
                              ? <span className="badge mute">N/A</span>
                              : <span className="badge ok">0</span>}
                      </td>
                      <td className="mono text-[11px] text-slate-500">
                        {r.coordinator ? `#${r.coordinator.id}` : "—"}
                      </td>
                      <td className="text-right">
                        <span className="btn btn-info btn-sm !min-h-[26px]">查看 →</span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <div className="mt-1.5 text-[11px] leading-relaxed text-[var(--text-faint)]">
          LAG = 各分区 log-end − 已提交，按该组提交过的 topic 统计；从未提交过的组显示 N/A（不拿全集群分区充数）。
        </div>
      </div>

      <div className="mt-3 space-y-3">
        {/* 选中组明细 */}
          <div className="card p-3.5">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-100">组明细</span>
              {selRow ? <span className="badge run mono">{selRow.groupId}</span> : <span className="badge mute">未选择</span>}
            </div>
            {!selRow && <div className="py-6 text-center text-[12px] text-slate-500">点上方列表任意一行查看成员与分区明细。</div>}
            {selRow && (
              <>
                <div className="grid grid-cols-2 gap-2 xl:grid-cols-4">
                  <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
                    <div className="card-title">State</div>
                    <div className="mono mt-0.5 truncate text-[12px] text-slate-100">{selRow.state ?? "—"}</div>
                  </div>
                  <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
                    <div className="card-title">Protocol</div>
                    <div className="mono mt-0.5 truncate text-[12px] text-slate-100" title={selRow.protocolType || selRow.protocol || "—"}>{selRow.protocolType || selRow.protocol || "—"}</div>
                  </div>
                  <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
                    <div className="card-title">成员</div>
                    <div className="mono mt-0.5 text-[12px] text-slate-100">{selRow.memberCount ?? 0}</div>
                  </div>
                  <div className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
                    <div className="card-title">Coordinator</div>
                    <div className="mono mt-0.5 truncate text-[12px] text-slate-100" title={selRow.coordinator ? `#${selRow.coordinator.id} ${selRow.coordinator.host}:${selRow.coordinator.port}` : "—"}>
                      {selRow.coordinator ? `#${selRow.coordinator.id}` : "—"}
                    </div>
                  </div>
                </div>

                {(selRow.members ?? []).length > 0 && (
                  <div className="mt-2.5">
                    <div className="card-title mb-1">成员与分配（{selRow.members?.length}）</div>
                    <div className="tbl-wrap">
                      <table className="tbl">
                        <thead><tr><th>消费者 ID</th><th>连接</th><th>Member ID</th><th>分配</th></tr></thead>
                        <tbody>
                          {(selRow.members ?? []).map((m) => (
                            <tr key={m.memberId}>
                              <td className="mono text-[11.5px] text-slate-200">{m.clientId || "—"}</td>
                              <td className="mono text-[11.5px] text-slate-400">{m.clientHost || "—"}</td>
                              <td className="mono max-w-[220px] truncate text-[11px] text-slate-500" title={m.memberId}>{m.memberId}</td>
                              <td className="mono text-[11.5px] text-slate-300">
                                {(m.assignment ?? []).length === 0 ? <span className="text-slate-600">—</span>
                                  : (m.assignment ?? []).map((a) => (
                                    <div key={a.topic}>{a.topic}: P[{a.partitions.join(",")}]</div>
                                  ))}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                <div className="mt-2.5">
                  <div className="card-t">
                    <span className="card-title !mb-0">分区 LAG</span>
                    {lag && (
                      <span className={`badge ml-auto ${lag.totalLag > 0 ? "warn" : "ok"}`}>total {lag.totalLag}</span>
                    )}
                  </div>
                  {!lag && <div className="py-3 text-center text-[11.5px] text-slate-500">LAG 查询中…</div>}
                  {lag && lag.rows.length === 0 && (
                    <div className="py-3 text-center text-[11.5px] text-slate-500">该组没有提交记录，无 LAG 可算。</div>
                  )}
                  {lag && lag.rows.length > 0 && (
                    <>
                      <div className="tbl-wrap">
                        <table className="tbl mono">
                          <thead><tr><th>Topic</th><th>P</th><th>Leader</th><th>End</th><th>Committed</th><th>Lag</th></tr></thead>
                          <tbody>
                            {lag.rows.map((r) => (
                              <tr key={`${r.topic}-${r.partition}`}>
                                <td className="max-w-[160px] truncate text-slate-200">{r.topic}</td>
                                <td>{r.partition}</td>
                                <td>{r.leader}</td>
                                <td>{r.end ?? "—"}</td>
                                <td>{r.committed ?? <span className="text-slate-600">无提交</span>}</td>
                                <td>{r.lag == null ? <span className="badge mute">N/A</span>
                                  : r.lag > 0 ? <span className="badge warn">{r.lag}</span>
                                    : <span className="badge ok">0</span>}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      {lag.uncommitted > 0 && (
                        <div className="mt-1.5 text-[11px] text-[var(--text-faint)]">
                          {lag.uncommitted} 个分区无提交记录（N/A），不计入 total。
                        </div>
                      )}
                    </>
                  )}
                </div>
              </>
            )}
          </div>

        {/* 演示操作（只读模式隐藏） */}
        {!readonly && (
          <div className="card p-3.5">
            <div className="card-t"><span className="text-[13px] font-bold text-slate-100">演示操作</span></div>
            <div className="grid grid-cols-2 items-end gap-2 xl:grid-cols-[1fr_1fr_auto_auto]">
              <div><label className={labelCls}>Group ID</label>
                <input className={inputCls} value={form.groupId}
                  onChange={(e) => setForm({ ...form, groupId: e.target.value })} /></div>
              <div><label className={labelCls}>Topics</label>
                <input className={inputCls} value={form.topics} placeholder="逗号分隔"
                  onChange={(e) => setForm({ ...form, topics: e.target.value })} /></div>
              <button onClick={() => requireLab(() => { void joinDemo(); })} disabled={loading} className="btn btn-ghost">
                {busy === "join" ? "入组中…" : "Join → 心跳 → 离组"}
              </button>
              <button onClick={() => requireLab(() => { void rebalanceDemo(); })} disabled={loading} className="btn btn-ghost">
                {busy === "rebalance" ? "演示中…" : "Rebalance 演示"}
              </button>
            </div>
            <div className="mt-1.5 text-[11px] text-slate-600" title="走 JoinGroup / SyncGroup / Heartbeat / LeaveGroup 协议，结束自动离组，位点保留">
              结束自动离组，位点保留，LAG 照常可查。
            </div>
          </div>
        )}

        {!readonly && (
          <div className="layout-grid layout-2eq">
          <div className="card p-3.5">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-100">入组结果</span>
              {joinOut && <span className="badge ok ml-auto">gen {String(joinOut["generation"])}</span>}
            </div>
            {!joinOut && <div className="py-4 text-center text-[12px] text-slate-500">运行一次入组演示后显示结果。</div>}
            {joinOut && (
              <div className="grid grid-cols-2 gap-2">
                {[["Generation", joinOut["generation"]], ["Leader", joinOut["isLeader"]],
                  ["Sync err", joinOut["syncError"] ?? 0], ["Heartbeat err", joinOut["heartbeatError"]]].map(([k, v]) => (
                  <div key={String(k)} className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
                    <div className="card-title">{String(k)}</div>
                    <div className="mono mt-0.5 text-[12px] text-slate-100">{String(v)}</div>
                  </div>
                ))}
                <div className="col-span-2 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-2.5 py-2">
                  <div className="card-title">Member ID</div>
                  <div className="mono mt-0.5 break-all text-[11px] text-slate-300">{String(joinOut["memberId"])}</div>
                </div>
              </div>
            )}
          </div>

          <div className="card p-3.5">
            <div className="card-t">
              <span className="text-[13px] font-bold text-slate-100">Rebalance 结果</span>
              {rebalOut && (rebalOut.rebalanced
                ? <span className="badge ok ml-auto">✓ 已发生</span>
                : <span className="badge warn ml-auto">未观察到递增</span>)}
            </div>
            {!rebalOut && <div className="py-4 text-center text-[12px] text-slate-500">m1 首入 → m2 加入 → m1 被 fencing 重入，generation 递增即证明 Rebalance。</div>}
            {rebalOut && (
              <>
                <div className="mono flex flex-wrap items-center gap-1.5 text-[12px]">
                  <span className="badge run">gen {String(rebalOut.generations.m1First)}</span>
                  <span className="text-slate-600">→</span>
                  <span className="badge run">gen {String(rebalOut.generations.m2Join)}</span>
                  <span className="text-slate-600">→</span>
                  <span className="badge run">gen {String(rebalOut.generations.m1Rejoin)}</span>
                </div>
                <div className="mono mt-1.5 text-[11.5px] leading-relaxed text-[var(--text-faint)]">{rebalOut.note}</div>
                <div className="scroll-box mt-2 max-h-[300px] space-y-1 overflow-y-auto overscroll-contain pr-1">
                  {rebalOut.steps.map((s, i) => (
                    <div key={i} className="codebox !p-1.5 !text-[10.5px]">
                      <span className="text-sky-300">{s.step}</span>
                      {s.generation != null && <span className="text-slate-500"> gen={s.generation}</span>}
                      {s.errorCode != null && s.errorCode !== 0 && <span className="text-rose-300"> ERR={s.errorCode} {s.errorName}</span>}
                      {s.note && <span className="text-amber-200/80"> · {s.note}</span>}
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
          </div>
        )}
      </div>
    </div>
  );
}
