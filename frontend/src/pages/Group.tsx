import { useCallback, useEffect, useRef, useState } from "react";
import { useConn } from "../conn";
import { GroupStateBadge, PageHeader } from "../components/bits";

const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;

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
interface LagDetail {
  totalLag: number; uncommitted: number; rows: LagRow[];
}

export function GroupPage() {
  const { conn } = useConn();
  const [rows, setRows] = useState<GroupRow[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [lag, setLag] = useState<LagDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [lagLoading, setLagLoading] = useState(false);
  const [error, setError] = useState("");
  const [lagError, setLagError] = useState("");
  const [partialBrokers, setPartialBrokers] = useState(0);
  const [auto, setAuto] = useState(false);
  const [filter, setFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [sortBy, setSortBy] = useState("lag");
  const [fetchedAt, setFetchedAt] = useState<number | null>(null);
  const selectedRef = useRef("");
  const overviewSeq = useRef(0);
  const lagSeq = useRef(0);

  const loadLag = useCallback(async (groupId: string, topics: string[], silent = false) => {
    const seq = ++lagSeq.current;
    if (!topics.length) {
      setLag(null);
      setLagError("");
      setLagLoading(false);
      return;
    }
    if (!silent) setLagLoading(true);
    try {
      const query = encodeURIComponent(topics.join(","));
      const response = await fetch(`${API}/api/clusters/${conn.id}/groups/${encodeURIComponent(groupId)}/lag?topics=${query}`);
      if (!response.ok) throw new Error(`Lag 查询失败 (${response.status})`);
      const detail = await response.json() as LagDetail;
      if (seq !== lagSeq.current || selectedRef.current !== groupId) return;
      setLag(detail);
      setLagError("");
    } catch (cause) {
      if (seq !== lagSeq.current || selectedRef.current !== groupId) return;
      setLag(null);
      setLagError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      if (seq === lagSeq.current) setLagLoading(false);
    }
  }, [conn.id]);

  const loadOverview = useCallback(async (silent = false) => {
    const seq = ++overviewSeq.current;
    if (!silent) setLoading(true);
    try {
      const response = await fetch(`${API}/api/clusters/${conn.id}/groups/overview`);
      if (!response.ok) throw new Error(`消费组列表获取失败 (${response.status})`);
      const data = await response.json() as { groups?: GroupRow[]; listGroupsErrors?: string[] };
      if (seq !== overviewSeq.current) return;
      const nextRows = data.groups ?? [];
      setRows(nextRows);
      setPartialBrokers(data.listGroupsErrors?.length ?? 0);
      setFetchedAt(Date.now());
      setError("");
      const selected = nextRows.find((row) => row.groupId === selectedRef.current) ?? nextRows[0];
      if (!selected) {
        selectedRef.current = "";
        setSelectedId("");
        setLag(null);
        setLagError("");
        setLagLoading(false);
        ++lagSeq.current;
      } else {
        selectedRef.current = selected.groupId;
        setSelectedId(selected.groupId);
        if (selected.error) {
          setLag(null);
          setLagError("");
          setLagLoading(false);
          ++lagSeq.current;
        } else {
          await loadLag(selected.groupId, selected.topics ?? [], silent);
        }
      }
    } catch (cause) {
      if (seq === overviewSeq.current) setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      if (seq === overviewSeq.current && !silent) setLoading(false);
    }
  }, [conn.id, loadLag]);

  useEffect(() => {
    selectedRef.current = "";
    setRows([]);
    setSelectedId("");
    setLag(null);
    setLagError("");
    setPartialBrokers(0);
    setFetchedAt(null);
    void loadOverview();
    return () => { ++overviewSeq.current; ++lagSeq.current; };
  }, [loadOverview]);

  useEffect(() => {
    if (!auto) return;
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void loadOverview(true);
    }, 8000);
    return () => window.clearInterval(timer);
  }, [auto, loadOverview]);

  const pick = (row: GroupRow) => {
    selectedRef.current = row.groupId;
    setSelectedId(row.groupId);
    setLag(null);
    setLagError("");
    if (row.error) {
      ++lagSeq.current;
      setLagLoading(false);
    } else {
      void loadLag(row.groupId, row.topics ?? []);
    }
  };
  const search = filter.trim().toLowerCase();
  const shown = rows.filter((row) => {
    const matchesText = !search || row.groupId.toLowerCase().includes(search) ||
      (row.topics ?? []).some((topic) => topic.toLowerCase().includes(search));
    const matchesStatus = statusFilter === "all" ||
      (statusFilter === "error" ? Boolean(row.error) :
        statusFilter === "active" ? !row.error && (row.memberCount ?? 0) > 0 :
          !row.error && (row.memberCount ?? 0) === 0);
    return matchesText && matchesStatus;
  }).sort((a, b) => sortBy === "lag"
    ? (b.totalLag ?? -1) - (a.totalLag ?? -1) || a.groupId.localeCompare(b.groupId)
    : a.groupId.localeCompare(b.groupId));
  const selected = rows.find((row) => row.groupId === selectedId) ?? null;
  const activeCount = rows.filter((row) => !row.error && (row.memberCount ?? 0) > 0).length;
  const knownLag = rows.reduce((sum, row) => sum + (row.error ? 0 : row.totalLag ?? 0), 0);
  const fmt = (value: number) => value.toLocaleString("zh-CN");

  return <div className="group-page">
    <PageHeader title="消费组" right={<>
      {loading && <span className="badge run pulsing">拉取中…</span>}
      {fetchedAt && <span className="badge mute mono">{new Date(fetchedAt).toLocaleTimeString("zh-CN", { hour12: false })}</span>}
      <button type="button" onClick={() => setAuto((current) => !current)}
        aria-pressed={auto} className={`btn btn-sm ${auto ? "btn-warn" : "btn-ghost"}`}>
        {auto ? "自动刷新 · 8s" : "自动刷新关"}
      </button>
      <button type="button" onClick={() => void loadOverview()} disabled={loading} className="btn btn-ghost btn-sm">刷新</button>
    </>} />
    {error && <div className="alert err mb-3" role="alert">{error}</div>}
    {partialBrokers > 0 && <div className="alert warn mb-3" role="status">{partialBrokers} 个 Broker 的消费组列表未采集到，当前结果可能不完整。</div>}

    <div className="group-layout">
      <section className="card group-list-card" aria-label="消费组列表">
        <div className="group-card-head">
          <h2>消费组一览</h2>
          <span className="badge mute">{rows.length} 组</span>
          <span className="badge ok">{activeCount} 活跃</span>
          <span className="badge warn">已知 Lag {fmt(knownLag)}</span>
        </div>
        <div className="group-filters">
          <input aria-label="搜索消费组或 Topic" placeholder="搜索消费组或 Topic" value={filter}
            onChange={(event) => setFilter(event.target.value)} className="field group-search" />
          <select aria-label="按状态筛选" className="field" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
            <option value="all">全部状态</option><option value="active">有成员</option><option value="empty">无成员</option><option value="error">查询失败</option>
          </select>
          <select aria-label="消费组排序" className="field" value={sortBy} onChange={(event) => setSortBy(event.target.value)}>
            <option value="lag">Lag 优先</option><option value="name">名称排序</option>
          </select>
        </div>
        <div className="group-list-scroll scroll-box">
          {rows.length === 0
            ? <div className="group-empty">{loading ? "正在获取消费组…" : "暂无消费组"}</div>
            : shown.length === 0
              ? <div className="group-empty">没有匹配的消费组</div>
              : <table className="tbl group-table">
                <thead><tr><th>消费组</th><th>状态</th><th>成员</th><th>Topic</th><th>Lag</th><th /></tr></thead>
                <tbody>{shown.map((row) => <tr key={row.groupId} tabIndex={0} role="button"
                  aria-selected={selectedId === row.groupId}
                  onClick={() => pick(row)}
                  onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); pick(row); } }}
                  className={selectedId === row.groupId ? "is-selected" : ""}>
                  <td className="mono group-name" title={row.groupId}>{row.groupId}</td>
                  <td>{row.error ? <span className="badge err">查询失败</span> : <GroupStateBadge state={row.state} />}</td>
                  <td className="mono">{row.memberCount ?? "—"}</td>
                  <td className="mono" title={(row.topics ?? []).join(", ")}>{row.topics?.length ?? 0}</td>
                  <td className="mono">{row.error || !row.scopedPartitions ? "N/A" : fmt(row.totalLag ?? 0)}</td>
                  <td className="group-row-action">查看 →</td>
                </tr>)}</tbody>
              </table>}
        </div>
      </section>

      <section className="card group-detail-card" aria-label="消费组明细">
        <div className="group-card-head">
          <h2>组明细</h2>
          {selected ? <span className="badge run mono" title={selected.groupId}>{selected.groupId}</span>
            : <span className="badge mute">未选择</span>}
        </div>
        <div className="group-detail-scroll scroll-box">
          {!selected ? <div className="group-empty">选择左侧消费组查看明细</div> : <>
            {selected.error && <div className="alert err" role="alert">组数据获取失败：{selected.error}</div>}
            <div className="group-facts">
              <div><small>状态</small><strong><GroupStateBadge state={selected.state} /></strong></div>
              <div><small>成员</small><strong className="mono">{selected.memberCount ?? 0}</strong></div>
              <div><small>已提交 Topic</small><strong className="mono">{selected.topics?.length ?? 0}</strong></div>
              <div><small>协调节点</small><strong className="mono" title={selected.coordinator ? `${selected.coordinator.host}:${selected.coordinator.port}` : "—"}>{selected.coordinator?.id != null ? `#${selected.coordinator.id}` : "—"}</strong></div>
            </div>
            <div className="group-detail-section">
              <div className="group-subhead"><h3>成员与分配</h3><span>{selected.protocol || selected.protocolType || "—"} · {selected.members?.length ?? 0} 成员</span></div>
              {selected.members?.length ? <div className="tbl-wrap group-inner-table">
                <table className="tbl"><thead><tr><th>消费者 ID</th><th>连接</th><th>Member ID</th><th>分配</th></tr></thead>
                  <tbody>{selected.members.map((member) => <tr key={member.memberId}>
                    <td className="mono">{member.clientId || "—"}</td>
                    <td className="mono">{member.clientHost || "—"}</td>
                    <td className="mono" title={member.memberId}>{member.memberId}</td>
                    <td className="mono">{member.assignment?.length
                      ? member.assignment.map((part) => `${part.topic}: P[${part.partitions.join(",")}]`).join(" · ") : "—"}</td>
                  </tr>)}</tbody>
                </table>
              </div> : <div className="group-inline-empty">当前没有活跃成员</div>}
            </div>
            <div className="group-detail-section">
              <div className="group-subhead"><h3>分区 Lag</h3>
                {lag && <span className={`badge ${lag.totalLag > 0 ? "warn" : "ok"}`}>合计 {fmt(lag.totalLag)}</span>}
                {lagLoading && <span className="badge mute">更新中…</span>}
              </div>
              {lagError && <div className="alert err" role="alert">{lagError}</div>}
              {!selected.topics?.length
                ? <div className="group-inline-empty">该组暂无已提交的 Topic，Lag 无法确定</div>
                : !lag
                  ? <div className="group-inline-empty">{lagLoading ? "正在查询分区 Lag…" : "暂无 Lag 数据"}</div>
                  : <><div className="tbl-wrap group-inner-table">
                    <table className="tbl mono"><thead><tr><th>Topic</th><th>分区</th><th>Leader</th><th>末端位点</th><th>已提交</th><th>Lag</th></tr></thead>
                      <tbody>{lag.rows.map((part) => <tr key={`${part.topic}-${part.partition}`}>
                        <td>{part.topic}</td><td>{part.partition}</td><td>{part.leader ?? "—"}</td>
                        <td>{part.end ?? "—"}</td><td>{part.committed ?? "无提交"}</td>
                        <td>{part.lag == null ? <span className="badge mute">N/A</span>
                          : part.lag > 0 ? <span className="badge warn">{fmt(part.lag)}</span>
                            : <span className="badge ok">0</span>}</td>
                      </tr>)}</tbody>
                    </table>
                  </div>{lag.uncommitted > 0 && <p className="group-lag-note">{lag.uncommitted} 个分区无提交位点，未计入合计。</p>}</>}
            </div>
          </>}
        </div>
      </section>
    </div>
  </div>;
}
