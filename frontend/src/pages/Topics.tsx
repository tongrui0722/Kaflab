import { useEffect, useState } from "react";
import { useConn } from "../conn";
import { useAuth, authHeaders } from "../auth";
import { PageHeader } from "../components/bits";

const API = (import.meta.env.VITE_API_BASE as string) || "http://localhost:8000";

interface TopicRow {
  topic: string; partitions: number; replication: number;
  leaders: number[]; hasOffline: boolean;
}

export function TopicsPage() {
  const { conn } = useConn();
  const { mode } = useAuth();
  const readonly = mode === "readonly";
  const [rows, setRows] = useState<TopicRow[]>([]);
  const [brokerCount, setBrokerCount] = useState(3);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [showCreate, setShowCreate] = useState(false);
  const [cName, setCName] = useState("");
  const [cParts, setCParts] = useState(3);
  const [cRf, setCRf] = useState(1);
  const [cConfigs, setCConfigs] = useState("");
  const [cBusy, setCBusy] = useState(false);
  const [cErr, setCErr] = useState<string | null>(null);
  const [cSuccess, setCSuccess] = useState<string | null>(null);

  const load = async (cid: string): Promise<TopicRow[] | null> => {
    setLoading(true);
    setErr(null);
    try {
      const r = await fetch(`${API}/api/clusters/${cid}/topics`);
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

  useEffect(() => { load(conn.id); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [conn.id]);

  const createTopic = async () => {
    if (cBusy) return;
    const name = cName.trim();
    if (!name) { setCErr("请输入 Topic 名称"); return; }
    if (!Number.isInteger(cParts) || cParts < 1) { setCErr("分区数必须为 ≥1 的整数"); return; }
    if (!Number.isInteger(cRf) || cRf < 1) { setCErr("副本数必须为 ≥1 的整数"); return; }
    // Kafbat TopicCreationDTO 对等字段：name / partitions / replicationFactor / configs
    let configs: Record<string, string> = {};
    if (cConfigs.trim()) {
      try {
        const parsed = JSON.parse(cConfigs) as unknown;
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("bad");
        if (Object.entries(parsed as Record<string, unknown>).some(([k, v]) => !k.trim() || typeof v !== "string")) {
          throw new Error("bad-config-value");
        }
        configs = parsed as Record<string, string>;
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
      // 请求超时等情况下，先刷新当前集群列表，避免把已创建成功误报为失败。
      const refreshed = await load(conn.id);
      if (refreshed?.some((row) => row.topic === name)) {
        setShowCreate(false);
        setCName("");
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

  const filtered = rows.filter((r) => r.topic.includes(q.trim()));
  return (
    <div>
      <PageHeader title="Topics" desc={`${conn.name} · 分区与 Leader 一览`} />
      <div className="card p-3">
        <div className="mb-2 flex items-center gap-2">
          <span className="card-title">Topics（{conn.name} · {rows.length}）</span>
          <input placeholder="过滤" value={q} onChange={(e) => setQ(e.target.value)}
            className="field mono ml-auto !h-[30px] !w-40 !text-[11.5px]" />
          {!readonly && (
            <button onClick={() => { setShowCreate((v) => !v); setCErr(null); setCSuccess(null); }}
              className="btn btn-primary btn-sm">新建</button>
          )}
          <button onClick={() => load(conn.id)}
            className="btn btn-ghost btn-sm">刷新</button>
        </div>
        {cSuccess && <div className="alert ok mb-2">{cSuccess}</div>}
        {!readonly && showCreate && (
          <div className="mb-2 grid grid-cols-2 items-end gap-2 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-2.5 xl:grid-cols-[1fr_110px_110px_auto]">
            <div><label className="lbl">Topic 名称</label>
              <input className="field mono" value={cName} placeholder="如 order-events"
                onChange={(e) => setCName(e.target.value)} /></div>
            <div><label className="lbl">分区数</label>
              <input type="number" min={1} max={1000} className="field mono" value={cParts}
                onChange={(e) => setCParts(Math.min(1000, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
            <div><label className="lbl">副本数（≤{brokerCount} 台 Broker）</label>
              <input type="number" min={1} max={brokerCount} className="field mono" value={cRf}
                onChange={(e) => setCRf(Math.min(brokerCount, Math.max(1, Math.floor(Number(e.target.value) || 1))))} /></div>
            <button onClick={createTopic} disabled={cBusy} className="btn btn-send btn-sm !w-auto">
              {cBusy ? "创建中…" : "创建"}
            </button>
            <div className="col-span-full"><label className="lbl">Topic 配置（可选 JSON，如 retention.ms / cleanup.policy）</label>
              <input className="field mono" value={cConfigs} placeholder='{"retention.ms": "604800000", "cleanup.policy": "delete"}'
                onChange={(e) => setCConfigs(e.target.value)} /></div>
            {cErr && <div className="alert err col-span-full">{cErr}</div>}
          </div>
        )}
        {loading && <div className="py-6 text-center text-[12px] text-slate-500">Metadata 拉取中…</div>}
        {err && <div className="alert err">{err}</div>}
        {!loading && !err && (
          <div className="tbl-wrap max-h-[640px] !border-0">
            <table className="tbl mono">
              <thead className="sticky top-0 bg-[var(--panel-2)]"><tr>
                <th>Topic</th>
                <th className="!text-center">Partitions</th>
                <th className="!text-center">RF</th>
                <th>Leaders</th>
                <th className="!text-center">状态</th>
                <th><span className="sr-only">操作</span></th>
              </tr></thead>
              <tbody className="tabular-nums">
                {filtered.map((r) => (
                  <tr key={r.topic}
                    onClick={() => { window.location.hash = `#/topics/${encodeURIComponent(r.topic)}`; }}
                    className="cursor-pointer transition-colors hover:bg-white/[0.025]">
                    <td className="font-semibold text-slate-100">{r.topic}</td>
                    <td className="!text-center text-slate-300">{r.partitions}</td>
                    <td className="!text-center text-slate-300">{r.replication}</td>
                    <td className="text-slate-400">{r.leaders.join(", ")}</td>
                    <td className="!text-center">{r.hasOffline ? <span className="badge err">OFFLINE</span> : <span className="badge ok">OK</span>}</td>
                    <td className="!text-right"><span className="text-[11px] text-sky-300/70">详情 →</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
