import { useEffect, useState } from "react";
import { useConn } from "../conn";
import { useAuth } from "../auth";
import { apiFetch } from "../api/http";
import { PageHeader } from "../components/bits";

const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;
type Point = { ts: number; broker_online: number; broker_offline: number; topic_total: number;
  topic_offline: number; topic_under_replicated: number };
type Data = { latest: Point | null; points: Point[]; collector: {last_ok: number | null; last_error: string | null} | null;
  lag: {running: boolean; last_ok: number | null; last_error: string | null; total: number | null;
    state: string; points: {ts: number; value: number; series: string}[]};
  rules: {broker_offline: number; topic_offline: number; topic_under_replicated: number; lag_threshold: number | null};
  alerts: {type: string; message: string; value: number}[]; healthState: string;
  events: {ts: number; kind: string; message: string}[]; serverTime: number };
const time = (ts?: number | null) => ts ? new Date(ts * 1000).toLocaleString() : "暂无";
const stateLabel: Record<string, string> = {healthy: "正常", abnormal: "存在异常",
  collection_failed: "采集失败", stale: "数据过期", partial: "部分采集失败", disabled: "未启用"};
const eventLabel: Record<string, string> = {
  brokerOffline_alert: "Broker 离线", brokerOffline_recovered: "Broker 恢复",
  topicOffline_alert: "Topic 离线", topicOffline_recovered: "Topic 恢复",
  topicUnderReplicated_alert: "副本不足", topicUnderReplicated_recovered: "副本恢复",
  collection_failed: "健康采集失败", collection_recovered: "健康采集恢复",
  lag_alert: "Lag 超阈值", lag_recovered: "Lag 恢复",
  lag_collection_failed: "Lag 采集失败", lag_collection_recovered: "Lag 采集恢复",
};
function Spark({ points, field }: {points: Point[]; field: keyof Point}) {
  const recent = points.slice(-40);
  if (recent.length < 2) return <div className="ui-note">暂无趋势数据</div>;
  const values = recent.map((p) => Number(p[field] || 0));
  const max = Math.max(1, ...values);
  const d = values.map((value, index) => ((index / (values.length - 1)) * 100).toFixed(1)
    + "," + (38 - value / max * 34).toFixed(1)).join(" ");
  return <svg viewBox="0 0 100 42" preserveAspectRatio="none" className="w-full h-20" role="img" aria-label="最近采样趋势">
    <polyline points={d} fill="none" stroke="#4d91de" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
  </svg>;
}
export function MonitoringPage() {
  const { conn } = useConn();
  const { mode } = useAuth();
  const [data, setData] = useState<Data | null>(null);
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  const [rules, setRules] = useState<Data["rules"] | null>(null);
  const load = async () => {
    try {
      const r = await apiFetch(API + "/api/clusters/" + conn.id + "/monitoring");
      if (!r.ok) throw new Error(await r.text());
      const next = await r.json() as Data;
      setData(next); setRules((current) => current || next.rules); setError("");
    } catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)); }
  };
  useEffect(() => {
    setRules(null); void load();
    const timer = window.setInterval(() => void load(), 30000);
    return () => window.clearInterval(timer);
  }, [conn.id]);
  const save = async () => {
    if (!rules) return;
    setSaving(true);
    try {
      const r = await apiFetch(API + "/api/clusters/" + conn.id + "/monitoring/rules", {
        method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(rules) });
      if (!r.ok) throw new Error(await r.text());
      await load();
    } catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)); }
    finally { setSaving(false); }
  };
  const status = data?.healthState || "stale";
  return <div className="space-y-4">
    <PageHeader title="监控与告警" desc={conn.name} right={<button className="btn btn-ghost btn-sm" onClick={() => void load()}>刷新</button>} />
    {error && <div className="alert err">{error}</div>}
    <div className="grid gap-4 lg:grid-cols-3">
      <section className="card p-4"><h2 className="font-semibold text-[15px]">采集状态</h2>
        <div className="mt-3 text-[18px] font-semibold">{stateLabel[status] || status}</div>
        <div className="ui-note mt-2">健康采集：{time(data?.collector?.last_ok)}</div>
        <div className="ui-note">Lag 采集：{time(data?.lag.last_ok)} · {stateLabel[data?.lag.state || "stale"] || data?.lag.state}</div>
        {data?.collector?.last_error && <p className="text-red-600 text-[12px] mt-2">{data.collector.last_error}</p>}
        {data?.lag.last_error && <p className="text-amber-700 text-[12px] mt-2">Lag：{data.lag.last_error}</p>}
      </section>
      <section className="card p-4"><h2 className="font-semibold text-[15px]">Broker 状态</h2>
        <div className="mt-3 text-[18px] font-semibold">{data?.latest?.broker_online ?? "—"} 在线 · {data?.latest?.broker_offline ?? "—"} 离线</div>
        <Spark points={data?.points || []} field="broker_offline" /></section>
      <section className="card p-4"><h2 className="font-semibold text-[15px]">Topic 状态</h2>
        <div className="mt-3 text-[18px] font-semibold">{data?.latest?.topic_total ?? "—"} 总数</div>
        <p className="ui-note">离线 {data?.latest?.topic_offline ?? "—"} · 副本不足 {data?.latest?.topic_under_replicated ?? "—"}</p>
        <Spark points={data?.points || []} field="topic_under_replicated" /></section>
    </div>
    <div className="grid gap-4 lg:grid-cols-2">
      <section className="card p-4"><h2 className="font-semibold text-[15px]">当前告警</h2>
        {data?.alerts.length ? data.alerts.map((alert, i) => <p key={i} className="alert err mt-2">{alert.message}：{alert.value}</p>)
          : <p className="ui-note mt-3">{status === "collection_failed" ? "本次采集失败，无法判断当前健康状态" : status === "stale" ? "没有近期采样，无法判断当前健康状态" : "没有触发的规则"}</p>}
        <h3 className="font-semibold text-[13px] mt-5">最近事件</h3>
        <div className="max-h-44 overflow-auto mt-2">{data?.events.map((event, i) =>
          <p key={i} className="text-[12px] py-1 border-b border-slate-200">{time(event.ts)} · {eventLabel[event.kind] || event.kind} · {event.message}</p>)}</div>
      </section>
      <section className="card p-4"><h2 className="font-semibold text-[15px]">告警规则</h2>
        {rules && <div className="grid grid-cols-2 gap-3 mt-3">
          {(["broker_offline", "topic_offline", "topic_under_replicated", "lag_threshold"] as const).map((key) =>
            <label key={key} className="lbl">{({broker_offline: "离线 Broker", topic_offline: "离线 Topic",
              topic_under_replicated: "副本不足 Topic", lag_threshold: "消费组 Lag"} as const)[key]}
              <input className="field mt-1" type="number" min={key === "lag_threshold" ? 0 : 1}
                disabled={mode === "readonly"} value={rules[key] ?? ""}
                onChange={(e) => setRules({...rules, [key]: e.target.value === "" ? null : Number(e.target.value)})} /></label>)}
        </div>}
        <button className="btn btn-primary btn-sm mt-4" disabled={mode === "readonly" || saving} onClick={() => void save()}>保存规则</button>
      </section>
    </div>
  </div>;
}
