import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { apiFetch } from "../api/http";
import { PageHeader } from "../components/bits";

const API = (import.meta.env.VITE_API_BASE as string) || window.location.origin;
type Operation = { id: string; cluster: string; topic: string; actor: string; status: string;
  attempts: number; error?: string; created: number; updated: number;
  events: { at: number; type: string; message: string }[] };
const labels: Record<string, string> = { queued: "等待执行", running: "正在执行", retrying: "等待重试",
  verifying: "正在核对", done: "已完成", failed: "失败" };
const time = (seconds: number) => new Date(seconds * 1000).toLocaleString();

export function OperationsPage() {
  const [params, setParams] = useSearchParams();
  const selectedId = params.get("task") || "";
  const [rows, setRows] = useState<Operation[]>([]);
  const [selectedTask, setSelectedTask] = useState<Operation | null>(null);
  const [error, setError] = useState("");
  const load = async () => {
    try {
      const response = await apiFetch(API + "/api/operations");
      if (!response.ok) throw new Error(await response.text());
      const result = await response.json() as { operations: Operation[] };
      setRows(result.operations);
      if (selectedId && !result.operations.some((row) => row.id === selectedId)) {
        const detail = await apiFetch(API + "/api/operations/" + selectedId);
        setSelectedTask(detail.ok ? await detail.json() as Operation : null);
      } else {
        setSelectedTask(null);
      }
      setError("");
    } catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)); }
  };
  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 3000);
    return () => window.clearInterval(timer);
  }, []);
  const selected = rows.find((row) => row.id === selectedId) || selectedTask || rows[0];
  return <div className="space-y-4">
    <PageHeader title="后台操作" desc="可查看 Topic 删除的执行与最终核对结果" />
    {error && <div className="alert err">{error}</div>}
    <div className="grid gap-4 lg:grid-cols-[minmax(280px,0.9fr)_minmax(420px,1.5fr)]">
      <section className="card p-4">
        <div className="flex items-center justify-between mb-3"><h2 className="text-[15px] font-semibold">任务列表</h2>
          <button className="btn btn-ghost btn-sm" onClick={() => void load()}>刷新</button></div>
        <div className="space-y-2 max-h-[65vh] overflow-auto">
          {rows.length === 0 && <p className="ui-note">暂无后台操作</p>}
          {rows.map((row) => <button key={row.id} type="button" onClick={() => setParams({ task: row.id })}
            className="w-full text-left rounded-xl border border-slate-200 bg-white/70 px-3 py-2 hover:border-blue-300"
            style={selected?.id === row.id ? { borderColor: "#6aa8e8" } : undefined}>
            <div className="flex justify-between gap-3"><strong className="text-[13px] truncate">{row.topic}</strong>
              <span className="text-[12px]">{labels[row.status] || row.status}</span></div>
            <div className="ui-note">{row.cluster} · {time(row.created)}</div>
          </button>)}
        </div>
      </section>
      <section className="card p-4 min-h-[340px]">
        {selected ? <>
          <div className="flex items-center justify-between gap-3 mb-3"><div><h2 className="text-[15px] font-semibold">{selected.topic}</h2>
            <p className="ui-note">{selected.id} · {selected.cluster} · {selected.actor}</p></div>
            <span className="status-badge">{labels[selected.status] || selected.status}</span></div>
          <div className="flex gap-4 text-[13px] mb-3"><span>尝试 {selected.attempts} 次</span><span>更新于 {time(selected.updated)}</span></div>
          {selected.error && <div className="alert err mb-3">{selected.error}</div>}
          <div className="max-h-[55vh] overflow-auto space-y-2">
            {selected.events.map((event, index) => <div key={index} className="rounded-lg bg-slate-50 border border-slate-200 px-3 py-2 text-[12px]">
              <span className="text-slate-500 mr-3">{time(event.at)}</span>
              <span className="font-semibold mr-3">{labels[event.type] || event.type}</span>
              <span>{event.message}</span>
            </div>)}
          </div>
        </> : <p className="ui-note">选择任务查看执行详情</p>}
      </section>
    </div>
  </div>;
}
