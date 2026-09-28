import type { KafkaTraceEvent } from "../types";

type Part = { partition: number; leader: number; replicas: number[]; isr: number[] };

/** 消费组内详情：Producer 没有 consumer group，此处展示本次发送真实的分区分配（Metadata）。 */
export function GroupDetailTab({ events, topic, partitions, activePartition }: {
  events: KafkaTraceEvent[]; topic: string; partitions: Part[]; activePartition?: number;
}) {
  const metaEv = events.find((e) => e.step === "metadata.updated");
  const metaParts = ((metaEv?.response ?? {}) as Record<string, unknown>)["partitions"];
  const rows: Part[] = (Array.isArray(metaParts) && metaParts.length > 0
    ? (metaParts as unknown as Part[]) : partitions).filter((p) => p != null);
  const groupEvs = events.filter((e) =>
    ["findcoordinator", "joingroup", "syncgroup", "offsetfetch"].includes(e.step));
  if (events.length === 0) {
    return <div className="py-8 text-center text-[12px] text-slate-500">发送后显示真实分区分配（来自 Metadata）。</div>;
  }
  return (
    <div className="space-y-2">
      <div className="text-[11.5px] leading-relaxed text-slate-500">
        Producer 不加入 consumer group，无 Coordinator / Generation / Member。
        此处展示本次发送命中的 <span className="mono text-slate-300">Topic → Partition → Leader</span> 真实分配。
      </div>
      <div className="card-title">Topic: {topic}</div>
      {rows.length === 0 ? (
        <div className="py-3 text-center text-[11.5px] text-slate-600">Metadata 暂无分区数据（N/A）。</div>
      ) : (
        <div className="tbl-wrap">
        <table className="tbl mono">
          <thead><tr>
            <th>Partition</th>
            <th>Leader</th>
            <th>Replicas</th>
            <th>ISR</th>
            <th>状态</th>
          </tr></thead>
          <tbody>
            {rows.map((p) => (
              <tr key={p.partition} className={p.partition === activePartition ? "!bg-[rgba(56,189,248,0.09)]" : ""}>
                <td>P{p.partition}</td>
                <td>Broker {p.leader}</td>
                <td>{(p.replicas ?? []).join(",")}</td>
                <td>{(p.isr ?? []).join(",")}</td>
                <td>{p.partition === activePartition ? <span className="badge run">ACTIVE</span> : <span className="text-slate-600">—</span>}</td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      )}
      {groupEvs.length > 0 && (
        <div>
          <div className="card-title mb-1 mt-2">组协调事件（本次 Trace 携带）</div>
          {groupEvs.map((e, i) => (
            <div key={i} className="codebox !p-1.5 !text-[11px]">
              {e.step} · {e.status}{e.broker ? ` · ${e.broker.host}:${e.broker.port}` : ""}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/** 协调解析：Bootstrap → Metadata → Leader 的决策路径（真实事件驱动）。 */
export function CoordTab({ events }: { events: KafkaTraceEvent[] }) {
  if (events.length === 0) {
    return <div className="py-8 text-center text-[12px] text-slate-500">发送后显示真实的 Leader 发现路径。</div>;
  }
  const start = events.find((e) => e.step === "session.start");
  const bootstrap = ((start?.request ?? {}) as Record<string, unknown>)["bootstrap"];
  const sel = events.find((e) => e.step === "partition.selected");
  const selResp = ((sel?.response ?? {}) as Record<string, unknown>);
  const conn = events.find((e) => e.step === "tcp.connect" && (e.stepIndex === 5));
  const coord = events.find((e) => e.step === "findcoordinator");
  const rows: [string, string][] = [
    ["Bootstrap（用户配置）", String(bootstrap ?? "N/A")],
    ["Metadata 来源", sel?.broker ? `${sel.broker.host}:${sel.broker.port}` : (metaHost(events) ?? "N/A")],
    ["选中 Partition", selResp["partition"] != null ? `P${String(selResp["partition"])}` : "N/A"],
    ["Leader Broker", selResp["leader"] != null ? `Broker ${String(selResp["leader"])}` : "N/A"],
    ["实际连接（Produce 目标）", conn?.broker ? `${conn.broker.host}:${conn.broker.port}` : "N/A"],
  ];
  return (
    <div className="space-y-2">
      <div className="text-[11.5px] leading-relaxed text-slate-500">
        回答“为什么最终连到这个 Broker”：Bootstrap 只用于首连和 Metadata，
        真正的 Produce 目标由 Metadata 返回的 Partition Leader 决定。
      </div>
      <div className="divide-y divide-white/5">
        {rows.map(([k, v]) => (
          <div key={k} className="flex items-center justify-between py-1.5 text-[12px]">
            <span className="text-slate-500">{k}</span>
            <span className="mono text-slate-200">{v}</span>
          </div>
        ))}
      </div>
      {coord ? (
        <div className="rounded-lg bg-black/25 p-2">
          <div className="card-title mb-1">Coordinator（真实 FindCoordinator）</div>
          <div className="mono text-[11px] text-slate-300">
            {JSON.stringify(coord.response ?? {})}
          </div>
        </div>
      ) : (
        <div className="text-[11.5px] text-slate-600">
          本次为 Producer 链路，无 FindCoordinator（消费组协调只出现在 Consumer / Group 页）。
        </div>
      )}
    </div>
  );
}

function metaHost(events: KafkaTraceEvent[]): string | null {
  const m = events.find((e) => e.step === "metadata" && e.broker);
  return m?.broker ? `${m.broker.host}:${m.broker.port}` : null;
}
