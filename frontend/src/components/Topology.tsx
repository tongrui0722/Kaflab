import type { KafkaTraceEvent } from "../types";

export interface TopoPart { p: number; leader?: number; }

const node = "flex-1 rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-2";

export function TopologyGraph({ topic = "test-topic", partitions = [], activePartition = 0, leader }:
  { topic?: string; partitions?: TopoPart[]; activePartition?: number; leader?: number }) {
  const brokers = [...new Set(partitions.map((x) => x.leader).filter((x) => x != null))] as number[];
  return (
    <div>
      <div className="card-title mb-2">Kafka 拓扑图{partitions.length ? "" : "（连接集群后由真实 Metadata 驱动）"}</div>
      <div className="flex items-stretch gap-1 overflow-x-auto text-center text-[10.5px]">
        <div className={node}>
          <div className="text-sky-300">⛁</div><div className="font-semibold text-slate-200">Producer</div>
          <div className="mono text-[var(--text-faint)]">kafka-sim-01</div>
        </div>
        <div className="flex items-center text-slate-600">→</div>
        <div className={node}>
          <div className="text-violet-300">❖</div><div className="font-semibold text-slate-200">Metadata</div>
        </div>
        <div className="flex items-center text-slate-600">→</div>
        <div className={`${node} !border-violet-400/30 !bg-[rgba(167,139,250,0.07)]`}>
          <div className="font-semibold text-violet-200">{topic}</div>
          <div className="mt-1 space-y-0.5">
            {partitions.length === 0 && <div className="mono text-slate-500">—</div>}
            {partitions.map((x) => {
              const active = x.p === activePartition;
              return (
                <div key={x.p} className={`mono rounded px-1 py-[1px] ${active ? "bg-[rgba(56,189,248,0.2)] text-sky-100 shadow-[inset_0_0_0_1px_rgba(56,189,248,0.5)]" : "text-slate-400"}`}>
                  P{x.p} → {x.leader ?? "?"} {active && "●"}
                </div>
              );
            })}
          </div>
        </div>
        <div className="flex items-center text-slate-600">→</div>
        <div className={node}>
          <div className="font-semibold text-slate-200">{leader != null ? `Broker ${leader}` : "Leader"}</div>
          <div className="badge ok mt-1">Produce</div>
        </div>
        <div className="flex items-center text-slate-600">→</div>
        <div className={node}>
          <div className="font-semibold text-slate-200">Kafka Cluster</div>
          <div className="mono mt-1 flex flex-wrap justify-center gap-1 text-slate-400">
            {brokers.length === 0 && <span className="text-slate-600">—</span>}
            {brokers.map((b) => (
              <span key={b} className={`badge ${b === leader ? "run" : "mute"}`}>{b}</span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export function FullTimeline({ events }: { events: KafkaTraceEvent[] }) {
  if (events.length === 0) {
    return (
      <div>
        <div className="mb-2"><span className="card-title">全链时间线</span></div>
        <div className="py-2 text-[11px] text-slate-600">发送后按真实 timestamp 生成。</div>
      </div>
    );
  }
  const t0 = events[0].timestamp;
  const total = Math.max(events[events.length - 1].timestamp - t0, 1);
  const items = events.slice(0, 14).map((e) => {
    const rel = e.timestamp - t0;
    const a = e.apiName === "LOCAL" ? e.step : `${e.apiName} ${e.direction === "request" ? "REQ" : e.direction === "response" ? "RES" : ""}`;
    const b = e.broker ? `${e.broker.host}:${e.broker.port}`
      : e.topic ? `${e.topic}${e.partition != null ? ` P${e.partition}` : ""}` : e.step;
    return { rel, a: a.trim(), b, lat: e.latencyMs } as const;
  });
  return (
    <div>
      <div className="mb-2 flex items-center justify-between">
        <span className="card-title">全链时间线（相对ms · 总{total}ms）</span>
      </div>
      <div className="grid grid-cols-4 gap-1.5 text-[10px] sm:grid-cols-5 lg:grid-cols-7">
        {items.map(({ rel, a, b, lat }, i) => (
          <div key={`${rel}-${a}-${i}`} className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] p-1.5">
            <div className="mono font-semibold text-sky-300">+{rel}ms{lat != null ? ` · ${lat}ms` : ""}</div>
            <div className="mt-1 h-1 overflow-hidden rounded bg-white/[0.06]">
              <div className="h-full rounded bg-gradient-to-r from-sky-500 to-cyan-300" style={{ width: `${Math.min((rel / total) * 100, 100)}%` }} />
            </div>
            <div className="mt-1 truncate font-semibold text-slate-200">{a}</div>
            <div className="mono truncate text-[var(--text-faint)]">{b}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
