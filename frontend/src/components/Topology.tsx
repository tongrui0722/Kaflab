import type { KafkaTraceEvent } from "../types";

export interface TopoPart {
  p: number;
  leader?: number;
  replicas?: number[];
  isr?: number[];
}

export interface TopoBroker { id?: number; host: string; port: number; }

/** A metadata-backed topology. Replica/ISR membership and broker endpoints all come from Kafka Metadata. */
export function TopologyGraph({ topic = "test-topic", partitions = [], brokers = [], activePartition, actualRoute = false, actualBrokerId, onSelectPartition }:
  { topic?: string; partitions?: TopoPart[]; brokers?: TopoBroker[]; activePartition?: number; actualRoute?: boolean; actualBrokerId?: number; onSelectPartition?: (partition: number) => void }) {
  const brokerIds = [...new Set([
    ...brokers.map((b) => b.id).filter((id): id is number => id != null),
    ...partitions.flatMap((p) => [...(p.replicas ?? []), ...(p.leader != null && p.leader >= 0 ? [p.leader] : [])]),
  ])].sort((a, b) => a - b);
  const partitionCols = Math.min(4, Math.max(partitions.length, 1));
  const partitionRows = Math.ceil(Math.max(partitions.length, 1) / partitionCols);
  const partitionStartX = 184;
  const partitionStrideX = 136;
  const partitionStrideY = 128;
  const brokerStartX = partitionStartX + partitionCols * partitionStrideX + 20;
  const width = Math.max(760, brokerStartX + 152);
  const height = Math.max(partitionRows * partitionStrideY + 70, brokerIds.length * 100 + 120, 300);
  const topicX = 24;
  const topicY = (height - 54) / 2;
  const brokerY = (index: number) => 72 + index * 100;
  const partY = (index: number) => 60 + Math.floor(index / partitionCols) * partitionStrideY;
  const partX = (index: number) => partitionStartX + (index % partitionCols) * partitionStrideX;
  const produceIsActual = actualRoute && activePartition != null;
  const activeIndex = partitions.findIndex((p) => p.p === activePartition);
  const active = activeIndex >= 0 ? partitions[activeIndex] : undefined;
  const addrFor = (id: number) => {
    const b = brokers.find((x) => x.id === id);
    return b ? `${b.host}:${b.port}` : "地址未返回";
  };

  return (
    <section aria-label="Kafka Topic 分区与副本拓扑">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <div>
          <div className="card-title !mb-0">真实分区拓扑</div>
          <div className="mt-1 text-[11px] text-[var(--text-faint)]">Kafka Metadata：Leader、副本、ISR 与 Broker 地址 · 点击分区卡片可设为发送目标</div>
        </div>
        <div className="flex flex-wrap gap-1.5 text-[10px]">
          <span className="badge run">Leader</span><span className="badge ok">ISR</span><span className="badge mute">Replica</span>
          {produceIsActual && <span className="badge warn">本次发送路径</span>}
        </div>
      </div>
      {partitions.length === 0 ? (
        <div className="rounded-lg border border-dashed border-[var(--border-soft)] px-4 py-7 text-center text-[12px] text-slate-500">
          {topic ? `未获取到 Topic「${topic}」的分区 Metadata。` : "输入 Topic 并连接集群后显示真实拓扑。"}
        </div>
      ) : (
        <div className="scroll-box overflow-x-auto rounded-xl border border-[var(--border-soft)] bg-black/15">
          <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${topic} 的 Kafka 分区、副本和 Broker 拓扑`} className="block min-w-full">
            <defs>
              <marker id="topology-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b" />
              </marker>
              <marker id="topology-arrow-active" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" fill="#38bdf8" />
              </marker>
            </defs>
            <text x={topicX} y="30" fill="#94a3b8" fontSize="11">TOPIC</text>
            <rect x={topicX} y={topicY} width="124" height="54" rx="10" fill="#111c30" stroke="#334155" />
            <text x={topicX + 62} y={topicY + 23} textAnchor="middle" fill="#e2e8f0" fontSize="12" fontWeight="700">{topic.length > 17 ? `${topic.slice(0, 16)}…` : topic}</text>
            <text x={topicX + 62} y={topicY + 41} textAnchor="middle" fill="#94a3b8" fontSize="10">{partitions.length} 个分区</text>
            <text x={partitionStartX} y="30" fill="#94a3b8" fontSize="11">PARTITIONS · LEADER / REPLICA / ISR</text>
            <text x={brokerStartX} y="30" fill="#94a3b8" fontSize="11">BROKERS</text>

            {partitions.map((p, i) => {
              const x = partX(i);
              const y = partY(i);
              const selected = p.p === activePartition;
              const isr = p.isr ?? [];
              const replicas = p.replicas?.length ? p.replicas : p.leader != null && p.leader >= 0 ? [p.leader] : [];
              return (
                <g key={p.p} className="topology-partition" role={onSelectPartition ? "button" : undefined} tabIndex={onSelectPartition ? 0 : undefined}
                  aria-label={onSelectPartition ? `选择分区 ${p.p}，Leader Broker ${p.leader ?? "未知"}` : undefined}
                  style={onSelectPartition ? { cursor: "pointer" } : undefined}
                  onClick={onSelectPartition ? () => onSelectPartition(p.p) : undefined}
                  onKeyDown={onSelectPartition ? (event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      onSelectPartition(p.p);
                    }
                  } : undefined}>
                  <path d={`M ${topicX + 124} ${topicY + 27} C ${topicX + 150} ${topicY + 27}, ${x - 28} ${y + 12}, ${x} ${y + 12}`} fill="none" stroke={selected ? "#38bdf8" : "#475569"} strokeWidth={selected ? 2 : 1.2} markerEnd={selected ? "url(#topology-arrow-active)" : "url(#topology-arrow)"} />
                  {replicas.map((id) => {
                    const targetX = brokerStartX;
                    const targetY = brokerY(brokerIds.indexOf(id)) + 44;
                    const isLeader = p.leader === id;
                    const inIsr = isr.includes(id);
                    return <path key={`${p.p}-${id}`} d={`M ${x + 112} ${y + 16} C ${x + 145} ${y + 16}, ${targetX - 30} ${targetY}, ${targetX} ${targetY}`} fill="none" stroke={selected && isLeader && produceIsActual ? "#38bdf8" : inIsr ? "#34d399" : "#475569"} strokeWidth={selected && isLeader && produceIsActual ? 2.5 : 1.2} strokeDasharray={inIsr ? undefined : "4 4"} markerEnd={selected && isLeader && produceIsActual ? "url(#topology-arrow-active)" : "url(#topology-arrow)"} />;
                  })}
                  <rect x={x} y={y} width="112" height="106" rx="10" fill={selected ? "#10243a" : "#0e1727"} stroke={selected ? "#38bdf8" : "#334155"} strokeWidth={selected ? 1.6 : 1} />
                  <text x={x + 12} y={y + 22} fill={selected ? "#bae6fd" : "#e2e8f0"} fontSize="12" fontWeight="700">Partition {p.p}</text>
                  <text x={x + 12} y={y + 42} fill="#94a3b8" fontSize="10">Leader</text>
                  <text x={x + 12} y={y + 58} fill={p.leader == null || p.leader < 0 ? "#fb7185" : "#c4b5fd"} fontSize="11" fontWeight="600">Broker {p.leader ?? "—"}</text>
                  <text x={x + 12} y={y + 78} fill="#94a3b8" fontSize="10">副本 {replicas.length} · ISR {isr.length}</text>
                  {selected && produceIsActual && <text x={x + 12} y={y + 96} fill="#38bdf8" fontSize="9">本次发送</text>}
                </g>
              );
            })}

            {brokerIds.map((id, i) => {
              const broker = brokers.find((b) => b.id === id);
              const isTarget = produceIsActual && (actualBrokerId ?? active?.leader) === id;
              const leaderCount = partitions.filter((p) => p.leader === id).length;
              const replicaCount = partitions.filter((p) => (p.replicas ?? []).includes(id)).length;
              return (
                <g key={id}>
                  <rect x={brokerStartX} y={brokerY(i)} width="132" height="88" rx="10" fill={isTarget ? "#102a40" : "#0e1727"} stroke={isTarget ? "#38bdf8" : "#334155"} strokeWidth={isTarget ? 1.7 : 1} />
                  <text x={brokerStartX + 66} y={brokerY(i) + 23} textAnchor="middle" fill={isTarget ? "#bae6fd" : "#e2e8f0"} fontSize="12" fontWeight="700">Broker {id}{isTarget ? " · 目标" : ""}</text>
                  <text x={brokerStartX + 66} y={brokerY(i) + 43} textAnchor="middle" fill="#94a3b8" fontSize="9">{addrFor(id)}</text>
                  <text x={brokerStartX + 66} y={brokerY(i) + 67} textAnchor="middle" fill="#64748b" fontSize="10">Leader {leaderCount} · Replica {replicaCount}</text>
                </g>
              );
            })}
            <text x={topicX} y={height - 18} fill="#64748b" fontSize="10">实线绿色 = 位于 ISR · 灰色虚线 = 副本未进入 ISR · 蓝色 = 本次 Produce 实际路径</text>
          </svg>
        </div>
      )}
    </section>
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
