import { useState } from "react";
import type { KafkaTraceEvent } from "../types";

export interface TopoPart {
  p: number;
  leader?: number;
  replicas?: number[];
  isr?: number[];
}

export interface TopoBroker { id?: number; host: string; port: number; }

/** Real Kafka metadata rendered as a partition-to-broker assignment matrix. */
export function TopologyGraph({ topic = "test-topic", partitions = [], brokers = [], activePartition, actualRoute = false, actualBrokerId, onSelectPartition }:
  { topic?: string; partitions?: TopoPart[]; brokers?: TopoBroker[]; activePartition?: number; actualRoute?: boolean; actualBrokerId?: number; onSelectPartition?: (partition: number) => void }) {
  const [focusedBroker, setFocusedBroker] = useState<number | null>(null);
  const sortedPartitions = [...partitions].sort((a, b) => a.p - b.p);
  const brokerIds = [...new Set([
    ...brokers.map((b) => b.id).filter((id): id is number => id != null),
    ...sortedPartitions.flatMap((p) => [...(p.replicas ?? []), ...(p.leader != null && p.leader >= 0 ? [p.leader] : [])]),
  ])].sort((a, b) => a - b);
  const underReplicated = sortedPartitions.filter((p) => {
    const replicas = p.replicas?.length ? p.replicas : p.leader != null && p.leader >= 0 ? [p.leader] : [];
    return replicas.some((id) => !(p.isr ?? []).includes(id));
  }).length;
  const noLeader = sortedPartitions.filter((p) => p.leader == null || p.leader < 0).length;
  const leaderFor = (p: TopoPart) => p.leader != null && p.leader >= 0 ? p.leader : undefined;
  const replicasFor = (p: TopoPart) => p.replicas?.length ? p.replicas : leaderFor(p) != null ? [leaderFor(p)!] : [];
  const brokerFor = (id: number) => brokers.find((b) => b.id === id);
  const leaderCount = (id: number) => sortedPartitions.filter((p) => p.leader === id).length;
  const replicaCount = (id: number) => sortedPartitions.filter((p) => (p.replicas ?? []).includes(id)).length;
  const isActualPartition = (p: TopoPart) => actualRoute && p.p === activePartition;

  return (
    <section aria-label="Kafka Topic 分区副本分布">
      <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="card-title !mb-0">分区副本分布</div>
          <div className="mt-1 text-[11px] text-[var(--text-faint)]">
            {topic} · Kafka Metadata · 点击分区设置发送目标，点击 Broker 高亮其分区
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-1.5 text-[10px]">
          <span className="badge run">Leader</span>
          <span className="badge ok">副本在 ISR</span>
          <span className="badge err">副本不在 ISR</span>
          {focusedBroker != null && (
            <button className="btn btn-ghost btn-sm" onClick={() => setFocusedBroker(null)}>
              清除 Broker 高亮
            </button>
          )}
        </div>
      </div>

      {sortedPartitions.length === 0 ? (
        <div className="rounded-lg border border-dashed border-[var(--border-soft)] px-4 py-7 text-center text-[12px] text-slate-500">
          {topic ? `未获取到 Topic「${topic}」的分区 Metadata。` : "输入 Topic 并连接集群后显示分区副本分布。"}
        </div>
      ) : (
        <>
          <div className="mb-3 grid grid-cols-2 gap-2 sm:grid-cols-5">
            {[
              ["分区", String(sortedPartitions.length)],
              ["Broker", String(brokerIds.length)],
              ["副本总数", String(sortedPartitions.reduce((n, p) => n + replicasFor(p).length, 0))],
              ["副本异常", String(underReplicated)],
              ["无 Leader", String(noLeader)],
            ].map(([label, value]) => (
              <div key={label} className="rounded-lg border border-[var(--border-soft)] bg-[var(--panel-3)] px-3 py-2">
                <div className="text-[10px] text-[var(--text-faint)]">{label}</div>
                <div className={`mono mt-0.5 text-[14px] font-semibold ${label === "副本异常" && underReplicated > 0 ? "text-rose-300" : "text-slate-100"}`}>{value}</div>
              </div>
            ))}
          </div>

          <div className="topology-matrix-wrap overflow-hidden rounded-xl border border-[var(--border-soft)]">
            <table className="topology-matrix-table w-full table-fixed border-collapse text-left text-[11px]">
              <thead className="bg-[var(--panel-3)]">
                <tr>
                  <th className="w-[90px] border-b border-[var(--border-soft)] bg-[var(--panel-3)] px-2 py-2 font-medium text-slate-400">Partition</th>
                  <th className="w-[72px] border-b border-[var(--border-soft)] px-1.5 py-2 font-medium text-slate-400">Leader</th>
                  {brokerIds.map((id) => {
                    const broker = brokerFor(id);
                    const focused = focusedBroker === id;
                    return (
                      <th key={id} className={`border-b border-[var(--border-soft)] px-1.5 py-1.5 ${focused ? "bg-sky-500/10" : ""}`}>
                        <button type="button" aria-pressed={focused} onClick={() => setFocusedBroker(focused ? null : id)}
                          className={`w-full rounded-md px-1.5 py-1 text-left transition-colors hover:bg-sky-500/10 ${focused ? "text-sky-200" : "text-slate-300"}`}>
                          <span className="block font-semibold">Broker {id}{id === actualBrokerId && actualRoute ? " · 本次发送" : ""}</span>
                          <span className="mono mt-0.5 block truncate text-[10px] text-[var(--text-faint)]" title={broker ? `${broker.host}:${broker.port}` : "地址未返回"}>{broker ? `${broker.host}:${broker.port}` : "地址未返回"}</span>
                          <span className="mt-0.5 block truncate text-[10px] font-normal text-slate-500">Leader {leaderCount(id)} · Replica {replicaCount(id)}</span>
                        </button>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {sortedPartitions.map((p) => {
                  const leader = leaderFor(p);
                  const replicaIds = replicasFor(p);
                  const inSyncIds = p.isr ?? [];
                  const containsBroker = focusedBroker == null || replicaIds.includes(focusedBroker);
                  const selected = p.p === activePartition;
                  const lastSent = isActualPartition(p);
                  return (
                    <tr key={p.p} className={`border-b border-[var(--border-soft)] last:border-b-0 transition-colors ${selected ? "bg-sky-500/[0.07]" : "hover:bg-white/[0.02]"} ${!containsBroker ? "opacity-35" : ""}`}>
                      <td className={`border-r border-[var(--border-soft)] px-1.5 py-2 ${selected ? "bg-[#e5effb]" : "bg-[var(--panel)]"}`}>
                        <button type="button" aria-pressed={selected} disabled={!onSelectPartition}
                          onClick={() => onSelectPartition?.(p.p)}
                          className={`flex min-h-8 items-center gap-2 rounded-md px-2 text-left transition-colors ${selected ? "font-semibold text-sky-200" : "text-slate-300 hover:bg-sky-500/10 hover:text-sky-100"} disabled:cursor-default`}>
                          <span className="mono">P{p.p}</span>
                          {lastSent && <span className="badge warn !px-1.5 !py-0">已发</span>}
                          {selected && !lastSent && <span className="badge run !px-1.5 !py-0">目标</span>}
                        </button>
                      </td>
                      <td className="px-1.5 py-2">
                        {leader == null ? <span className="badge err">无 Leader</span> : (
                          <span className={`badge ${lastSent && leader === actualBrokerId ? "run" : "mute"}`} title={`Broker ${leader}`}>B{leader}</span>
                        )}
                      </td>
                      {brokerIds.map((id) => {
                        const isReplica = replicaIds.includes(id);
                        if (!isReplica) return <td key={id} className={`px-1.5 py-2 ${focusedBroker === id ? "bg-sky-500/[0.06]" : ""}`}><span className="text-slate-700">—</span></td>;
                        const isLeader = leader === id;
                        const inSync = inSyncIds.includes(id);
                        const isActualLeader = lastSent && isLeader && actualBrokerId === id;
                        return (
                        <td key={id} className={`min-w-0 px-1.5 py-2 ${focusedBroker === id ? "bg-sky-500/[0.06]" : ""}`}>
                            <div title={`${isLeader ? "Leader" : "Replica"} · ${inSync ? "ISR" : "不同步"}`} className={`flex min-h-8 min-w-0 items-center gap-1 rounded-md border px-1.5 py-1 ${isActualLeader ? "border-sky-400/60 bg-sky-500/15" : isLeader ? "border-violet-400/30 bg-violet-500/[0.08]" : inSync ? "border-emerald-500/20 bg-emerald-500/[0.06]" : "border-rose-500/30 bg-rose-500/[0.08]"}`}>
                              <span className={`topology-role-long truncate text-[10px] font-semibold ${isLeader ? "text-violet-200" : inSync ? "text-emerald-200" : "text-rose-200"}`}>{isLeader ? "Leader" : "Replica"}</span>
                              <span className={`topology-role-short text-[10px] font-semibold ${isLeader ? "text-violet-200" : inSync ? "text-emerald-200" : "text-rose-200"}`}>{isLeader ? "L" : "R"}</span>
                              <span className={`ml-auto shrink-0 text-[11px] ${inSync ? "text-emerald-300" : "text-rose-300"}`}>{inSync ? "ISR" : "!"}</span>
                            </div>
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-[10px] text-[var(--text-faint)]">
            <span>Leader 是分区当前写入节点；ISR 表示已跟上 Leader 的副本。点击 Broker 表头可高亮其 Leader/Replica 分区。</span>
            {focusedBroker != null && <span className="mono text-sky-300">当前聚焦 Broker {focusedBroker}</span>}
          </div>
        </>
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
