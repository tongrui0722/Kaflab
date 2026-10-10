import type { ReactNode } from "react";
import type { EvStatus } from "../types";

export function StatusBadge({ status, text }: { status: EvStatus | "warn"; text?: string }) {
  const cls =
    status === "success" ? "ok" :
    status === "error" ? "err" :
    status === "warn" ? "warn" : "run";
  const label = text ?? (status === "success" ? "成功" : status === "error" ? "失败" : status === "warn" ? "告警" : "进行中");
  return (
    <span className={`badge ${cls} ${status === "pending" ? "pulsing" : ""}`}>
      {label}
    </span>
  );
}

/** 消费组状态徽标（概览页与消费组页共用，保证两处显示一致）。
 *  直接展示 Kafka DescribeGroups 原值英文：Stable / Empty /
 *  PreparingRebalancing / CompletingRebalancing / Dead，仅按类别着色。 */
export function GroupStateBadge({ state }: { state?: string | null }) {
  const normalized = (state || "").toLowerCase();
  const cls =
    normalized === "stable" ? "ok" :
    normalized === "empty" ? "mute" :
    normalized.includes("rebalanc") ? "warn" :
    normalized === "dead" ? "err" : "mute";
  return <span className={`badge ${cls}`}>{state || "?"}</span>;
}

export function KV({ k, v, mono }: { k: string; v: ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-3 py-[3px] text-[12px]">
      <span className="shrink-0 text-[var(--text-faint)]">{k}</span>
      <span className={`min-w-0 truncate text-right text-slate-700 ${mono ? "mono" : ""}`}>{v}</span>
    </div>
  );
}

/** 页头：标题 + 说明 + 右侧操作 */
export function PageHeader({ title, desc, right }: { title: string; desc?: string; right?: ReactNode }) {
  return (
    <div className="page-h flex flex-wrap items-end gap-2">
      <div className="min-w-0">
        <h1>{title}</h1>
        {desc && <p>{desc}</p>}
      </div>
      {right && <div className="ml-auto flex items-center gap-2">{right}</div>}
    </div>
  );
}
