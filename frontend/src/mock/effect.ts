import type { KafkaTraceEvent, TraceStep } from "../types";

export function stepsFromEvents(events: KafkaTraceEvent[]): TraceStep[] {
  const byIdx = new Map<number, KafkaTraceEvent[]>();
  for (const e of events) {
    if (!byIdx.has(e.stepIndex)) byIdx.set(e.stepIndex, []);
    byIdx.get(e.stepIndex)!.push(e);
  }
  const pick = (i: number) => {
    const arr = byIdx.get(i) ?? [];
      return [...arr].reverse().find((x) => x.direction === "response") ?? arr[arr.length - 1];
  };
  const titles: Record<number, [string, string]> = {
    1: ["TCP Connect", ""],
    2: ["ApiVersions", "协商支持的 API 版本"],
    3: ["Metadata", "查询 Topic 元数据"],
    4: ["Partition Selection", "选择 Partition"],
    5: ["Connect Leader", "连接 Leader Broker"],
    6: ["Produce", "发送消息"],
    7: ["Produce Response", "确认"],
  };
  return [1, 2, 3, 4, 5, 6, 7].map((i) => {
    const e = pick(i);
    const lat = e?.latencyMs != null ? `${e.latencyMs} ms` : "—";
    const desc =
      e?.broker ? `${e.broker.host}:${e.broker.port}` :
      e?.topic ? `topic=${e.topic}` : titles[i][1];
    // 请求已发出 = 该步已完成；只有还没收到任何事件才算 pending
    const status = !e ? "pending"
      : e.status === "error" ? "error"
      : e.direction === "request" && e.status === "pending" ? "success"
      : e.status;
    return {
      index: i, title: titles[i][0], desc,
      latency: lat,
      status,
      event: e,
    };
  });
}

export function fmtTime(ms: number): string {
  return new Date(ms).toLocaleTimeString("zh-CN", { hour12: false });
}
