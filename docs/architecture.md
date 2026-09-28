# Architecture（Phase 0）

## 1. 前后端技术架构

```text
React 18 + TS(strict) + Vite + Tailwind
  │  REST: POST /api/sessions /api/producer/send /api/metadata ...
  │  WS:   ws://host/ws/session/{sessionId}  (统一 KafkaTraceEvent)
  ▼
FastAPI (Backend)
  ├── routers/      HTTP + WS 薄层（参数校验、Session 绑定，不写协议解析）
  ├── session/      SessionManager（创建/查询/历史）
  ├── trace/        TraceManager（traceId、事件流、内存环形缓冲 + 可回放）
  ├── network/      TcpConnector（原生 socket，直连 Broker，超时可配）
  ├── protocol/     二进制编解码（ApiVersions/Metadata/Produce + 通用 header）
  ├── producer/     ProducerEngine（业务编排：路由→Leader→Produce→Retry）
  ├── consumer/     预留（Phase 4）
  └── fault/        FaultEngine（Phase 5，默认关闭，中间件式注入）
```

为什么后端用 Python 而非 C++：提示词建议 C++ 以便控制 TCP/协议，但 MVP 需快速交付“真实 TCP + 真实二进制协议”且可测；Python 原生 socket 同样能完整控制 `TCP→ApiVersions→Metadata→Produce` 且协议编解码可单测。目录已按 `network/protocol` 与业务分离，后续可平滑替换为 C++ 引擎（TraceEvent 接口不变）。

## 2. 数据模型

```typescript
interface KafkaTraceEvent {
  traceId: string; timestamp: number;
  apiKey: number; apiName: string; apiVersion: number;
  correlationId: number; direction: "request" | "response" | "local";
  step: string; stepIndex: number; status: "pending"|"success"|"error";
  broker?: { id?: number; host: string; port: number };
  topic?: string; partition?: number;
  latencyMs?: number; errorCode?: number; errorName?: string;
  request?: unknown; response?: unknown;
  rawRequest?: string; rawResponse?: string;
}
interface Session {
  id: string; clientId: string; operation: "Producer"|"Consumer";
  topic: string; status: "running"|"success"|"error";
  startTime: number; endTime?: number; durationMs?: number;
  bootstrap: string; partition?: number; offset?: number;
}
```

WS 事件名（`type` 字段）：`trace.started | trace.tcp.connected | trace.request.sent |
trace.response.received | trace.metadata.updated | trace.partition.selected |
trace.produce.sent | trace.produce.success | trace.failed | trace.completed`
每条 WS 消息 = `{ type, sessionId, event: KafkaTraceEvent }`。

## 3. 真实链路时序（Producer）

```text
UI POST /api/producer/send → SessionManager.create → ProducerEngine.run (后台 task)
 → TcpConnector.connect(bootstrap) → WS trace.tcp.connected
 → ApiVersions v0 encode→send→decode → WS request.sent/response.received
 → Metadata v1 (topic) → 解析 brokers/topics/partitions/leader → WS metadata.updated
 → Partition 选择 (auto=sticky/轮询或指定) → WS partition.selected
 → TcpConnector.connect(leader) → WS tcp.connected
 → Produce v9 (acks/timeout/records) → WS produce.sent → decode → WS produce.success
 → Session success + offset → WS completed → Recent 可回放
异常任一步 → trace.failed（含 errorCode/errorName）→ 可选 Metadata Refresh→Retry（Phase 5 前只做一次自动 refresh）
```

无 Kafka 可连（超时/拒绝）→ 自动降级 `mock` 全链路回放（与效果图一致：36ms，P2→103，offset 10231），`session.mode="mock"` 明确标注，不伪装成真实。

## 4. 目录结构（实际）

```text
kafka-client-lab/
├── frontend/src/{pages,components,api,types,mock}
├── backend/app/{network,protocol,producer,consumer,trace,session,fault,routers}
├── docs/{ui-design.md,architecture.md,api.md}
└── README.md
```

## 5. MVP vs 迭代

MVP：Producer 真实链 + Trace 点击联动 + Inspector(HEX/树) + Topology + Recent + Session。
Phase 4：consumer/routers + group 页（FindCoordinator/JoinGroup/SyncGroup/Fetch/Heartbeat）。
Phase 5：fault/ 注入矩阵 + NotLeader→Refresh→Retry 可视化。
