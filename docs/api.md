# API（Phase 0）

Base: `VITE_API_BASE=http://localhost:8000`，WS: `VITE_WS_BASE=ws://localhost:8000`

## REST

### POST /api/sessions — 创建 Session
Req: `{ "clientId?": "kafka-sim-01", "operation": "Producer", "topic?": "test-topic", "bootstrap?": "host:9092" }`
Res: `Session`

### GET /api/sessions — 历史
Res: `Session[]`（内存，最新在前，上限 50）

### POST /api/producer/send — 发送（MVP 核心）
Req:
```json
{
  "bootstrap": "192.168.127.129:9092",
  "topic": "test-topic", "partition": "auto",
  "key": "user001", "value": "hello kafka",
  "acks": "all", "retries": 3,
  "requestTimeoutMs": 3000, "deliveryTimeoutMs": 30000,
  "clientId": "kafka-sim-01", "sessionId?": "sess-..."
}
```
Res: `{ "sessionId": "sess-...", "traceId": "tr-...", "status": "running", "mode": "live|mock" }`
说明：立即返回，后续全部经 WS 推送；`mode=mock` 表示后端连不上 Broker 已降级回放。

### GET /api/metadata — 直查 Metadata（供 Metadata Explorer）
Query: `bootstrap, topic` → Res: `{ clusterId, controller, brokers[], topics[] }`

### GET /api/traces/:traceId — 取某 trace 全事件（回放）
Res: `KafkaTraceEvent[]`

### Consumer（Phase 4 预留，已有骨架）
`POST /api/consumer/start | POST /api/consumer/stop | GET /api/groups`

### Fault（Phase 5 预留）
`POST /api/faults {name, enabled} | DELETE /api/faults`

## WebSocket — ws://host/ws/session/{sessionId}

消息：`{ "type": string, "sessionId": string, "event": KafkaTraceEvent }`

| type | 含义 |
|---|---|
| trace.started | Session 开始 |
| trace.tcp.connected | TCP 已连（bootstrap/leader） |
| trace.request.sent | 某 API Request 已发（含 correlationId） |
| trace.response.received | Response 已收（含 latency） |
| trace.metadata.updated | Metadata 解析完成（含 brokers/partitions） |
| trace.partition.selected | Partition/Leader 已选 |
| trace.produce.sent | Produce 已发 |
| trace.produce.success | Produce 成功（含 offset） |
| trace.failed | 失败（含 errorCode/errorName） |
| trace.completed | Session 完成（含 total） |

## 错误模型

```json
{ "code": "TIMEOUT|NETWORK|PROTOCOL|NOT_LEADER|UNKNOWN", "message": "human readable", "retryable": true, "traceId": "tr-..." }
```
所有异常进 WS `trace.failed` + HTTP 4xx/5xx 同构。
