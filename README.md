# Kafka Client Lab

> Kafka Client Protocol & Trace Laboratory
>
> **核心体验：** “我发送一条消息，然后亲眼看到 Kafka Client 到底向哪个 Broker 发了什么请求、为什么发、经过了哪些步骤、返回了什么，以及每一步耗时多少。”

`Trace > Dashboard` · `Protocol > 图表` · `真实 Client > Mock` · `链路可解释性 > UI 装饰` · `故障可复现 > 单纯监控`

---

## 产品定位

不是 Kafka Admin UI，也不是 Monitoring UI，而是 **Kafka Client Laboratory**：通过 Web UI 操作一个真实的 Kafka Client，实时观察 `TCP → ApiVersions → Metadata → Partition/Leader 路由 → Produce/Fetch → Consumer Group/Heartbeat/Rebalance` 完整链路，面向 Kafka 学习、客户端开发、故障定位、协议分析、故障模拟。

核心链（MVP 已实现）：

```text
用户输入消息 → TCP Connect → ApiVersions → Metadata → 解析Topic/Partition/Leader
→ 建立 Leader 连接 → Produce Request → Produce Response → Offset → 完整 Trace
```

## 快速启动

### 前端（静态 UI + Mock，可直接看效果）

```bash
cd frontend
npm install
npm run dev
# http://localhost:5173
```

### 后端（Session + WebSocket + 真实 Kafka TCP Client）

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
# 前端 .env: VITE_API_BASE=http://localhost:8000  VITE_WS_BASE=ws://localhost:8000
```

### 真实发送（有 Kafka 时）

1. 打开 Producer 页，`Bootstrap Servers` 填如 `192.168.127.129:9092`
2. Topic `test-topic`，Key `user001`，Value `hello kafka`，Partition `Auto`
3. 点击 **发送消息**，中间 Trace 实时推进，点击任意节点右侧 Inspector 查看 Request/Response/HEX/Decoded
4. 无 Kafka 时后端自动进入 `mock` 模式，回放与效果图一致的 36ms 全链路（含 offset=10231），保证验收流程不断

## 技术架构

```text
Web UI (React+TS+Vite+Tailwind)
  │ REST / WebSocket (统一 KafkaTraceEvent)
  ▼
Backend (FastAPI)
  ├── Session Manager / Trace Manager
  ├── Kafka Client Engine (原生 TCP + 二进制协议编解码, 无 librdkafka)
  │     network/ protocol/ producer/ consumer/ fault/
  └── WS Publisher (trace.started / request.sent / response.received / ... / completed)
        ▼
Kafka Broker(s)
```

* 协议与业务分离，Trace 与 Client 解耦，所有 Request/Response 必产生 TraceEvent
* 连接配置全部由 UI 输入，代码无硬编码 Broker IP
* 协议编解码有单元测试（`backend/tests/test_protocol.py`）

## MVP 范围（本仓库已交付）

* [x] Phase 0 设计文档：`docs/ui-design.md` `docs/architecture.md` `docs/api.md`
* [x] Phase 1 前端静态 UI：Dashboard / Producer / Trace / Inspector / Metadata / Topology / Protocol / Recent / Session
* [x] Phase 2 后端 Session + WebSocket + REST（Mock Event 联调通过）
* [x] Phase 3 真实 Client：TCP + ApiVersions + Metadata + Produce（Python 原生实现）
* [ ] Phase 4 Consumer 全链（FindCoordinator/JoinGroup/SyncGroup/Fetch/Heartbeat/OffsetCommit）— UI 骨架已留位，API 已预留
* [ ] Phase 5 Fault Injection（NotLeader/Timeout/Disconnect/Rebalance 等）— UI 开关 + API 骨架已留位，默认关闭

## 后续 Roadmap

1. Consumer Fetch 可视化 + Consumer Group Members/Assignment 实时页
2. Rebalance 时间线（Normal/Warning/Rebalance/Error 四态着色）
3. Fault 矩阵：WrongBroker / MetadataTimeout / ProduceTimeout / DisconnectBeforeResponse / HeartbeatTimeout / PollTimeout
4. HEX ↔ Decoded 双向对照 + CorrelationId 串联 + 历史 Session 回放
5. librdkafka 可选引擎对照模式（与原生引擎 Trace 对比）

## 项目结构

```text
kafka-client-lab/
├── frontend/src/{pages,components,api,types,mock}
├── backend/app/{network,protocol,producer,consumer,trace,session,fault,routers}
├── docs/{ui-design.md,architecture.md,api.md}
└── README.md
```

详细见 `docs/architecture.md` 与 `docs/api.md`。
