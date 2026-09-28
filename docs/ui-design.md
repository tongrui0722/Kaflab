# UI 设计（Phase 0）

> 先于编码的重新分析：截图仅作视觉与产品方向参考，本方案已做信息降载与布局优化。

## 1. 产品信息架构

```text
Kafka Client Lab
├── Dashboard（状态总览，不做操作）
├── Producer（MVP 核心：配置 → Trace → Inspector 三栏）
├── Consumer（Phase 4：Fetch / Offset / Heartbeat）
├── Consumer Group（Phase 4：Members / Assignment / Rebalance）
├── Protocol Inspector（独立：API × Version × Request/Response × HEX/Decoded）
├── Trace（历史 Session 回放，按 traceId 检索）
├── Topology（Metadata 驱动的 Producer→Topic→Partition→Leader→Cluster 图）
├── Fault Injection（Phase 5，默认关闭）
└── Settings（Bootstrap / 超时 / 显示偏好）
```

信息优先级：`Trace > Inspector > Topology > Dashboard`。Dashboard 只回答“连上没、慢不慢、对不对”，一切协议细节下沉到 Trace 点击。

## 2. 页面结构（Producer，桌面优先 3 栏）

```text
┌ Sidebar 200px ┬ TopStatus 56px ┬──────────────────────────────┐
│ 导航           │ Cluster/Client │                              │
│               │ Op/Latency/Status                              │
├───────────────┼────────────────┴──────────────────────────────┤
│               │ ┌ Config 280px ┬ Trace flex ┬ Inspector 360px ┐ │
│               │ │ 表单         │ Tabs       │ Req/Res Tabs    │ │
│               │ │ 发送按钮     │ 纵向链路   │ KV + HEX + 树   │ │
│               │ │ Recent列表   │ Topology   │                 │ │
│               │ │              │ 全链时间线 │                 │ │
│               │ └──────────────┴────────────┴─────────────────┘ │
└───────────────┴───────────────────────────────────────────────┘
```

* 三栏宽度可拖动（CSS grid + resize handle，MVP 用固定比例 + 允许横向滚动，预留拖拽）。
* 中间 Tabs：`请求追踪 / 消费组内详情 / 协调解析 / 时间线 / 故障数据` → MVP 只实现 `请求追踪`，其余为残疾态 Tab（点击提示 Roadmap）。
* 右侧 Tabs：`Request / Response` + `Raw HEX` + `Protocol 解析树`，与中间 Trace 联动（选中节点即切换内容）。
* 默认只显示最重要的 KV，HEX/Decoded 折叠，点击展开。

## 3. 用户操作流程（MVP）

```text
1. 左栏填写 Bootstrap/Topic/Key/Value/Acks/Timeouts（Partition=Auto 默认）
2. 点击「发送消息」→ 生成 Session（sess-YYYYMMDD-HHmmss）→ WS 订阅 ws/session/{id}
3. 中间 Trace 逐节点点亮（TCP→ApiVersions→Metadata→Partition→ConnectLeader→Produce→Response）
4. 点击任意节点 → 右侧 Inspector 切到该 API 的 Request/Response/HEX
5. 完成态：顶部 Latency/Status/Offset 更新，Recent 追加一条，点击可回放
6. 失败态：节点变红，右侧 ErrorCode/错误解释 + “Metadata Refresh→Retry” 链（如有）
```

## 4. UI/UX 优化方案（相对截图）

| 截图问题 | 优化 |
|---|---|
| 右侧 Request/Response/HEX/树全部堆叠，信息拥挤 | 右侧分 Tab + 折叠，默认 KV，HEX 懒加载，等宽字体 + 横向滚动 |
| 中间 Trace/拓扑/时间线纵向堆叠过长 | 中间 Tabs 化，Topology 与全链时间线收进可折叠卡片，Trace 保持首屏 |
| 状态颜色滥用 | 语义收敛：蓝=主操作/链接，绿=SUCCESS，红=ERROR，黄=WARNING/RETRY，紫=Metadata/Protocol；其余全部灰阶 |
| 高信息密度导致层级不清 | 卡片统一 `bg-[#0f172a] border-white/5 rounded-xl`，标题 12px 灰色大写，数值 13-14px 白色，等宽字体用于 IP/HEX/Offset |
| 无 Session 概念 | 顶部显示 SessionID + Recent 可回放，Trace 页可按 traceId 搜索 |
| 无加载/失败态 | Trace 节点三态：pending（脉冲）/success（绿勾）/error（红叉）；WS 断开顶部黄条提示并自动 Mock 回放 |

## 5. 组件设计

* `StatusBadge`：SUCCESS/WARNING/ERROR/REBALANCE 四态圆点 + 文字。
* `TraceTimeline`：纵向链路，props: steps[] {index,title,desc,latency,status,correlationId}，onSelect。
* `Inspector`：props: event: KafkaTraceEvent，Tabs + HexView + ProtoTree。
* `TopologyGraph`：纯 CSS/SVG：Producer→Metadata→Topic→P0..P3→Leader Broker→Cluster，active partition 高亮描边 + ACTIVE 徽标。
* `RecentList`：最近发送记录，props 可点击回放。
* `TopStatus`：Cluster/Client/Operation/Latency/Status 五卡。
* `HexView`：offset + hex + ascii 三列，等宽 11px。
* `ProtoTree`：可展开树（details/summary 原生实现，零依赖）。

## 6. 响应式

桌面优先（≥1280 3 栏，768–1280 左右两栏 Trace+Inspector 上下叠，<768 单栏 Tab 切换）。本 MVP 交付桌面完整态 + 移动可访问态。
