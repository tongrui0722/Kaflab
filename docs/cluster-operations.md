# KAFLAB 集群操作实现与影响说明

> 核对基准：2026-10-22 仓库当前代码。本文描述**现有实现**；实际效果还取决于 Kafka 版本、ACL、Broker 配置、网络路由和部署方式。文中的“只读”指应用没有主动发送写入类 Kafka API；另见[只读操作的边界](#只读操作的边界)。

## 1. 先区分三种“操作”

1. **KAFLAB 本地配置变更**：新增、编辑、移除纳管集群，以及开关自动 Lag 采集。它们修改 KAFLAB 的文件或本地数据库，**不会**在 Kafka 中创建、修改或删除集群/Broker。
2. **Kafka 查询**：连接探测、元数据、健康状态、消费组、Lag、Topic 配置和消息浏览。它们产生连接、网络流量及 Broker 查询负载，通常不改变消息和提交位点。
3. **Kafka 写入或管理命令**：创建/删除 Topic、扩容分区、调整副本、修改 Topic 配置、生产消息、带消费组的消费、压测及部分客户端模拟。它们会改变 Kafka 的数据或元数据；超时后不能仅凭 HTTP 状态断定“没有执行”。

当前前端是 React/TypeScript，后端是 FastAPI。常规工作台和大部分管理功能由后端自行编码 Kafka TCP 协议帧；**客户端模拟**才启动独立语言客户端进程。页面向 FastAPI 发 HTTP 请求，后端根据纳管配置或页面填写的独立连接参数访问 Broker：

```text
浏览器页面 → FastAPI 路由 → 连接配置 / 权限检查
                        ├─ TCP + TLS/SASL + Kafka 协议 → Bootstrap → Metadata
                        │                                  ├─ Controller：Topic 管理
                        │                                  ├─ Leader：Produce / Fetch / ListOffsets
                        │                                  └─ Coordinator：消费组 / OffsetFetch / OffsetCommit
                        ├─ 本地文件/SQLite：配置、采样、会话、追踪
                        └─ 本地子进程：Python / Java / Node.js / Go 客户端模拟
```

## 2. 操作总表

| 页面/操作 | 后端接口或入口 | 关键实现 | 对 Kafka 的影响 | 对 KAFLAB 的影响 |
| --- | --- | --- | --- | --- |
| 新增纳管集群 | `POST /api/clusters`（无 `id`） | 保存连接、安全协议、认证及采集开关 | 不创建 Kafka 集群；后续采集可能开始连接 | 写 `clusters.json`；只读模式也允许新增 |
| 编辑纳管集群 | `POST /api/clusters`（带 `id`） | 覆盖本地配置 | 不修改 Broker 配置 | 更新文件；连接地址/协议/认证机制变化时清空该集群已知节点缓存；需全部功能模式 |
| 移除纳管集群 | `DELETE /api/clusters/{cid}` | 删除本地记录 | **不删除 Kafka 集群、Topic 或消息** | 删除连接记录和已知节点缓存；历史采样文件并未在该接口清理；需全部功能模式 |
| 开关自动 Lag 采集 | 编辑集群的 `monitoringEnabled` | 后台采集循环按开关工作 | 只增加/停止元数据、消费组、位点查询负载 | 每轮成功结果写 SQLite；不修改消费者提交位点 |
| 集群探测 | `POST /api/clusters/{cid}/test` | TCP、`ApiVersions`、`Metadata v0/v1` | 查询 Broker、Topic 数和 Controller | 更新 `broker_nodes.json` 中的已见 Broker |
| 集群总览 / Topic 列表与详情 | `GET /api/clusters/{cid}/overview`、`/topics`、`/topics/{topic}` | Metadata；总览另查消费组 | 查询负载；不主动写消息/位点 | 汇总健康状态；刷新已知 Broker 缓存 |
| Topic 创建 | `POST /api/clusters/{cid}/topics` | `CreateTopics v0` 发给 Controller | 新建 Topic、分区和副本，可能应用初始配置 | 返回结果；需全部功能模式 |
| Topic 删除 | `DELETE /api/clusters/{cid}/topics/{topic}` | `DeleteTopics v0–v3` 发给 Controller | 启用删除时删除 Topic 及其数据，执行可能异步 | 仅返回/展示结果；需全部功能模式 |
| 分区扩容 | `POST /api/clusters/{cid}/topics/{topic}/partitions` | `CreatePartitions v0` | 增加分区及副本任务；不能用此操作缩回 | 需全部功能模式 |
| 副本调整 | `POST /api/clusters/{cid}/topics/{topic}/reassignment` | 为**所有分区**生成轮询分配并发 `AlterPartitionReassignments v0` | 可能搬迁现有副本，产生磁盘/网络负载；提交后异步追 ISR | 需全部功能模式 |
| 查看/修改 Topic 配置 | `GET`/`PUT /api/clusters/{cid}/topics/{topic}/config` | `DescribeConfigs v0` / **`AlterConfigs v0`** | GET 查询；PUT 改变保留、清理、ISR 等配置，详见[重要风险](#topic-配置修改的重要风险) | PUT 需全部功能模式 |
| 浏览 Topic 消息 | `GET /api/clusters/{cid}/topics/{topic}/messages` | Metadata、ListOffsets、`Fetch v4`，临时连接 | 读取消息，不入组、不提交 Offset；增加读 I/O | 不保存消费进度 |
| 消费组列表 / 详情 / Lag | `/groups/overview`、`/groups/{id}/lag` 等 | 每 Broker `ListGroups`，`DescribeGroups`，`OffsetFetch`，`ListOffsets` | 查询状态和位点；不提交 Offset | 总览和采集可能写本地采样库 |
| Producer 工作台 | `POST /api/producer/send` | 后台任务，`Produce v0`；最多 1 万条，生成批次每次最多 100 条 | 写入消息、增长 Offset；失败可能已有部分批次成功 | 保存 Session/Trace；需全部功能模式 |
| Consumer 工作台（无 Group ID） | `POST /api/consumer/fetch` | `Fetch v4`，必要时 `ListOffsets` | 读取指定分区，不提交位点 | 保存 Session/Trace |
| Consumer 工作台（有 Group ID） | 同上 | `FindCoordinator`、`OffsetFetch`、`JoinGroup`、`SyncGroup`、`OffsetCommit`、`Heartbeat`、`LeaveGroup` | **可能触发 Rebalance 并改变该组已提交 Offset** | 保存 Session/Trace；需全部功能模式 |
| 压测 | `POST /api/loadtest/run` | 独立连接参数，多 worker 连续 `Produce v0` | 大量写入及 Broker/网络/磁盘负载 | Job/统计在后端内存；需全部功能模式 |
| 生产端模拟 | `POST /api/client-sim/runs` | 启动本地语言客户端子进程 | 写入消息；启用“Topic 不存在时创建”还会发建 Topic 命令 | 保存运行元数据和日志；需全部功能模式 |
| 消费端模拟 | 同上 | 本地语言客户端订阅/拉取 | `commit=true` 时可自动提交位点；入组可能触发 Rebalance | 保存运行元数据和日志；需全部功能模式 |

### 2.1 哪些接口直接使用“当前工作空间”

路径含 `/api/clusters/{cid}` 的功能从纳管集群读取连接配置。Producer/Consumer 工作台、压测和客户端模拟接收**请求体中的 Bootstrap 与认证参数**；页面可以从工作空间带入，但实际运行目标以提交的配置为准。这意味着选错地址会操作另一个集群。客户端模拟、压测可不依赖当前工作空间。

## 3. 公共技术机制

### 3.1 连接、认证和路由

- `KafkaClient` 使用 TCP 长度前缀帧，`codec.py` 编解码协议请求，`correlationId` 配对响应。`SSL`/`SASL_SSL` 经 Python SSL 上下文建立 TLS；SASL 支持 PLAIN、SCRAM-SHA-256、SCRAM-SHA-512。工作台和纳管集群路径默认走系统 CA；压测支持指定 KAFLAB 主机可访问的 CA 文件路径。
- 常规协议路径对逗号分隔的 Bootstrap **只取第一个种子地址**，之后依据 Metadata 中的 `host:port` 寻找 Controller、Leader 和 Coordinator；压测会依次尝试多个种子，多语言客户端自行处理种子列表。必须确保 Broker 的 `advertised.listeners` 对 KAFLAB 主机可解析、可达，且地址能路由到**对应 Broker**。如果外部代理/NodePort 把多个 Broker 地址都转发到任意 Pod，创建 Topic 等 Controller 操作可能报 `NOT_CONTROLLER (41)`。
- 部分读写链路连接元数据中的目标失败时会回退 Bootstrap 地址；这只能缓解单点不可达，**不能保证**请求最终到达正确的 Broker/Leader/Coordinator。Kafka 返回的错误码会被映射成 HTTP 错误或页面提示。
- Topic 创建/删除及分区/副本管理先用 Metadata 找 Controller。创建和删除只在 Kafka **明确返回** `NOT_CONTROLLER` 时刷新元数据并重试一次；超时、连接中断、响应不完整等结果不自动重发，需刷新列表核实。分区扩容/重分配同样会在明确的 Controller 错误后重试一次。

### 3.2 权限与运行边界

- 登录成功后 Token 只保存在后端进程内存，重启后失效。`readonly`/`full` 是 KAFLAB 页面和**部分后端路由**的操作模式；不是 Kafka ACL。只读模式仍允许新增纳管集群，编辑/移除及主要写入操作由 `_need_lab` 限制。
- **当前 API 鉴权并不统一**：`GET /api/clusters`、总览、趋势、Topic 配置查询及客户端模拟接口会检查登录；若干探测、Topic 列表/详情/消息、消费组查询、Session/Trace 读取以及**无 Group ID 的 Consumer Fetch**路由目前没有统一 Token 检查。前端登录保护不能代替服务端鉴权。生产部署应额外限制 API 网络访问，并把补齐路由鉴权列为优先改进。
- `clusters.json` 中保存 `saslPassword` 明文；当前 `GET /api/clusters` 对已登录用户返回完整 `model_dump()`，包含密码。`users.json` 的本地用户密码也不是哈希存储。应限制数据卷、备份和 API 访问权限。容器使用 `KCL_AUTH_SOURCE=environment` 时用 `KCL_USERNAME`/`KCL_PASSWORD`，不设置时默认 `admin`/`admin`；这是 KAFLAB 登录账户，**不是** Kafka 的 SASL 账户。
- `KCL_DATA_DIR` 指向持久化目录；新数据目录初次启动为空。未配置它的本地开发模式可能载入示例集群。多个后端进程共享文件/SQLite 时，内存 Token、任务状态与文件写入没有跨进程协调，不宜把当前实现视为多副本管理服务。

## 4. 集群纳管、探测与监控

### 4.1 纳管集群

新增/编辑时后端把名称、Bootstrap、安全协议、SASL 参数和采集开关写入 `clusters.json`，ID 是 KAFLAB 自己生成的 `c-…` 标识，**不是 Kafka Cluster ID**。保存只校验基本字段；需要另点“探测”或打开页面才会验证真实连接。移除只删除本地记录。由 `broker_nodes.json` 保留的已见 Broker ID，会与当前 Metadata 的 Broker 及分区副本集合比较；若某已见 ID 不再出现，界面显示离线。这是**基于元数据的状态推断**，不是逐节点 TCP 健康检查，也不是 Kubernetes Pod 健康状态。

### 4.2 总览与 Lag

总览并发读取 Topic 元数据和消费组快照：统计分区数、无 Leader 分区、ISR 少于副本数的分区、Broker Leader/副本/ISR 分布、消费组成员与 Lag。Controller ID 来自 `Metadata v1`；若此查询失败，Topic 分布仍可展示，但 Controller 可能为空。无提交位点的分区记为未知，不伪造为零；部分 Broker/组查询失败时，Lag 标记为部分或不可用。

后端启动即启动 `LagCollector`，**不依赖用户是否登录或页面是否打开**。默认每 60 秒采集一次，每个集群外层总超时 30 秒，可由 `KCL_LAG_INTERVAL_SECONDS`、`KCL_LAG_TIMEOUT_SECONDS` 调整。每轮重新读取纳管列表，仅采集 `monitoringEnabled=true` 的集群；采集成功把组 Lag 写入 `monitoring_samples.db`，样本保留 3 天。采集失败保留上次成功值并记录错误，不能把旧值误认为当前实时值。采集过程会对每个 Broker 查 ListGroups，并按分区查末端 Offset，Topic/分区多时负载和耗时明显增加。计算口径是 **log-end Offset − 已提交 Offset**，不是消费者进程的真实处理耗时。

## 5. Topic 管理的具体影响

### 5.1 创建、删除、扩容、重分配

- **创建**：校验 Topic 名称、分区数 `1–1000` 和副本数不超过 Metadata 中的 Broker 数；向当前 Controller 发 `CreateTopics v0`。成功响应代表创建请求被 Kafka 接受，Topic 元数据和副本可见性可能稍后才稳定。初始配置随请求提交；集群策略/ACL 仍可拒绝。
- **删除**：按 Broker `ApiVersions` 选择已实现的 `DeleteTopics v0–v3`，向 Controller 提交。返回成功后需再刷新 Topic 列表确认删除完成；若 Broker `delete.topic.enable=false`，当前代码把错误 `73` 显示为删除被禁用。删除会丢失 Topic 中的数据及其分区结构，平台没有回收站。
- **扩容分区**：`CreatePartitions v0` 提交的是**目标总分区数**，而不是“再加几个”；只能增加，不能通过该接口减少。新增分区使后续分区选择空间变化，依赖 Key 的生产者路由可能与扩容前不同，相关消费者组也可能重新分配。
- **调整副本数**：代码不只改变一个 `replication.factor` 数字，而是根据当前 Broker ID 对该 Topic 的**每个分区**重新生成轮询副本列表，再发 `AlterPartitionReassignments v0`。因此即使目标副本数与现值相同，也可能改变副本位置。提交后 Kafka 异步搬迁，可能带来网络、磁盘和 ISR 压力；本接口没有等待完成、进度查询或撤销逻辑，需观察后续 Metadata/ISR。

### 5.2 Topic 配置修改的重要风险

读取使用 `DescribeConfigs v0`。保存把页面传来的键值送入 **`AlterConfigs v0`**，`null`/空串被编码为该配置项的空值；请求的 `validate_only=false`，会真实应用。当前代码只传“本次变更的键”，**没有**先合并该 Topic 的全部现有覆盖配置。Kafka 的旧式 `AlterConfigs` 属于整体替换语义，遗漏的现有覆盖项可能被重置为默认值；它也已被增量式 API 取代。因此，在现有实现升级为 `IncrementalAlterConfigs` 并验证前，**不要在含有其他 Topic 覆盖配置的生产集群上使用此保存功能**。这是从请求编码和 Kafka API 语义得出的风险判断，不是每种 Broker 版本均已做现场复测。[Apache Kafka KIP-339](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/87298616/KIP-339%2BCreate%2Ba%2Bnew%2BIncrementalAlterConfigs%2BAPI)、[KIP-894](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/235837121/KIP-894%2BUse%2BincrementalAlterConfigs%2BAPI%2Bfor%2Bsyncing%2Btopic%2Bconfigurations)说明旧 API 的非增量问题。

常见影响包括：`retention.ms`/`retention.bytes` 改变历史数据保留，`cleanup.policy` 改变清理方式，`min.insync.replicas` 与生产者 `acks=all` 一起影响可写性，`max.message.bytes` 影响生产请求可否被接受。更新返回成功也不代表已有日志数据会立即按新策略清理。

## 6. 消息与消费组

### 6.1 Topic 消息浏览与 Consumer 工作台

Topic 的“消息”页按分区及最早/最新/指定 Offset/时间定位，通过 `ListOffsets` 与 `Fetch v4` 读取，支持 Key/Value 过滤和续查；单次默认取 100 条、最多返回 1000 条，扫描最多 1 万条或 30 秒，每次 Fetch 目标上限 1 MiB。它不用 Group 协议，也不提交 Offset；代价是 Broker 读取/传输负载。

Consumer 工作台是另一条技术链：一次任务读取**一个指定分区**。显式 Offset 优先；有 Group ID 时优先查询该组已提交 Offset；都没有则以分区末端 Offset 减去 `maxMessages` 为起点。无 Group ID 不提交。填写 Group ID 时，后端会 `JoinGroup → SyncGroup → OffsetCommit → Heartbeat → LeaveGroup`；即使只想“看一下消息”，也可能触发其他成员 Rebalance，且改变提交位点。当前提交值按 `start + 本次返回消息数` 计算，任务完成后会离组；这不是持续运行的标准消费者，不适合替代业务消费程序。

### 6.2 消费组与 Lag 查询

消费组一览先从每个 Broker 拉 `ListGroups` 求并集，再查询组状态、成员/Coordinator、已提交 Offset 和各分区末端 Offset。对未提交分区，Lag 标为未知或近似，不应把“0”理解为没有积压。查询本身不 `JoinGroup`、不 `OffsetCommit`，但在分区和组很多时会占用较多 Kafka 请求及后端时间。页面上的“健康”“在线”来自这些元数据和历史观测，不能代替 Broker 进程、磁盘、网络及应用端到端监控。

### 6.3 Producer 工作台

后端以 `ApiVersions → Metadata → 选分区/Leader → Produce v0` 发送。`auto` 取第一个健康分区，不是按 Key 散列分区；生成消息最多 1 万条，按最多 100 条一批发到同一选中分区。因此“发送 1 万条”时，单次 Produce 响应的**首 Offset**每批大约递增 100，最终 `lastOffset` 才覆盖全部记录；若检查其他分区，其 Offset 不一定变化。发送结果记录已确认条数、首/末 Offset、采样内容和字节数。代码把请求的 `acks=0` 提升为至少 `acks=1` 以取得响应/Offset，页面上的 `acks=0` 不能解释为真正的 fire-and-forget。

当前不是事务或幂等生产者。遇到特定错误会重试，Leader 变化会刷新 Metadata；超时或任务中途失败时，先前批次仍可能已写入，重试也可能造成重复消息。应看 Session 的已确认条数与 Kafka 实际 Offset，而不是把一次 HTTP 失败理解为全部回滚。

## 7. 独立工具：压测与客户端模拟

**压测**独立填写连接目标，可选从当前工作空间带入；以多 worker、每条一次的 `Produce v0` 循环，按条数（上限 100 万）或时长（5–3600 秒）运行，并发上限 64，Value 上限 65536 字节。它减少 Trace/HEX 开销并在内存中累计成功、失败、吞吐和延迟百分位，生成 Markdown 报告。没有持久化 Job/停止接口；服务重启会丢失任务状态，已写入 Kafka 的消息不会撤销。压测可能显著增加 Broker CPU、网络、磁盘占用和消费者积压，测试 Topic 的保留策略也决定后续存储占用。

**客户端模拟**支持 Python `confluent-kafka`、Java `kafka-clients`、Node.js `KafkaJS`、Go `franz-go`，各有两个预置版本；版本是否可用由主机中已安装运行包决定。后端最多同时启动 3 个本地子进程，配置通过子进程标准输入传入，采集标准输出/错误输出和结构化事件。生产端可以显式开启“Topic 不存在时创建”，由独立 Python AdminClient 先检查/创建；不开启时不主动发 `CreateTopics`。消费端 `commit` 默认 `false`，但订阅 Group 仍可能造成 Rebalance；设为 `true` 时由对应语言客户端自动提交。指定分区直读禁止提交，Node.js 版本仅支持消费组订阅。停止进程会先请求终止，超时 4 秒后强制结束；此前成功写入的消息或已提交位点不会回滚。

运行元数据和 JSONL 日志保存在 `KCL_CLIENT_SIM_DATA` 或数据目录下的 `client_sim_data`，单次日志上限 16 MiB，支持原始输出下载。它展示的是被启动客户端及其适配器的实际输出；`info` 级别、具体库版本及库自身日志行为决定原始日志是否丰富，不能以“日志很少”直接判定客户端未发送。运行配置快照不保存 SASL 密码和消息 Key/Value，但原始客户端输出仍可能包含业务数据，日志需要按敏感数据管理。

## 8. 只读操作的边界与故障判读

1. **指定不存在的 Topic 的 Metadata 请求**：当前多处使用 `Metadata v0/v1`，此版本没有 `allow_auto_topic_creation=false` 字段。若 Broker 开启 `auto.create.topics.enable`，按名称查询不存在的 Topic **可能**导致 Broker 自动创建；这是 Broker 行为而非 KAFLAB 显式 `CreateTopics`。全量列表查询不以具体缺失名称为目标。[Apache Kafka 协议文档](https://kafka.apache.org/protocol.html)说明从 Metadata v4 起才有该控制字段。需要严格只读环境时应在 Broker 侧关闭自动创建，或先升级平台查询协议并显式禁用。
2. **HTTP 502/503/504 不等于未生效**：请求可能已到 Kafka 而响应丢失。创建、删除、配置变更、分区/副本操作应先刷新 Kafka 元数据/配置确认，再决定是否重试。`NOT_CONTROLLER (41)` 尤其要检查 `advertised.listeners`、外部地址与 Broker 的一一映射，和 Controller 是否正在切换。
3. **“在线 Broker”是元数据视角**：当前节点离线统计结合了之前见过的 Broker ID 与分区副本分配；首次观察之前就消失且没有副本分配的 Broker 可能无法显示。集群状态显示不能替代逐节点探活。
4. **任务可部分成功**：批量生产、压测、重分配、客户端模拟都可能留下部分消息或部分分区变更。Topic 删除与副本搬迁还有异步完成阶段；需要后续核对。

## 9. 本地数据与代码索引

| 本地数据 | 当前用途 |
| --- | --- |
| `clusters.json` | 纳管集群连接及认证信息，包含明文 SASL 密码 |
| `broker_nodes.json` | 已见 Broker ID/地址，供离线推断 |
| `monitoring_samples.db` | Lag 样本及采集状态；样本保留 3 天 |
| `sessions.json`、`traces/*.json` | 工作台任务与协议追踪；Session 最多保留 100 条，已结束 Trace 文件最多保留 200 个 |
| `client_sim_data/*.json`、`*.jsonl` | 模拟进程状态和事件/原始日志 |
| `users.json` | 本地模式登录用户；容器环境认证模式改用环境变量 |

| 技术实现 | 代码位置 |
| --- | --- |
| FastAPI 初始化、后台采集生命周期 | [`backend/app/main.py`](../backend/app/main.py)、[`backend/app/lag_collector.py`](../backend/app/lag_collector.py) |
| REST 路由、鉴权、Topic 管理、总览、工作台与压测接口 | [`backend/app/routers/api.py`](../backend/app/routers/api.py) |
| 集群配置与 Broker 离线推断 | [`backend/app/cluster/manager.py`](../backend/app/cluster/manager.py)、[`nodes.py`](../backend/app/cluster/nodes.py) |
| Kafka 连接、认证、原始协议编解码 | [`backend/app/network/client.py`](../backend/app/network/client.py)、[`tcp.py`](../backend/app/network/tcp.py)、[`codec.py`](../backend/app/protocol/codec.py) |
| 生产、消费、压测 | [`backend/app/producer/engine.py`](../backend/app/producer/engine.py)、[`backend/app/consumer/engine.py`](../backend/app/consumer/engine.py)、[`backend/app/producer/loadtest.py`](../backend/app/producer/loadtest.py) |
| Topic 配置、消息浏览、监控存储 | [`backend/app/topic_config.py`](../backend/app/topic_config.py)、[`backend/app/browser/service.py`](../backend/app/browser/service.py)、[`backend/app/monitoring.py`](../backend/app/monitoring.py) |
| 模拟进程及各语言适配器 | [`backend/app/client_sim/manager.py`](../backend/app/client_sim/manager.py)、[`routes.py`](../backend/app/client_sim/routes.py)、[`backend/client_runners/`](../backend/client_runners/) |
| 页面路由与工作台 | [`frontend/src/App.tsx`](../frontend/src/App.tsx)、[`frontend/src/pages/`](../frontend/src/pages/) |

## 10. 按现有实现建议优先处理的限制

1. 将 Topic 配置保存改为 `IncrementalAlterConfigs`，并对“只改一项不丢其他覆盖配置”做集成验证。
2. 为所有 API 统一服务端鉴权；纳管集群响应隐藏 SASL 密码，用户文件改为密码哈希，并明确只读模式与 Kafka ACL 的不同职责。
3. 指定 Topic 的只读查询改用可设置 `allow_auto_topic_creation=false` 的 Metadata 版本，同时在平台 UI 中提示 Broker 自动创建策略。
4. 对重分配补进度/取消或至少明确结果回查；对压测补停止与持久化任务状态；对大规模 Lag 采集分批、限流并显示采样时间。
