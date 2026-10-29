# KAFLAB 架构与目录约定

## 分层

```text
浏览器（frontend/src/pages）
  → frontend/src/api 与状态模块
  → FastAPI 路由（backend/app/routers）
  → 业务模块（cluster、producer、consumer、client_sim）
  → network / protocol → Kafka
```

前端通过 `frontend/src/main.tsx` 按固定顺序加载 `styles/` 中的样式。基础令牌和组件样式先加载，工作区样式随后加载，`ui-system.css` 最后统一字体、控件和表面层级。增加新页面样式时，优先修改相应层，避免在页面组件内复制全局规则。

| 样式文件 | 内容 |
| --- | --- |
| `foundation.css` | 设计变量、全局元素、基础卡片、表单、按钮、表格 |
| `shell.css` | 导航、登录、集群选择与公共框架 |
| `overview.css` | 概览、Lag 图与健康状态 |
| `workbench.css` | 集群、Topic、Producer 工作区 |
| `tools.css` | 客户端模拟、消费组、压测及 Consumer 布局 |
| `ui-system.css` | 最终的跨页面视觉规范与覆盖规则 |

## 后端路由

`backend/app/main.py` 统一注册路由和服务，并在应用生命周期中启动/停止 Lag 采集器和客户端模拟管理器。

| 路由模块 | 负责的路径 |
| --- | --- |
| `routers/api.py` | 登录、健康检查、集群、Topic、消费组概览、监控 |
| `routers/permissions.py` | 令牌查询与完整功能权限判断，供各路由共用 |
| `routers/workbench.py` | 会话、元数据、Producer、Consumer、Trace、独立消费组查询 |
| `routers/loadtest.py` | 压测任务启动、状态和报告 |
| `routers/stream.py` | 会话 WebSocket 流 |
| `client_sim/routes.py` | 客户端模拟进程与日志 |

路由模块只处理请求解析、权限和错误转换。Kafka 交互放在相应业务模块；网络连接与协议编码由 `network/`、`protocol/` 提供。现有集群与 Topic 路由仍集中在 `api.py`，后续改动可按业务域继续迁移，同时保持 URL 与响应格式稳定。

## 运行与持久化

- `cluster/manager.py` 保存集群连接配置；生产、消费和管理操作通过这些配置或页面单独填写的连接地址访问 Kafka。
- `lag_collector.py` 在后端启动时运行，定时拉取 Lag，并通过 `monitoring.py` 写入 SQLite。浏览器登录与否不影响采样。
- `client_sim/manager.py` 创建本机子进程，收集 stdout/stderr 与运行状态。客户端模拟使用 `client_runners/` 中固定版本的语言客户端。
- Docker 把数据统一挂载到 `/data`；本地开发时数据保存在 `backend/` 下。源码包不包含凭据、Trace 或模拟日志。

## 部署入口

`Dockerfile` 是标准镜像入口：分别构建前端和固定版本的客户端，最后把产物复制到 Python 运行镜像。目标环境使用单个 `docker run` 启动；`docker/entrypoint.sh` 负责读取环境变量并启动服务。详细命令见 [Docker 部署指南](docker.md)。

`scripts/package-docker-source.py` 维护离线转运所需的源码清单。新增 Docker 构建必需文件时，同步更新清单并检查 `.dockerignore`。第三方项目放在 `reference-projects/` 仅供对照，已从 Git 和 Docker 构建上下文排除。

## 参考项目

目录组织和 README 导航参考了 [KafkaVista](https://github.com/suwtyunwei/kafkaVista) 的公开仓库。KAFLAB 保留现有 Python/FastAPI 与 React 实现，以及单容器 `docker run` 部署方式；未引入参考项目的运行代码。
