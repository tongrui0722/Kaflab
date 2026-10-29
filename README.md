# KAFLAB

Kafka 集群工作台与客户端模拟工具。浏览集群状态、管理 Topic、查看消费组与 Lag，并用独立客户端进程复现生产和消费行为。

## 功能

| 区域 | 功能 |
| --- | --- |
| 集群工作空间 | 集群概览、Broker 与 Topic 状态、Topic 创建/配置/分区操作、消费组与 Lag |
| 数据工作台 | Producer、Consumer、请求链路与协议细节 |
| 工具 | Python、Java、Node.js、Go 客户端模拟；生产压测与运行日志 |

客户端模拟和压测可以独立填写 Kafka 地址，无需进入某个集群工作空间。客户端模拟在 KAFLAB 所在主机启动独立进程。

## Docker 快速启动

在能够下载构建依赖的 Linux 主机上：

```bash
docker build -t kaflab:local .
mkdir -p "$HOME/kaflab/data"
docker run -d --name kaflab --init --restart unless-stopped \
  -p 8000:8000 \
  --mount "type=bind,source=$HOME/kaflab/data,target=/data" \
  kaflab:local
```

打开 `http://<主机 IP>:8000`。未传入账号变量时，登录账号和密码均为 `admin`。要指定账号，在 `docker run` 中加入 `-e KCL_USERNAME=operator -e KCL_PASSWORD=你的密码`。数据保存在宿主机的 `$HOME/kaflab/data`。镜像构建、跨主机迁移、证书挂载与全部环境变量见 [Docker 部署指南](docs/docker.md)。

## 本地开发

后端需要 Python 3.11+，前端需要 Node.js 20+：

```bash
cd backend
pip install -r requirements-dev.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

另开终端启动前端：

```bash
cd frontend
npm ci
npm run dev
```

访问 `http://localhost:5173`。本地首次启动会创建 `backend/app/users.json`；默认凭据为 `admin/admin`。首次建号可设置 `KCL_USERNAME` 和 `KCL_PASSWORD`；已有用户文件不会被覆盖。前端连接其他后端地址时，设置 `VITE_API_BASE`。

后端启动后立即为已保存集群采集一次 Lag，此后默认每 60 秒采集一次，每次最多等待 30 秒。采集独立于登录状态，数据保留 3 天。间隔和超时可通过 `KCL_LAG_INTERVAL_SECONDS`、`KCL_LAG_TIMEOUT_SECONDS` 设置。

### 本地客户端模拟依赖

模拟页面不会在运行时下载依赖。按需准备固定版本，然后刷新页面：

```bash
python backend/client_runners/setup.py python 2.16.0
python backend/client_runners/setup.py java 4.0.1
python backend/client_runners/setup.py node 2.2.4
python backend/client_runners/setup.py go 1.21.0
```

当前支持 Python confluent-kafka 2.13.2 / 2.16.0、Java kafka-clients 3.9.1 / 4.0.1、Node.js KafkaJS 1.16.0 / 2.2.4、Go franz-go 1.20.5 / 1.21.0。安装脚本分别需要 pip、JDK 与 Maven、npm、Go 工具链；Docker 镜像构建时会预装这些运行依赖。

## 项目目录

```text
backend/
  app/
    routers/          HTTP 与 WebSocket 路由
    cluster/          集群配置和元数据
    producer/         生产链路与压测
    consumer/         消费链路和消费组
    client_sim/       独立客户端进程管理
    protocol/         Kafka 协议编解码
    network/          连接与认证
    trace/            链路记录
  client_runners/     各语言客户端及安装脚本
  tests/              后端测试
frontend/
  src/
    pages/            页面
    components/       复用组件
    api/              接口调用
    styles/           按加载顺序分层的样式
docs/                 部署、架构和界面规范
docker/               镜像入口脚本
scripts/              源码打包脚本
```

模块边界、数据路径与接口分组见 [架构说明](docs/architecture.md)。集群操作的请求流程及影响见 [集群操作说明](docs/cluster-operations.md)。

## 检查

```bash
cd backend
python -m pytest -q
cd ../frontend
npm run build
```

Windows 目录中的源码也可用 `python scripts/package-docker-source.py` 打成 `release/kaflab-docker-source.tar.gz`，传到 Linux 构建。打包清单只包含 Docker 构建所需源码和文档，不含本地凭据、运行数据或参考项目。
