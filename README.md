# KAFLAB

Kafka 集群工作台与客户端模拟工具。工作台用于查看集群、Topic、消费组及原有 Producer/Consumer 功能；工具区的**生产端模拟**和**消费端模拟**会在 KAFLAB 主机启动独立的本地客户端进程，连接到页面指定的 Kafka 地址。模拟工具不依赖当前选中的工作集群，也可一键带入其连接配置。

## 启动

后端需要 Python 3.11+：

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

前端需要 Node.js：

```bash
cd frontend
npm install
npm run dev
```

打开 `http://localhost:5173`。首次登录可使用后端生成的 `backend/app/users.json` 中的账号；未设置 `KCL_USERNAME` 和 `KCL_PASSWORD` 时初始账号为 `admin/admin`，也可通过这两个环境变量指定首次建号凭据。已有账号文件不会被环境变量覆盖。前端如需访问其他地址的后端，设置 `VITE_API_BASE`。

后端启动后会立即对所有已保存集群采集一次消费组 Lag，此后每 60 秒采集一次；每个集群单次采集最多 30 秒。采样不依赖用户登录或浏览器页面，历史记录保存在 `backend/monitoring_samples.db`，保留 3 天。集群总览登录后从该历史库读取曲线；采集失败会保留已有数据并显示最近一次采集错误。停止后端后采样也会停止。

## 客户端模拟

运行页面不会下载依赖。先在 KAFLAB 主机准备所需的固定版本，再刷新页面选择已就绪的版本：

```bash
python backend/client_runners/setup.py python 2.16.0
python backend/client_runners/setup.py java 4.0.1
python backend/client_runners/setup.py node 2.2.4
python backend/client_runners/setup.py go 1.21.0
```

目前每种客户端提供两个版本：Python confluent-kafka `2.13.2`、`2.16.0`；Java kafka-clients `3.9.1`、`4.0.1`；Node.js KafkaJS `1.16.0`、`2.2.4`；Go franz-go `1.20.5`、`1.21.0`。Python 安装脚本需要 pip，Java 需要 JDK 与 Maven，Node.js 需要 npm，Go 需要 Go 工具链。构建后的 Go 程序运行时不依赖 Go 工具链。

两个模拟页支持独立配置 Bootstrap Servers、Topic、PLAINTEXT/SSL/SASL、安全认证和 CA 文件；生产端可设消息模板、分区、数量、速率、时长；消费端可设消费组、起始位置、消息数量、时长，默认**不提交 Offset**。每次运行有独立 PID、状态、实时日志和历史记录；同一主机最多并发 3 个模拟进程。停止按钮会终止对应进程。只读模式可查看日志，启动和停止需要切换到“全部功能”。

运行记录保存在 `backend/client_sim_data/`（可通过 `KCL_CLIENT_SIM_DATA` 覆盖）。记录不会保存消息内容或 SASL 密码；日志最多保留 4 MB，密码会被脱敏。连接凭据只在启动时通过进程标准输入传入。请为模拟器使用专用测试 Topic，并按需控制生产条数与提交 Offset。

## 验证

```bash
cd backend
python -m pytest -q
cd ../frontend
npm run build
```

当前版本的模拟进程运行在 KAFLAB 所在主机。Linux Docker 镜像内已包含运行依赖；各环境可通过 `docker run -e` 传入常用配置，并将持久化数据挂载到 `/data`。构建、迁移及启动命令见 [Docker 部署指南](docs/docker.md)。
