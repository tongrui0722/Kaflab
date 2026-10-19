# KAFLAB 标准镜像部署

镜像包含前端、FastAPI 后端及 Python、Java、Node.js、Go 客户端模拟器。运行时宿主机只需要 Docker。容器内 HTTP 端口固定为 `8000`，所有持久化数据统一在 `/data`；后端固定单工作进程，避免重复运行 Lag 采集任务。

## 构建一次，在各环境载入

在可访问 PyPI、npm、Maven Central 和 Go 模块仓库的 Linux 主机构建，并导出镜像：

```bash
docker build -t kaflab:local .
docker save -o kaflab-image.tar kaflab:local
```

若构建日志在 Go 阶段显示 `proxy.golang.org` 连接失败，可给构建容器指定可访问的 Go 模块代理，例如：

```bash
docker build --build-arg GOPROXY=https://goproxy.cn,direct -t kaflab:local .
```

也可以将 `GOPROXY` 改为环境内可访问的内部模块代理。它只影响构建阶段的 Go 依赖下载，不是 `docker run -e` 参数。Go 的 `GOPROXY` 用法见 [Go 模块参考](https://go.dev/ref/mod#environment-variables)。

Windows 项目可先运行 `python scripts/package-docker-source.py`，得到 `release/kaflab-docker-source.tar.gz`；将源码包传到 Linux 构建主机并解压。源码包不包含集群凭据、运行数据或本地虚拟环境。每台目标 Linux 主机只需执行 `docker load -i kaflab-image.tar`，不必再次构建。构建主机与目标主机应使用相同 CPU 架构。

### 更新已有容器

源码改动不会自动进入已构建的镜像，重新登录也不会更新容器中的前后端。先把最新源码包解压到新的构建目录，构建新标签：

```bash
mkdir -p "$HOME/kaflab-build"
tar -xzf kaflab-docker-source.tar.gz -C "$HOME/kaflab-build"
cd "$HOME/kaflab-build"
docker build -t kaflab:updated .
```

重建容器前，核对旧容器的 `/data` 挂载及原有 `-e` 参数。尤其当 `/data` 是 Docker 自动创建的匿名卷时，新容器必须显式复用同一个卷，才能继续看到原来的集群和监控数据：

```bash
docker inspect kaflab --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Type}} {{.Name}} {{.Source}}{{end}}{{end}}'
```

确认数据挂载后，执行 `docker stop kaflab && docker rm kaflab`，再按下文原有的 `docker run` 命令重新创建容器，保留相同的端口、`-e` 和 `/data` 挂载参数，只把末尾镜像名改为 `kaflab:updated`。不要使用 `docker rm -v`。最后在浏览器强制刷新页面；新建集群表单应显示“自动采集 Lag”开关。

## 每个环境直接 docker run

在各主机准备独立的宿主机数据目录。常用配置直接通过 `-e` 传入：

```bash
mkdir -p "$HOME/kaflab/data"
docker run -d --name kaflab --init --restart unless-stopped \
  -p 8000:8000 \
  -e KCL_USERNAME=operator \
  -e KCL_PASSWORD='change-me' \
  -e KCL_DATA_DIR=/data \
  -e KCL_LAG_INTERVAL_SECONDS=60 \
  -e KCL_LAG_TIMEOUT_SECONDS=30 \
  -e KCL_LOG_LEVEL=info \
  --mount "type=bind,source=$HOME/kaflab/data,target=/data" \
  kaflab:local
```

不传 `KCL_USERNAME` 和 `KCL_PASSWORD` 时，登录凭据为 `admin/admin`；上例则为 `operator/change-me`。只传其中一项时，另一项仍取默认值 `admin`。每次 `docker run` 都以当前传入的变量为准，即使挂载的数据目录已有 `users.json`。访问 `http://<主机 IP>:8000`；用 `docker logs -f kaflab` 查看服务日志，或用 `curl http://127.0.0.1:8000/api/health` 检查接口。停止并移除容器用 `docker stop kaflab && docker rm kaflab`；挂载的数据目录不会删除。更新镜像后，用同一条 `docker run` 命令重建容器即可。

| 环境变量 | 默认/模板值 | 作用 |
| --- | --- | --- |
| `KCL_USERNAME` | `admin` | 当前容器的登录用户名 |
| `KCL_PASSWORD` | `admin` | 当前容器的登录密码 |
| `KCL_DATA_DIR` | `/data` | 容器内持久化目录；标准镜像要求保持 `/data` |
| `KCL_LAG_INTERVAL_SECONDS` | `60` | Lag 采样间隔，允许 10–3600 秒 |
| `KCL_LAG_TIMEOUT_SECONDS` | `30` | 单集群采样超时，允许 1–300 秒 |
| `KCL_LOG_LEVEL` | `info` | `critical`、`error`、`warning`、`info`、`debug`、`trace` |

`KCL_CLIENT_SIM_DATA` 可以不设，默认自动使用 `/data/client_sim_data`。若覆盖，必须位于 `/data` 下。Docker 镜像始终使用环境变量进行登录验证，不会读取或修改已挂载的 `users.json`。该文件只供非 Docker 的本地运行模式使用。

`-e KCL_DATA_DIR=/data` 指的是**容器内**路径，标准镜像固定使用 `/data`。宿主机的数据目录和端口分别由 `--mount source=`、`-p` 指定，不能仅靠 `-e` 更改。上面的命令在各主机使用统一的 `$HOME/kaflab/data` 和端口 `8000`。同一主机运行多个实例时，需为容器名、端口和数据目录分别命名。也可以把挂载参数换成 `--mount type=volume,source=kaflab-data,target=/data`，由 Docker 管理数据卷。

## 数据迁移与备份

集群配置、Broker 记录、会话、Trace、客户端模拟日志及 Lag 历史都会写入 `/data`。Docker 登录凭据由环境变量提供，不写入数据目录。首次启动且目录为空时不会加入开发环境的示例集群。迁移当前本地数据时，先停止旧服务，再把实际存在的内容复制到宿主机数据目录：

| 当前项目路径 | `/data` 下的路径 |
| --- | --- |
| `backend/clusters.json` | `clusters.json` |
| `backend/broker_nodes.json` | `broker_nodes.json` |
| `backend/sessions.json` | `sessions.json` |
| `backend/monitoring_samples.db` | `monitoring_samples.db` |
| `backend/traces/` | `traces/` |
| `backend/client_sim_data/` | `client_sim_data/` |

备份时复制整个宿主机数据目录。请确认容器能读写，并限制其他系统用户读取其中的 Kafka 凭据和日志。

## Kafka 地址与证书

页面中的 Bootstrap 地址和 Kafka 返回的 `advertised.listeners` 地址都必须能从容器内部访问；容器内的 `localhost` 指容器自身。Kafka 位于同一 Linux 主机时，可给 `docker run` 增加 `--add-host=host.docker.internal:host-gateway`，再使用主机可达 IP 或 `host.docker.internal`。连接其他主机时不需要此参数。

自签 CA 文件需要另加只读挂载，例如 `--mount "type=bind,source=$HOME/kaflab/certs,target=/certs,readonly"`，然后在页面填写容器路径 `/certs/ca.pem`。

仓库还保留可选的 [Compose 部署指南](docker-compose.md)。Compose 和直接 `docker run` 选择一种即可，避免两个容器占用同一宿主机端口。

## 基础镜像拉取失败

若 `docker build` 在 `load metadata` 阶段报错，且地址为 `docker.mirrors.tuna.tsinghua.edu.cn`，问题在 Docker daemon 配置的镜像源或 DNS，尚未执行到本项目的构建步骤。清华镜像站的 [Docker CE 页面](https://mirrors.tuna.tsinghua.edu.cn/help/docker-ce/)说明该站提供 Docker 软件包，并非 Docker Hub 镜像服务。

先在 Linux 构建主机上查看实际生效的镜像源和 daemon 配置：

```bash
docker info | sed -n '/Registry Mirrors:/,/Live Restore Enabled:/p'
sudo cat /etc/docker/daemon.json
```

从 `registry-mirrors` 数组中移除失效的 `https://docker.mirrors.tuna.tsinghua.edu.cn`，保留配置文件的其他字段和有效 JSON 格式。然后验证配置、重启 Docker，并单独测试基础镜像：

```bash
sudo dockerd --validate --config-file=/etc/docker/daemon.json
sudo systemctl restart docker
docker pull python:3.11-slim-bookworm
docker build -t kaflab:local .
```

若移除镜像源后仍无法拉取，则确认构建主机能否访问 Docker Hub；需要代理时按 [Docker daemon 代理配置](https://docs.docker.com/engine/daemon/proxy/)设置可从 **Linux 构建主机**访问的代理地址。另一种方式是在可拉取基础镜像的主机构建，再用 `docker save` / `docker load` 分发成品镜像。仅在 `docker run` 中添加 `-e` 不能解决基础镜像拉取问题。
