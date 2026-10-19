# 可选：Docker Compose 启动

标准部署方式见 [Docker run 指南](docker.md)。如果已有 Compose 插件，也可在源码目录中启动同一镜像：

```bash
cp compose.env.example .env
# 按需修改登录用户名、密码及 KCL_PORT；缺省为 admin/admin 和 8000
mkdir -p data
docker compose up -d --build
docker compose ps
```

`compose.yaml` 会将 `./data` 挂载到容器内的 `/data`，并把宿主机 `KCL_PORT` 映射到容器的 `8000`。服务日志使用 `docker compose logs -f kaflab` 查看；`docker compose down` 不会删除 `./data`。构建源码包可运行 `python scripts/package-docker-source.py`，输出为 `release/kaflab-docker-source.tar.gz`。

可在 `.env` 中调整 Lag 采样间隔、超时及日志级别，然后重建容器。Kafka 地址、证书挂载和数据迁移规则与 [标准指南](docker.md) 相同。
