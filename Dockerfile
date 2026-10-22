# Build the UI and the pinned Kafka client simulators for the Linux image.
FROM node:20-bookworm-slim AS node-build
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build
RUN npm install --prefix /out/node/1.16.0 --no-save --ignore-scripts kafkajs@1.16.0 \
 && npm install --prefix /out/node/2.2.4 --no-save --ignore-scripts kafkajs@2.2.4

FROM maven:3.9-eclipse-temurin-17 AS java-build
WORKDIR /build/java
COPY backend/client_runners/java/pom.xml ./
COPY backend/client_runners/java/src/ ./src/
RUN mvn -q -Dkafka.version=3.9.1 -DskipTests clean package \
 && mkdir -p /out/java/3.9.1 \
 && cp target/client-sim-1.0.0.jar /out/java/3.9.1/client-sim.jar \
 && mvn -q -Dkafka.version=4.0.1 -DskipTests clean package \
 && mkdir -p /out/java/4.0.1 \
 && cp target/client-sim-1.0.0.jar /out/java/4.0.1/client-sim.jar

FROM golang:1.25-bookworm AS go-build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /build/go
COPY backend/client_runners/go/go.mod backend/client_runners/go/main.go ./
RUN mkdir -p /out/go/1.20.5 /out/go/1.21.0 \
 && go mod edit -require=github.com/twmb/franz-go@v1.20.5 \
 && go mod tidy \
 && CGO_ENABLED=0 go build -ldflags='-X main.clientVersion=1.20.5' -o /out/go/1.20.5/client-sim . \
 && go mod edit -require=github.com/twmb/franz-go@v1.21.0 \
 && go mod tidy \
 && CGO_ENABLED=0 go build -ldflags='-X main.clientVersion=1.21.0' -o /out/go/1.21.0/client-sim .

FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    KCL_DATA_DIR=/data \
    KCL_AUTH_SOURCE=environment \
    KCL_STATIC_DIR=/app/frontend/dist \
    KCL_LAG_INTERVAL_SECONDS=60 \
    KCL_LAG_TIMEOUT_SECONDS=30 \
    KCL_LOG_LEVEL=info
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates nodejs openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app/backend
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/app/ ./app/
COPY backend/client_runners/python_client.py backend/client_runners/node_client.cjs backend/client_runners/topic_admin.py ./client_runners/
COPY docker/entrypoint.sh /app/docker-entrypoint.sh
COPY --from=node-build /build/frontend/dist/ /app/frontend/dist/
COPY --from=node-build /out/node/ ./client_runners/installed/node/
COPY --from=java-build /out/java/ ./client_runners/installed/java/
COPY --from=go-build /out/go/ ./client_runners/installed/go/
RUN python -m venv client_runners/installed/python/2.13.2 \
 && client_runners/installed/python/2.13.2/bin/pip install --no-cache-dir confluent-kafka==2.13.2 \
 && python -m venv client_runners/installed/python/2.16.0 \
 && client_runners/installed/python/2.16.0/bin/pip install --no-cache-dir confluent-kafka==2.16.0 \
 && mkdir -p /data
VOLUME ["/data"]
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3)" || exit 1
ENTRYPOINT ["sh", "/app/docker-entrypoint.sh"]
