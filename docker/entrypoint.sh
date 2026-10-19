#!/bin/sh
set -eu

if [ "${KCL_DATA_DIR:-/data}" != '/data' ]; then
  echo '标准镜像的数据目录固定为 /data；请将宿主机目录或 Docker 卷挂载到 /data。' >&2
  exit 1
fi

if [ -n "${KCL_CLIENT_SIM_DATA:-}" ]; then
  case "$KCL_CLIENT_SIM_DATA" in
    /data/*) ;;
    *) echo 'KCL_CLIENT_SIM_DATA 必须位于 /data 下，以确保模拟日志持久化。' >&2; exit 1 ;;
  esac
fi

case "${KCL_LOG_LEVEL:-info}" in
  critical|error|warning|info|debug|trace) ;;
  *) echo 'KCL_LOG_LEVEL 只能为 critical、error、warning、info、debug 或 trace。' >&2; exit 1 ;;
esac

mkdir -p /data
if [ ! -w /data ]; then
  echo '数据目录 /data 不可写；请检查宿主机挂载目录的权限。' >&2
  exit 1
fi

exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1 \
  --log-level "${KCL_LOG_LEVEL:-info}"
