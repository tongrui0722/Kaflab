from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from .permissions import _need_lab

router = APIRouter()


# ---- 压测（独立工具：轻量 Produce 循环 + 聚合统计 + Markdown 报告） ----
_load_jobs: dict = {}

async def _load_worker(job_id: str, request: Request, body: dict):
    from ..producer.loadtest import run_load_job
    job = _load_jobs[job_id]
    try:
        await run_load_job(job, body)
    except Exception as e:  # noqa: BLE001
        job.update({"status": "failed", "error": str(e)[:300]})


@router.post("/api/loadtest/run")
async def loadtest_run(body: dict, background: BackgroundTasks, request: Request):
    _need_lab(request)
    import time as _t
    import uuid as _u
    mode = body.get("mode", "count") if body.get("mode") in ("count", "duration") else "count"
    total = max(1, min(1_000_000, int(body.get("total", 100))))
    duration_sec = max(5, min(3600, float(body.get("durationSec", 60))))
    conc = max(1, min(64, int(body.get("concurrency", 4))))
    value_size = max(1, min(65536, int(body.get("valueSize", 128))))
    job_id = f"load-{_u.uuid4().hex[:8]}"
    _load_jobs[job_id] = {
        "id": job_id, "status": "running", "mode": mode,
        "total": total if mode == "count" else None,
        "durationTarget": duration_sec if mode == "duration" else None,
        "concurrency": conc, "valueSize": value_size, "done": 0,
        "topic": body.get("topic", "test-topic"),
        "bootstrap": body.get("bootstrap", ""),
        "startedAt": int(_t.time() * 1000),
    }
    payload = {
        "bootstrap": body.get("bootstrap", ""),
        "topic": body.get("topic", "test-topic"),
        "partition": body.get("partition", "auto"),
        "acks": body.get("acks", "all"),
        "retries": int(body.get("retries", 1)),
        "requestTimeoutMs": int(body.get("requestTimeoutMs", 10000)),
        "deliveryTimeoutMs": int(body.get("deliveryTimeoutMs", 30000)),
        "securityProtocol": body.get("securityProtocol", "PLAINTEXT"),
        "saslMechanism": body.get("saslMechanism", "PLAIN"),
        "saslUsername": body.get("saslUsername", ""),
        "saslPassword": body.get("saslPassword", ""),
        "caPath": body.get("caPath", ""),
        "mode": mode, "total": total, "durationSec": duration_sec,
        "concurrency": conc, "valueSize": value_size,
    }
    background.add_task(_load_worker, job_id, request, payload)
    return {"jobId": job_id, "total": total if mode == "count" else None}


@router.get("/api/loadtest/{job_id}")
async def loadtest_status(job_id: str):
    job = _load_jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job


@router.get("/api/loadtest/{job_id}/report")
async def loadtest_report(job_id: str):
    from ..producer.loadtest import build_report
    job = _load_jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job.get("status") != "done":
        raise HTTPException(409, "job not finished")
    return {"markdown": build_report(job)}


