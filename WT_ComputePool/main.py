from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import os
import traceback
import uuid

from fastapi import Body, Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from scheduler.carbon import fetch_carbon_intensity
from WT_ComputePool.db import get_database_debug_info, get_db, initialize_database
from WT_ComputePool.models import JobStatus, NodeStatus
from WT_ComputePool.repository import (
    clear_nonterminal_jobs,
    create_job,
    get_job,
    get_node,
    list_jobs,
    list_nodes,
    mark_job_completed,
    mark_job_failed,
    mark_node_status,
    requeue_job,
    serialize_job,
    serialize_node,
    update_job_progress,
    upsert_node,
)
from WT_ComputePool.services import (
    assign_next_queued_job,
    monitor_cluster_state,
    poll_job_for_node,
    process_stale_nodes,
)


CARBON_API_KEY = os.getenv("ELECTRICITY_MAPS_API_KEY")


@asynccontextmanager
async def lifespan(_: FastAPI):
    initialize_database()
    monitor_task = asyncio.create_task(monitor_cluster_state())
    try:
        yield
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


class NodeMetricsPayload(BaseModel):
    success_rate: float | None = None
    uptime: float | None = None
    speed: float | None = None


class NodeRegistrationPayload(BaseModel):
    node_id: str
    status: str = NodeStatus.IDLE.value
    metrics: NodeMetricsPayload = Field(default_factory=NodeMetricsPayload)
    carbon_intensity: float | None = None
    carbon_zone: str | None = None
    cpu: float | None = None
    current_job_id: str | None = None


class HeartbeatPayload(BaseModel):
    status: str | None = None
    metrics: NodeMetricsPayload = Field(default_factory=NodeMetricsPayload)
    carbon_intensity: float | None = None
    carbon_zone: str | None = None
    cpu: float | None = None
    current_job_id: str | None = None


class JobResultPayload(BaseModel):
    status: str
    reason: str | None = None


class JobSubmissionPayload(BaseModel):
    task_type: str = "demo_task"
    payload: dict[str, object] = Field(default_factory=dict)


class JobProgressPayload(BaseModel):
    progress: int


class JobCompletionPayload(BaseModel):
    result: dict[str, object] | None = None


def _build_node_record(
    node_id: str,
    payload: NodeRegistrationPayload | HeartbeatPayload | None = None,
) -> dict:
    metrics = payload.metrics.model_dump(exclude_none=True) if payload else {}

    node_record = {
        "id": node_id,
        "metrics": metrics,
        "cpu": payload.cpu if payload else None,
        "last_seen": datetime.utcnow().isoformat(),
    }

    if payload is None:
        node_record["status"] = NodeStatus.IDLE.value
    else:
        if payload.status is not None:
            node_record["status"] = payload.status

    if payload:
        if payload.carbon_intensity is not None:
            node_record["carbon_intensity"] = payload.carbon_intensity
        if payload.carbon_zone is not None:
            node_record["carbon_zone"] = payload.carbon_zone
        if payload.current_job_id is not None:
            node_record["current_job_id"] = payload.current_job_id

    return node_record


def _enrich_carbon_intensity(node_record: dict) -> dict:
    carbon_intensity = node_record.get("carbon_intensity")
    if carbon_intensity is not None and float(carbon_intensity) > 0:
        return node_record

    carbon_zone = node_record.get("carbon_zone")
    if not carbon_zone:
        return node_record
    if not CARBON_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="ELECTRICITY_MAPS_API_KEY is not configured",
        )

    try:
        node_record["carbon_intensity"] = fetch_carbon_intensity(
            zone=str(carbon_zone),
            api_key=CARBON_API_KEY,
        )
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail=f"failed to fetch carbon intensity for zone '{carbon_zone}'",
        ) from exc

    return node_record


@app.get("/")
def root():
    return {"message": "Backend running"}


@app.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {
        "status": "OK",
        "database": "connected",
        "database_debug": get_database_debug_info(),
    }


@app.post("/register-node")
def register_node(
    node_id: str | None = None,
    payload: NodeRegistrationPayload | None = Body(default=None),
    db: Session = Depends(get_db),
):
    resolved_node_id = payload.node_id if payload else node_id
    if not resolved_node_id:
        raise HTTPException(status_code=400, detail="node_id is required")

    node_data = _enrich_carbon_intensity(
        _build_node_record(node_id=resolved_node_id, payload=payload),
    )
    node = upsert_node(db, node_data)
    db.commit()
    db.refresh(node)

    return {
        "message": f"Node {resolved_node_id} registered",
        "node": serialize_node(node),
    }


@app.get("/debug")
def debug(db: Session = Depends(get_db)):
    jobs = list_jobs(db)
    return {
        "database_debug": get_database_debug_info(),
        "nodes": [serialize_node(node) for node in list_nodes(db)],
        "jobs": [serialize_job(job) for job in jobs],
        "queue": [
            serialize_job(job) for job in jobs if job.status == JobStatus.QUEUED.value
        ],
    }


@app.get("/nodes")
def get_nodes(db: Session = Depends(get_db)):
    return {node.id: serialize_node(node) for node in list_nodes(db)}


@app.post("/nodes/{node_id}/heartbeat")
def heartbeat(node_id: str, payload: HeartbeatPayload, db: Session = Depends(get_db)):
    existing_node = get_node(db, node_id)
    node_data = _build_node_record(node_id=node_id, payload=payload)
    if existing_node is not None:
        serialized_existing = serialize_node(existing_node)
        if serialized_existing.get("carbon_zone") and "carbon_zone" not in node_data:
            node_data["carbon_zone"] = serialized_existing["carbon_zone"]

    node_data = _enrich_carbon_intensity(node_data)
    node = upsert_node(db, node_data)
    db.commit()
    db.refresh(node)
    return {"message": "Heartbeat recorded", "node": serialize_node(node)}


@app.post("/submit-job")
def submit_job(
    task_type: str = "demo_task",
    payload: JobSubmissionPayload | None = Body(default=None),
    db: Session = Depends(get_db),
):
    job_id = str(uuid.uuid4())
    resolved_task_type = payload.task_type if payload else task_type
    resolved_payload = payload.payload if payload else {}
    job = create_job(
        db,
        job_id=job_id,
        task_type=resolved_task_type,
        payload=resolved_payload,
    )
    db.commit()
    db.refresh(job)

    return {
        "message": "Job submitted",
        "job_id": job_id,
        "task_type": resolved_task_type,
        "job": serialize_job(job),
    }


@app.get("/jobs")
def get_jobs(db: Session = Depends(get_db)):
    return [serialize_job(job) for job in list_jobs(db)]


@app.post("/assign-job")
def assign_job(db: Session = Depends(get_db)):
    result = assign_next_queued_job(db)
    if not result["assigned"]:
        return result

    db.commit()
    return result


@app.post("/nodes/{node_id}/poll-job")
def poll_job(node_id: str, db: Session = Depends(get_db)):
    print(f"[poll] polling job for node {node_id}")
    try:
        node = get_node(db, node_id)
        if node is None:
            print("[poll] node not found")
            return {"job": None}

        if node.status == NodeStatus.OFFLINE.value:
            print("[poll] node offline")
            return {"job": None}

        job = poll_job_for_node(db, node_id=node_id)
        if not job:
            print("[poll] no job available")
            return {"job": None}

        print(f"[poll] job assigned: {job['id']}")
        db.commit()
        return {"job": job}
    except Exception as exc:
        db.rollback()
        print(f"[poll] ERROR for node {node_id}: {exc}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="failed to poll job") from exc


@app.post("/jobs/{job_id}/progress")
def progress_job(
    job_id: str,
    payload: JobProgressPayload,
    db: Session = Depends(get_db),
):
    job = get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status not in {JobStatus.ASSIGNED.value, JobStatus.RUNNING.value}:
        raise HTTPException(
            status_code=409,
            detail="job must be assigned or running before progress can be reported",
        )

    update_job_progress(db, job, payload.progress)
    db.commit()
    db.refresh(job)
    return {"message": "Progress updated", "job": serialize_job(job)}


@app.post("/jobs/{job_id}/complete")
def complete_job(
    job_id: str,
    payload: JobCompletionPayload | None = Body(default=None),
    db: Session = Depends(get_db),
):
    job = get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status not in {JobStatus.ASSIGNED.value, JobStatus.RUNNING.value}:
        raise HTTPException(
            status_code=409,
            detail="job must be assigned or running before completion",
        )

    assigned_node_id = job.assigned_node_id
    if payload is not None:
        job.result = payload.result
    mark_job_completed(db, job)
    if assigned_node_id:
        mark_node_status(db, node_id=assigned_node_id, status=NodeStatus.IDLE.value)

    db.commit()
    db.refresh(job)
    return {"message": "Job completed", "job": serialize_job(job)}


@app.post("/jobs/{job_id}/fail")
def fail_job(job_id: str, payload: JobResultPayload, db: Session = Depends(get_db)):
    job = get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if payload.status not in {JobStatus.FAILED.value, JobStatus.QUEUED.value}:
        raise HTTPException(
            status_code=400,
            detail="status must be either 'failed' or 'queued'",
        )
    if job.status not in {JobStatus.ASSIGNED.value, JobStatus.RUNNING.value}:
        raise HTTPException(
            status_code=409,
            detail="job must be assigned or running before failure can be reported",
        )

    assigned_node_id = job.assigned_node_id
    reason = payload.reason or "job execution failed"
    if payload.status == JobStatus.FAILED.value:
        mark_job_failed(db, job, reason=reason)
    else:
        requeue_job(db, job, reason=reason)

    if assigned_node_id:
        mark_node_status(db, node_id=assigned_node_id, status=NodeStatus.IDLE.value)

    db.commit()
    db.refresh(job)
    return {"message": "Job updated", "job": serialize_job(job)}


@app.post("/scheduler/reconcile")
def reconcile_cluster(db: Session = Depends(get_db)):
    result = process_stale_nodes(db)
    db.commit()
    return result


@app.post("/debug/clear-nonterminal-jobs")
def clear_active_jobs(db: Session = Depends(get_db)):
    result = clear_nonterminal_jobs(db)
    db.commit()
    return result
