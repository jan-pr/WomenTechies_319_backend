from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import uuid

from fastapi import Body, Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from WT_ComputePool.db import Base, engine, get_db
from WT_ComputePool.models import Job, JobStatus, NodeStatus
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
    upsert_node,
)
from WT_ComputePool.services import assign_next_queued_job, monitor_cluster_state, process_stale_nodes


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
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
    availability: str = NodeStatus.IDLE.value
    metrics: NodeMetricsPayload = Field(default_factory=NodeMetricsPayload)
    carbon_intensity: float | None = None
    carbon_zone: str | None = None
    zone: str | None = None
    cpu: float | None = None


class HeartbeatPayload(BaseModel):
    status: str | None = None
    availability: str | None = None
    metrics: NodeMetricsPayload = Field(default_factory=NodeMetricsPayload)
    carbon_intensity: float | None = None
    cpu: float | None = None


class JobResultPayload(BaseModel):
    status: str
    reason: str | None = None


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
        node_record["availability"] = NodeStatus.IDLE.value
    else:
        if payload.status is not None:
            node_record["status"] = payload.status
        if payload.availability is not None:
            node_record["availability"] = payload.availability

    if payload:
        if payload.carbon_intensity is not None:
            node_record["carbon_intensity"] = payload.carbon_intensity
        if isinstance(payload, NodeRegistrationPayload):
            if payload.carbon_zone is not None:
                node_record["carbon_zone"] = payload.carbon_zone
            if payload.zone is not None:
                node_record["zone"] = payload.zone

    return node_record

@app.get("/")
def root():
    return {"message": "Backend running"}


@app.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"status": "OK", "database": "connected"}


@app.post("/register-node")
def register_node(
    node_id: str | None = None,
    payload: NodeRegistrationPayload | None = Body(default=None),
    db: Session = Depends(get_db),
):
    resolved_node_id = payload.node_id if payload else node_id
    if not resolved_node_id:
        raise HTTPException(status_code=400, detail="node_id is required")

    node = upsert_node(
        db,
        _build_node_record(node_id=resolved_node_id, payload=payload),
    )
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
        for field in ("carbon_zone", "zone"):
            if serialized_existing.get(field) and field not in node_data:
                node_data[field] = serialized_existing[field]

    node = upsert_node(db, node_data)
    db.commit()
    db.refresh(node)
    return {"message": "Heartbeat recorded", "node": serialize_node(node)}


@app.post("/submit-job")
def submit_job(task_type: str = "demo_task", db: Session = Depends(get_db)):
    job_id = str(uuid.uuid4())
    job = create_job(db, job_id=job_id, task_type=task_type)
    db.commit()
    db.refresh(job)

    return {
        "message": "Job submitted",
        "job_id": job_id,
        "task_type": task_type,
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


@app.post("/jobs/{job_id}/complete")
def complete_job(job_id: str, db: Session = Depends(get_db)):
    job = get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    assigned_node_id = job.assigned_node_id
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
