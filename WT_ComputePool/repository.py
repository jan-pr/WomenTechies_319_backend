from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from WT_ComputePool.models import Job, JobEvent, JobStatus, Node, NodeStatus


def serialize_node(node: Node) -> dict[str, Any]:
    return {
        "id": node.id,
        "status": node.status,
        "availability": node.status,
        "metrics": {
            key: value
            for key, value in {
                "success_rate": node.success_rate,
                "uptime": node.uptime,
                "speed": node.speed,
            }.items()
            if value is not None
        },
        "carbon_intensity": node.carbon_intensity,
        "carbon_zone": node.carbon_zone,
        "zone": node.carbon_zone,
        "cpu": node.cpu,
        "current_job_id": node.current_job_id,
        "last_seen": node.last_seen.isoformat() if node.last_seen else None,
    }


def serialize_job(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "task_type": job.task_type,
        "payload": job.payload or {},
        "progress": job.progress,
        "result": job.result,
        "status": job.status,
        "assigned_node": job.assigned_node_id,
        "failure_reason": job.failure_reason,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "submitted_at": job.submitted_at.isoformat() if job.submitted_at else None,
        "queued_at": job.queued_at.isoformat() if job.queued_at else None,
        "assigned_at": job.assigned_at.isoformat() if job.assigned_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


def log_job_event(
    db: Session,
    job_id: str,
    event_type: str,
    message: str | None = None,
    payload: dict[str, Any] | None = None,
) -> JobEvent:
    event = JobEvent(
        job_id=job_id,
        event_type=event_type,
        message=message,
        payload=payload,
    )
    db.add(event)
    return event


def get_node(db: Session, node_id: str) -> Node | None:
    return db.get(Node, node_id)


def upsert_node(db: Session, node_data: dict[str, Any]) -> Node:
    node = db.get(Node, node_data["id"])
    if node is None:
        node = Node(id=node_data["id"])
        db.add(node)

    metrics = node_data.get("metrics", {})
    incoming_status = node_data.get("status")
    node.status = incoming_status or node.status or NodeStatus.IDLE.value
    node.success_rate = metrics.get("success_rate", node.success_rate)
    node.uptime = metrics.get("uptime", node.uptime)
    node.speed = metrics.get("speed", node.speed)
    node.carbon_intensity = node_data.get("carbon_intensity", node.carbon_intensity)
    carbon_zone = node_data.get("carbon_zone")
    if carbon_zone is not None:
        node.carbon_zone = carbon_zone
    node.cpu = node_data.get("cpu", node.cpu)
    if "current_job_id" in node_data:
        node.current_job_id = node_data["current_job_id"]
    elif incoming_status is not None and incoming_status != NodeStatus.BUSY.value:
        node.current_job_id = None
    node.last_seen = datetime.utcnow()
    return node


def list_nodes(db: Session) -> list[Node]:
    return list(db.scalars(select(Node).order_by(Node.id)))


def list_stale_nodes(db: Session, stale_after_seconds: int) -> list[Node]:
    cutoff = datetime.utcnow() - timedelta(seconds=stale_after_seconds)
    statement = (
        select(Node)
        .where(Node.last_seen < cutoff)
        .where(Node.status != NodeStatus.OFFLINE.value)
        .order_by(Node.last_seen.asc())
    )
    return list(db.scalars(statement))


def create_job(
    db: Session,
    job_id: str,
    task_type: str,
    payload: dict[str, Any] | None = None,
) -> Job:
    now = datetime.utcnow()
    job = Job(
        id=job_id,
        task_type=task_type,
        payload=payload or {},
        progress=0,
        status=JobStatus.QUEUED.value,
        submitted_at=now,
        queued_at=now,
    )
    db.add(job)
    log_job_event(db, job_id=job_id, event_type=JobStatus.SUBMITTED.value)
    log_job_event(db, job_id=job_id, event_type=JobStatus.QUEUED.value)
    return job


def list_jobs(db: Session) -> list[Job]:
    jobs = list(db.scalars(select(Job).order_by(Job.created_at.desc())))
    print(
        "[db] fetched jobs="
        f"{[(job.id, job.status, job.assigned_node_id) for job in jobs]}"
    )
    return jobs


def get_job(db: Session, job_id: str) -> Job | None:
    return db.get(Job, job_id)


def get_active_job_for_node(db: Session, node_id: str) -> Job | None:
    statement = (
        select(Job)
        .where(Job.assigned_node_id == node_id)
        .where(Job.status.in_([JobStatus.ASSIGNED.value, JobStatus.RUNNING.value]))
        .order_by(Job.assigned_at.desc(), Job.started_at.desc(), Job.created_at.desc())
    )
    return db.scalar(statement)


def list_nonterminal_jobs(db: Session) -> list[Job]:
    statement = (
        select(Job)
        .where(Job.status.in_([JobStatus.QUEUED.value, JobStatus.ASSIGNED.value, JobStatus.RUNNING.value]))
        .order_by(Job.created_at.asc())
    )
    return list(db.scalars(statement))


def get_next_queued_job(db: Session) -> Job | None:
    statement = (
        select(Job)
        .where(Job.status == JobStatus.QUEUED.value)
        .order_by(Job.queued_at.asc(), Job.created_at.asc())
    )
    return db.scalar(statement)


def list_reassignable_jobs_for_node(db: Session, node_id: str) -> list[Job]:
    statement = (
        select(Job)
        .where(Job.assigned_node_id == node_id)
        .where(Job.status.in_([JobStatus.ASSIGNED.value, JobStatus.RUNNING.value]))
        .order_by(Job.assigned_at.asc(), Job.started_at.asc(), Job.created_at.asc())
    )
    return list(db.scalars(statement))


def node_has_active_jobs(
    db: Session,
    node_id: str,
    exclude_job_id: str | None = None,
) -> bool:
    statement = (
        select(Job)
        .where(Job.assigned_node_id == node_id)
        .where(Job.status.in_([JobStatus.ASSIGNED.value, JobStatus.RUNNING.value]))
        .order_by(Job.created_at.asc())
    )
    jobs = list(db.scalars(statement))
    if exclude_job_id is not None:
        jobs = [job for job in jobs if job.id != exclude_job_id]
    return bool(jobs)


def mark_job_assigned_and_running(
    db: Session,
    job: Job,
    node_id: str,
    scores: dict[str, Any] | None = None,
) -> Job:
    now = datetime.utcnow()
    job.assigned_node_id = node_id
    job.status = JobStatus.RUNNING.value
    job.progress = 0
    job.assigned_at = now
    job.started_at = now
    job.failure_reason = None
    log_job_event(
        db,
        job_id=job.id,
        event_type=JobStatus.ASSIGNED.value,
        payload={"node_id": node_id, "scores": scores or {}},
    )
    log_job_event(
        db,
        job_id=job.id,
        event_type=JobStatus.RUNNING.value,
        payload={"node_id": node_id},
    )
    return job


def mark_job_completed(db: Session, job: Job) -> Job:
    now = datetime.utcnow()
    job.status = JobStatus.COMPLETED.value
    job.progress = 100
    job.completed_at = now
    log_job_event(db, job_id=job.id, event_type=JobStatus.COMPLETED.value)
    return job


def mark_job_failed(db: Session, job: Job, reason: str) -> Job:
    now = datetime.utcnow()
    job.status = JobStatus.FAILED.value
    job.failure_reason = reason
    job.completed_at = now
    log_job_event(
        db,
        job_id=job.id,
        event_type=JobStatus.FAILED.value,
        message=reason,
    )
    return job


def update_job_progress(db: Session, job: Job, progress: int) -> Job:
    bounded_progress = max(0, min(100, int(progress)))
    job.progress = bounded_progress
    if bounded_progress > 0 and job.started_at is None:
        job.started_at = datetime.utcnow()
    if job.status == JobStatus.ASSIGNED.value:
        job.status = JobStatus.RUNNING.value
    log_job_event(
        db,
        job_id=job.id,
        event_type="progress",
        payload={"progress": bounded_progress},
    )
    return job


def requeue_job(db: Session, job: Job, reason: str) -> Job:
    now = datetime.utcnow()
    job.status = JobStatus.QUEUED.value
    job.failure_reason = reason
    job.assigned_node_id = None
    job.progress = 0
    job.result = None
    job.queued_at = now
    job.assigned_at = None
    job.started_at = None
    job.completed_at = None
    log_job_event(
        db,
        job_id=job.id,
        event_type=JobStatus.QUEUED.value,
        message=reason,
    )
    return job


def mark_node_status(db: Session, node_id: str, status: str) -> Node | None:
    node = db.get(Node, node_id)
    if node is None:
        return None
    node.status = status
    if status != NodeStatus.BUSY.value:
        node.current_job_id = None
    node.last_seen = datetime.utcnow()
    return node


def mark_node_job(db: Session, node_id: str, job_id: str | None) -> Node | None:
    node = db.get(Node, node_id)
    if node is None:
        return None
    node.current_job_id = job_id
    node.last_seen = datetime.utcnow()
    return node


def clear_nonterminal_jobs(db: Session) -> dict[str, Any]:
    jobs = list_nonterminal_jobs(db)
    cleared_job_ids = [job.id for job in jobs]
    affected_node_ids = {
        job.assigned_node_id
        for job in jobs
        if job.assigned_node_id
    }

    for job in jobs:
        db.query(JobEvent).filter(JobEvent.job_id == job.id).delete(synchronize_session=False)
        db.delete(job)

    reset_nodes: list[str] = []
    for node_id in affected_node_ids:
        node = db.get(Node, node_id)
        if node is None or node.status == NodeStatus.OFFLINE.value:
            continue
        node.status = NodeStatus.IDLE.value
        node.current_job_id = None
        node.last_seen = datetime.utcnow()
        reset_nodes.append(node.id)

    return {
        "cleared_job_ids": cleared_job_ids,
        "reset_node_ids": reset_nodes,
    }
