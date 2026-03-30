from __future__ import annotations

import asyncio
import os
from typing import Any

from sqlalchemy.orm import Session

from WT_ComputePool.db import SessionLocal
from WT_ComputePool.models import JobStatus, NodeStatus
from WT_ComputePool.redis_client import (
    ASSIGNED_JOBS_KEY,
    JOB_QUEUE_KEY,
    enqueue_job,
    r,
    remove_job_from_queue,
)
from WT_ComputePool.repository import (
    get_job,
    get_next_queued_job,
    list_nonterminal_jobs,
    list_nodes,
    list_reassignable_jobs_for_node,
    list_stale_nodes,
    mark_job_assigned_and_running,
    mark_node_status,
    requeue_job,
    serialize_job,
    serialize_node,
)
from scheduler.scheduler import assign_job_to_node


HEARTBEAT_TIMEOUT_SECONDS = int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "60"))
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "15"))
CARBON_API_KEY = os.getenv("ELECTRICITY_MAPS_API_KEY")


def assign_next_queued_job(db: Session) -> dict[str, Any]:
    while True:
        job_id = r.lindex(JOB_QUEUE_KEY, -1)
        if job_id is None:
            fallback_job = get_next_queued_job(db)
            if fallback_job is None:
                return {"assigned": False, "reason": "no queued jobs"}
            enqueue_job(fallback_job.id)
            job_id = fallback_job.id

        job = get_job(db, job_id)
        if job is None:
            remove_job_from_queue(job_id)
            continue
        if job.status != JobStatus.QUEUED.value:
            remove_job_from_queue(job_id)
            continue

        print("Assigning job:", job_id)
        nodes = [serialize_node(node) for node in list_nodes(db)]
        result = assign_job_to_node(
            serialize_job(job),
            nodes,
            carbon_api_key=CARBON_API_KEY,
        )
        if not result["assigned"]:
            return result

        node_id = result["node_id"]
        print("Node selected:", node_id)
        if node_id in r.hvals(ASSIGNED_JOBS_KEY):
            enqueue_job(job_id)
            return {"assigned": False, "reason": "node already busy"}

        r.hset(ASSIGNED_JOBS_KEY, job_id, node_id)
        try:
            assigned_node = mark_node_status(db, node_id=node_id, status=NodeStatus.BUSY.value)
            if assigned_node is None:
                raise RuntimeError(f"node '{node_id}' not found")

            mark_job_assigned_and_running(
                db,
                job=job,
                node_id=node_id,
                scores=result.get("scores"),
            )
            remove_job_from_queue(job_id)
            db.commit()
        except Exception:
            db.rollback()
            r.hdel(ASSIGNED_JOBS_KEY, job_id)
            enqueue_job(job_id)
            raise

        print("Node marked busy")
        return {
            **result,
            "job": serialize_job(job),
            "node": serialize_node(assigned_node),
        }


def assign_all_queued_jobs(db: Session) -> list[dict[str, Any]]:
    assignments: list[dict[str, Any]] = []
    while True:
        result = assign_next_queued_job(db)
        if not result.get("assigned"):
            break
        assignments.append(result)
    return assignments


def process_stale_nodes(db: Session) -> dict[str, Any]:
    stale_nodes = list_stale_nodes(db, stale_after_seconds=HEARTBEAT_TIMEOUT_SECONDS)
    reassigned_jobs: list[str] = []
    offline_nodes: list[str] = []

    for node in stale_nodes:
        offline_nodes.append(node.id)
        mark_node_status(db, node_id=node.id, status=NodeStatus.OFFLINE.value)
        for job in list_reassignable_jobs_for_node(db, node_id=node.id):
            requeue_job(
                db,
                job,
                reason=f"node {node.id} heartbeat expired; job re-queued",
            )
            enqueue_job(job.id)
            reassigned_jobs.append(job.id)

    auto_assignments = assign_all_queued_jobs(db) if offline_nodes else []
    return {
        "offline_nodes": offline_nodes,
        "requeued_jobs": reassigned_jobs,
        "assignments": auto_assignments,
    }


async def monitor_cluster_state() -> None:
    while True:
        db = SessionLocal()
        try:
            process_stale_nodes(db)
            db.commit()
        except Exception:
            db.rollback()
        finally:
            db.close()
        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


def repair_redis_state(db: Session) -> dict[str, Any]:
    assigned_jobs = r.hgetall(ASSIGNED_JOBS_KEY)
    removed_assignments: list[str] = []
    requeued_jobs: list[str] = []
    kept_nodes: set[str] = set()

    for job_id, node_id in assigned_jobs.items():
        job = get_job(db, job_id)
        if (
            job is None
            or job.status not in {JobStatus.ASSIGNED.value, JobStatus.RUNNING.value}
            or job.assigned_node_id != node_id
        ):
            r.hdel(ASSIGNED_JOBS_KEY, job_id)
            removed_assignments.append(job_id)
            continue

        if node_id in kept_nodes:
            requeue_job(
                db,
                job,
                reason="redis repair removed duplicate assignment for node",
            )
            r.hdel(ASSIGNED_JOBS_KEY, job_id)
            enqueue_job(job_id)
            removed_assignments.append(job_id)
            requeued_jobs.append(job_id)
            continue

        kept_nodes.add(node_id)
        remove_job_from_queue(job_id)

    for job in list_nonterminal_jobs(db):
        if job.status == JobStatus.QUEUED.value:
            enqueue_job(job.id)
            continue
        if job.status in {JobStatus.ASSIGNED.value, JobStatus.RUNNING.value}:
            remove_job_from_queue(job.id)

    return {
        "removed_assignments": removed_assignments,
        "requeued_jobs": requeued_jobs,
        "active_assignments": r.hgetall(ASSIGNED_JOBS_KEY),
        "job_queue": r.lrange(JOB_QUEUE_KEY, 0, -1),
    }
