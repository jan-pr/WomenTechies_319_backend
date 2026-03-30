from __future__ import annotations

import asyncio
import os
from typing import Any

from sqlalchemy.orm import Session

from WT_ComputePool.db import SessionLocal
from WT_ComputePool.models import NodeStatus
from WT_ComputePool.repository import (
    get_next_queued_job,
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
    job = get_next_queued_job(db)
    if job is None:
        return {"assigned": False, "reason": "no queued jobs"}

    nodes = [serialize_node(node) for node in list_nodes(db)]
    result = assign_job_to_node(
        serialize_job(job),
        nodes,
        carbon_api_key=CARBON_API_KEY,
    )
    if not result["assigned"]:
        return result

    node_id = result["node_id"]
    assigned_node = mark_node_status(db, node_id=node_id, status=NodeStatus.BUSY.value)
    mark_job_assigned_and_running(
        db,
        job=job,
        node_id=node_id,
        scores=result.get("scores"),
    )
    return {
        **result,
        "job": serialize_job(job),
        "node": serialize_node(assigned_node) if assigned_node else result["node"],
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
