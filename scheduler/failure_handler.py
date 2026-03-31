"""Failure handling helpers for scheduler state updates."""

from __future__ import annotations

from typing import Any, Mapping

from scheduler.trust import clamp_score


def mark_node_busy(node: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy of a node marked as busy after job assignment."""
    updated_node = dict(node)
    updated_node["status"] = "busy"
    return updated_node


def mark_node_idle(node: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy of a node marked idle after job completion or recovery."""
    updated_node = dict(node)
    updated_node["status"] = "idle"
    return updated_node


def update_node_metrics_after_failure(
    node: Mapping[str, Any],
    success_rate_penalty: float = 0.05,
    uptime_penalty: float = 0.02,
) -> dict[str, Any]:
    """Apply conservative trust penalties to a node after a failed execution."""
    metrics = dict(node.get("metrics", {}))
    current_success_rate = float(metrics.get("success_rate", 0.0))
    current_uptime = float(metrics.get("uptime", 0.0))

    normalized_success_rate = (
        current_success_rate / 100.0
        if current_success_rate > 1
        else current_success_rate
    )
    normalized_uptime = current_uptime / 100.0 if current_uptime > 1 else current_uptime

    metrics["success_rate"] = clamp_score(normalized_success_rate - success_rate_penalty)
    metrics["uptime"] = clamp_score(normalized_uptime - uptime_penalty)

    updated_node = dict(node)
    updated_node["metrics"] = metrics
    updated_node["status"] = "idle"
    return updated_node


def build_failure_result(
    job: Mapping[str, Any],
    node: Mapping[str, Any],
    reason: str,
) -> dict[str, Any]:
    """Create a structured failure payload for upstream retry or logging flows."""
    if not reason:
        raise ValueError("reason is required")

    return {
        "job_id": job.get("id"),
        "node_id": node.get("id"),
        "status": "failed",
        "reason": reason,
        "retryable": True,
    }
