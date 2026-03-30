"""Job scheduling orchestration for decentralized compute nodes."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from scheduler.selector import rank_nodes, select_best_node


def assign_job_to_node(
    job: Mapping[str, Any],
    nodes: Sequence[Mapping[str, Any]],
    carbon_api_key: str | None = None,
    weights: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Assign a job to the best available node and return a structured result."""
    selected = select_best_node(
        nodes=nodes,
        carbon_api_key=carbon_api_key,
        weights=weights,
    )
    if selected is None:
        return {
            "job_id": job.get("id"),
            "assigned": False,
            "reason": "no idle nodes available",
            "node_id": None,
        }

    node = selected["node"]
    return {
        "job_id": job.get("id"),
        "assigned": True,
        "node_id": node.get("id"),
        "node": node,
        "scores": {
            "trust_score": selected["trust_score"],
            "carbon_score": selected["carbon_score"],
            "final_score": selected["final_score"],
        },
    }


def build_assignment_queue(
    jobs: Sequence[Mapping[str, Any]],
    nodes: Sequence[Mapping[str, Any]],
    carbon_api_key: str | None = None,
    weights: Mapping[str, float] | None = None,
) -> list[dict[str, Any]]:
    """Create assignment decisions for a sequence of jobs using current node rankings."""
    ranked_nodes = rank_nodes(
        nodes=nodes,
        carbon_api_key=carbon_api_key,
        weights=weights,
    )
    available_nodes = ranked_nodes.copy()
    assignments: list[dict[str, Any]] = []

    for job in jobs:
        if not available_nodes:
            assignments.append(
                {
                    "job_id": job.get("id"),
                    "assigned": False,
                    "reason": "no idle nodes available",
                    "node_id": None,
                }
            )
            continue

        selected = available_nodes.pop(0)
        assignments.append(
            {
                "job_id": job.get("id"),
                "assigned": True,
                "node_id": selected["node"].get("id"),
                "node": selected["node"],
                "scores": {
                    "trust_score": selected["trust_score"],
                    "carbon_score": selected["carbon_score"],
                    "final_score": selected["final_score"],
                },
            }
        )

    return assignments
