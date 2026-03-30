"""Node filtering and ranking helpers for scheduler assignment."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from scheduler.carbon import get_node_carbon_score
from scheduler.trust import calculate_trust_score


DEFAULT_SELECTOR_WEIGHTS = {
    "trust": 0.7,
    "carbon": 0.3,
}


def is_node_idle(node: Mapping[str, Any]) -> bool:
    """Return True when a node is available for immediate scheduling."""
    status = node.get("status")
    availability = node.get("availability")

    if status is not None and str(status).lower() != "idle":
        return False
    if availability is not None and str(availability).lower() != "idle":
        return False
    return status is not None or availability is not None


def filter_idle_nodes(nodes: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Filter a list of nodes down to those currently marked idle."""
    return [node for node in nodes if is_node_idle(node)]


def score_node(
    node: Mapping[str, Any],
    carbon_api_key: str | None = None,
    weights: Mapping[str, float] | None = None,
) -> dict[str, float]:
    """Compute the component scores and final ranking score for a node."""
    active_weights = dict(DEFAULT_SELECTOR_WEIGHTS)
    if weights:
        active_weights.update(weights)

    total_weight = sum(active_weights.values())
    if total_weight <= 0:
        raise ValueError("selector weights must sum to a value greater than zero")

    trust_score = calculate_trust_score(node)
    carbon_score = get_node_carbon_score(node, api_key=carbon_api_key)
    final_score = (
        trust_score * active_weights["trust"] + carbon_score * active_weights["carbon"]
    ) / total_weight

    return {
        "trust_score": trust_score,
        "carbon_score": carbon_score,
        "final_score": final_score,
    }


def rank_nodes(
    nodes: Sequence[Mapping[str, Any]],
    carbon_api_key: str | None = None,
    weights: Mapping[str, float] | None = None,
) -> list[dict[str, Any]]:
    """Rank idle nodes from best to worst based on trust and carbon scores."""
    ranked_nodes: list[dict[str, Any]] = []
    for node in filter_idle_nodes(nodes):
        scores = score_node(node, carbon_api_key=carbon_api_key, weights=weights)
        ranked_nodes.append(
            {
                "node": dict(node),
                **scores,
            }
        )

    return sorted(ranked_nodes, key=lambda item: item["final_score"], reverse=True)


def select_best_node(
    nodes: Sequence[Mapping[str, Any]],
    carbon_api_key: str | None = None,
    weights: Mapping[str, float] | None = None,
) -> dict[str, Any] | None:
    """Select the highest-ranked idle node or return None when none are available."""
    ranked_nodes = rank_nodes(nodes, carbon_api_key=carbon_api_key, weights=weights)
    if not ranked_nodes:
        return None
    return ranked_nodes[0]
