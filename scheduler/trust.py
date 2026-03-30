"""Trust score helpers for scheduler node ranking."""

from __future__ import annotations

from typing import Mapping


DEFAULT_TRUST_WEIGHTS = {
    "success_rate": 0.5,
    "uptime": 0.3,
    "speed": 0.2,
}


def clamp_score(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    """Clamp a numeric score between the provided bounds."""
    return max(minimum, min(maximum, value))


def normalize_percentage(value: float) -> float:
    """Normalize a percentage-like value into a 0.0 to 1.0 range."""
    if value > 1:
        value = value / 100.0
    return clamp_score(float(value))


def normalize_speed(
    speed: float,
    fastest_expected: float = 1.0,
    slowest_expected: float = 10.0,
) -> float:
    """Convert a latency-like speed metric into a score where faster is better."""
    if slowest_expected <= fastest_expected:
        raise ValueError("slowest_expected must be greater than fastest_expected")
    if speed < 0:
        raise ValueError("speed must be non-negative")

    bounded_speed = min(max(speed, fastest_expected), slowest_expected)
    range_size = slowest_expected - fastest_expected
    normalized = 1.0 - ((bounded_speed - fastest_expected) / range_size)
    return clamp_score(normalized)


def calculate_trust_score(
    node: Mapping[str, object],
    weights: Mapping[str, float] | None = None,
    fastest_expected: float = 1.0,
    slowest_expected: float = 10.0,
) -> float:
    """Calculate a composite trust score from node performance signals."""
    metrics = node.get("metrics", {})
    if not isinstance(metrics, Mapping):
        raise ValueError("node['metrics'] must be a mapping when provided")

    active_weights = dict(DEFAULT_TRUST_WEIGHTS)
    if weights:
        active_weights.update(weights)

    success_rate = normalize_percentage(float(metrics.get("success_rate", 0.0)))
    uptime = normalize_percentage(float(metrics.get("uptime", 0.0)))
    speed = normalize_speed(
        float(metrics.get("speed", slowest_expected)),
        fastest_expected=fastest_expected,
        slowest_expected=slowest_expected,
    )

    total_weight = sum(active_weights.values())
    if total_weight <= 0:
        raise ValueError("weights must sum to a value greater than zero")

    weighted_score = (
        success_rate * active_weights["success_rate"]
        + uptime * active_weights["uptime"]
        + speed * active_weights["speed"]
    )
    return clamp_score(weighted_score / total_weight)
