"""Carbon intensity lookup and scoring helpers for scheduler decisions."""

from __future__ import annotations

from typing import Any, Mapping

import requests


DEFAULT_ELECTRICITY_MAPS_URL = "https://api.electricitymap.org/v3/carbon-intensity/latest"


def fetch_carbon_intensity(
    zone: str,
    api_key: str,
    base_url: str = DEFAULT_ELECTRICITY_MAPS_URL,
    timeout: int = 10,
    session: requests.sessions.Session | None = None,
) -> float:
    """Fetch the latest carbon intensity for a given Electricity Maps zone."""
    if not zone:
        raise ValueError("zone is required")
    if not api_key:
        raise ValueError("api_key is required")

    http_client = session or requests
    headers = {"auth-token": api_key}
    params = {"zone": zone}

    try:
        response = http_client.get(
            base_url,
            headers=headers,
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"failed to fetch carbon intensity for zone '{zone}'") from exc

    payload = response.json()
    intensity = extract_carbon_intensity(payload)
    return float(intensity)


def extract_carbon_intensity(payload: Mapping[str, Any]) -> float:
    """Extract a carbon intensity value from an Electricity Maps API response."""
    if "carbonIntensity" not in payload:
        raise ValueError("carbonIntensity not found in Electricity Maps response")

    intensity = payload["carbonIntensity"]
    if intensity is None:
        raise ValueError("carbonIntensity is missing from Electricity Maps response")
    return float(intensity)


def carbon_intensity_to_score(
    carbon_intensity: float,
    max_expected_intensity: float = 800.0,
) -> float:
    """Convert carbon intensity into a 0.0 to 1.0 score where lower is better."""
    if max_expected_intensity <= 0:
        raise ValueError("max_expected_intensity must be greater than zero")
    if carbon_intensity < 0:
        raise ValueError("carbon_intensity must be non-negative")

    bounded_intensity = min(carbon_intensity, max_expected_intensity)
    return max(0.0, 1.0 - (bounded_intensity / max_expected_intensity))


def get_node_carbon_score(
    node: Mapping[str, Any],
    api_key: str | None = None,
    max_expected_intensity: float = 800.0,
    session: requests.sessions.Session | None = None,
) -> float:
    """Return a node carbon score from cached data or a fresh API lookup."""
    carbon_intensity = node.get("carbon_intensity")
    if carbon_intensity is None:
        zone = node.get("carbon_zone")
        if not zone:
            raise ValueError("node must include 'carbon_intensity' or a carbon zone")
        if not api_key:
            raise ValueError("api_key is required when carbon intensity is not provided")
        carbon_intensity = fetch_carbon_intensity(
            zone=str(zone),
            api_key=api_key,
            session=session,
        )

    return carbon_intensity_to_score(
        carbon_intensity=float(carbon_intensity),
        max_expected_intensity=max_expected_intensity,
    )
