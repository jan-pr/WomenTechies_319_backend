"""HTTP client helpers for communicating with the backend."""

from __future__ import annotations

import time
from typing import Any

import requests

try:
    from agent.config import AgentConfig
except ModuleNotFoundError:  # pragma: no cover - script execution fallback
    from config import AgentConfig


MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2


def _request(
    method: str,
    url: str,
    timeout: int,
    expected_statuses: set[int] | None = None,
    **kwargs: Any,
) -> requests.Response:
    """Send an HTTP request with basic retries and explicit error handling."""
    expected = expected_statuses or {200}
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
            if response.status_code in expected:
                return response
            raise RuntimeError(
                f"{method} {url} failed with status {response.status_code}: {response.text}"
            )
        except (requests.RequestException, RuntimeError) as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            print(
                f"[api] request failed (attempt {attempt}/{MAX_RETRIES}): {exc}. "
                f"Retrying in {RETRY_BACKOFF_SECONDS}s."
            )
            time.sleep(RETRY_BACKOFF_SECONDS)

    raise RuntimeError(f"request failed after {MAX_RETRIES} attempts: {last_error}")


def _agent_headers(config: AgentConfig) -> dict[str, str]:
    headers = {"X-Node-Id": config.node_id}
    if config.session_token:
        headers["X-Node-Session-Token"] = config.session_token
    return headers


def register_node(config: AgentConfig, node_data: dict[str, Any]) -> dict[str, Any]:
    """Register this node with the backend."""
    response = _request(
        "POST",
        f"{config.backend_url}/register-node",
        timeout=config.request_timeout_seconds,
        json=node_data,
    )
    return response.json()


def send_heartbeat(
    config: AgentConfig,
    node_id: str,
    status: str,
    current_job_id: str | None,
    progress: int,
) -> dict[str, Any]:
    """Send the node heartbeat to the backend."""
    payload = {
        "status": status,
        "cpu": progress if status == "busy" else 0,
        "current_job_id": current_job_id,
    }
    response = _request(
        "POST",
        f"{config.backend_url}/nodes/{node_id}/heartbeat",
        timeout=config.request_timeout_seconds,
        headers=_agent_headers(config),
        json=payload,
    )
    return response.json()


def poll_job(config: AgentConfig, node_id: str) -> dict[str, Any]:
    """Poll the backend for work assigned to this node."""
    response = _request(
        "POST",
        f"{config.backend_url}/nodes/{node_id}/poll-job",
        timeout=config.request_timeout_seconds,
        expected_statuses={200},
        headers=_agent_headers(config),
    )
    return response.json()


def report_progress(config: AgentConfig, job_id: str, progress: int) -> dict[str, Any]:
    """Report job progress to the backend."""
    response = _request(
        "POST",
        f"{config.backend_url}/jobs/{job_id}/progress",
        timeout=config.request_timeout_seconds,
        headers=_agent_headers(config),
        json={"progress": progress},
    )
    return response.json()


def complete_job(config: AgentConfig, job_id: str, result: Any) -> dict[str, Any]:
    """Report successful job completion to the backend."""
    response = _request(
        "POST",
        f"{config.backend_url}/jobs/{job_id}/complete",
        timeout=config.request_timeout_seconds,
        headers=_agent_headers(config),
        json={"result": result},
    )
    return response.json()


def fail_job(config: AgentConfig, job_id: str, error: str) -> dict[str, Any]:
    """Report a job failure to the backend."""
    response = _request(
        "POST",
        f"{config.backend_url}/jobs/{job_id}/fail",
        timeout=config.request_timeout_seconds,
        headers=_agent_headers(config),
        json={"status": "failed", "reason": error},
    )
    return response.json()
