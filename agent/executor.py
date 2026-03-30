"""Job execution for the worker agent."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


ProgressCallback = Callable[[int], None]


def _require_positive_int(payload: dict[str, Any], key: str) -> int:
    """Read and validate a non-negative integer payload field."""
    value = payload.get(key)
    if not isinstance(value, int):
        raise ValueError(f"payload['{key}'] must be an integer")
    if value < 0:
        raise ValueError(f"payload['{key}'] must be non-negative")
    return value


def _execute_compute_sum(
    payload: dict[str, Any],
    progress_callback: ProgressCallback,
) -> dict[str, Any]:
    """Compute sum(range(n)) with real progress updates every ~10%."""
    n = _require_positive_int(payload, "n")
    if n == 0:
        progress_callback(100)
        return {"status": "success", "result": 0}

    chunk_size = max(1, n // 10)
    total = 0
    for current in range(n):
        total += current
        if current == n - 1 or (current + 1) % chunk_size == 0:
            progress = min(100, int(((current + 1) / n) * 100))
            progress_callback(progress)

    return {"status": "success", "result": total}


def _execute_sleep_task(
    payload: dict[str, Any],
    progress_callback: ProgressCallback,
) -> dict[str, Any]:
    """Sleep for the requested time and report progress once per second."""
    seconds = _require_positive_int(payload, "seconds")
    if seconds == 0:
        progress_callback(100)
        return {"status": "success", "result": 0}

    for second in range(1, seconds + 1):
        time.sleep(1)
        progress = int((second / seconds) * 100)
        progress_callback(progress)

    return {"status": "success", "result": seconds}


def execute_job(
    job: dict[str, Any],
    progress_callback: ProgressCallback,
) -> dict[str, Any]:
    """Execute a supported job and return a normalized success result."""
    task_type = job.get("task_type") or job.get("type")
    if not task_type:
        raise ValueError(f"missing task type in job: {job}")

    payload = job.get("payload", {})
    if not isinstance(payload, dict):
        raise ValueError("job payload must be an object")

    print(f"[executor] running task_type={task_type}")

    if task_type == "compute_sum":
        return _execute_compute_sum(payload, progress_callback)
    if task_type == "sleep_task":
        return _execute_sleep_task(payload, progress_callback)
    if task_type == "demo_task":
        return _execute_sleep_task({"seconds": 5}, progress_callback)

    raise ValueError(f"unsupported task_type: {task_type}")
