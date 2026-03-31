"""Job execution for the worker agent."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from typing import Any

try:
    from agent.sandbox_runner import execute_sandbox_job, is_sandbox_job
except ModuleNotFoundError:  # pragma: no cover - script execution fallback
    from sandbox_runner import execute_sandbox_job, is_sandbox_job


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


def _require_string(payload: dict[str, Any], key: str) -> str:
    """Read and validate a non-empty string payload field."""
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"payload['{key}'] must be a non-empty string")
    return value


def _optional_string_list(payload: dict[str, Any], key: str) -> list[str]:
    """Read and validate an optional list of strings."""
    value = payload.get(key, [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"payload['{key}'] must be a list of strings")
    return value


def _execute_python_script(
    payload: dict[str, Any],
    progress_callback: ProgressCallback,
) -> dict[str, Any]:
    """Execute a Python script from a file path or inline code."""
    script_path = payload.get("script_path")
    code = payload.get("code")
    args = _optional_string_list(payload, "args")
    timeout_seconds = payload.get("timeout_seconds", 300)

    if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        raise ValueError("payload['timeout_seconds'] must be a positive integer")

    if script_path and code:
        raise ValueError("provide either 'script_path' or 'code', not both")
    if not script_path and not code:
        raise ValueError("payload must include either 'script_path' or 'code'")

    temp_path: str | None = None
    try:
        if code is not None:
            code_string = _require_string(payload, "code")
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".py",
                delete=False,
            ) as handle:
                handle.write(code_string)
                temp_path = handle.name
            script_to_run = temp_path
        else:
            script_to_run = _require_string(payload, "script_path")
            if not os.path.isfile(script_to_run):
                raise ValueError(f"python file not found: {script_to_run}")

        print(f"[executor] running python script {script_to_run}")
        completed = subprocess.run(
            [sys.executable, script_to_run, *args],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )

        if completed.returncode != 0:
            error_output = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(
                f"python script failed with exit code {completed.returncode}: "
                f"{error_output or 'no error output'}"
            )

        progress_callback(100)
        return {
            "status": "success",
            "result": {
                "returncode": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            },
        }
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"python script timed out after {timeout_seconds} seconds"
        ) from exc
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


def execute_job(
    job: dict[str, Any],
    progress_callback: ProgressCallback,
    *,
    sandbox_cpu_limit: str = "1.0",
    sandbox_memory_limit: str = "512m",
) -> dict[str, Any]:
    """Execute a supported job and return a normalized success result."""
    if is_sandbox_job(job):
        payload = job.get("payload", {})
        if not isinstance(payload, dict):
            raise ValueError("sandbox job payload must be an object")
        result = execute_sandbox_job(
            payload,
            cpu_limit=sandbox_cpu_limit,
            memory_limit=sandbox_memory_limit,
        )
        progress_callback(100)
        if result["status"] != "success":
            raise RuntimeError(json.dumps(result["result"]))
        return result

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
    if task_type == "python_script":
        return _execute_python_script(payload, progress_callback)
    if task_type == "demo_task":
        return _execute_sleep_task({"seconds": 5}, progress_callback)

    raise ValueError(f"unsupported task_type: {task_type}")
