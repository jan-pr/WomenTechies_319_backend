"""Docker-based sandbox execution for arbitrary code jobs."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any
import uuid


LANGUAGE_CONFIG: dict[str, dict[str, Any]] = {
    "python": {
        "image": "python:3.11-slim",
        "run_command": lambda entrypoint: ["python", entrypoint],
        "requirements_file": "requirements.txt",
        "install_command": lambda requirements_file, requirements: (
            f"python -m pip install --no-cache-dir -r {requirements_file}"
            if requirements
            else ""
        ),
    },
    "node": {
        "image": "node:20-alpine",
        "run_command": lambda entrypoint: ["node", entrypoint],
        "requirements_file": "package-deps.txt",
        "install_command": lambda requirements_file, requirements: (
            "npm install --no-package-lock --omit=dev "
            + " ".join(requirements)
            if requirements
            else ""
        ),
    },
}


def _require_dict(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict) or not value:
        raise ValueError(f"payload['{key}'] must be a non-empty object")
    return value


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"payload['{key}'] must be a non-empty string")
    return value


def _optional_string_list(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key, [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"payload['{key}'] must be a list of strings")
    return value


def is_sandbox_job(job: dict[str, Any]) -> bool:
    """Return true when the job should use Docker sandbox execution."""
    payload = job.get("payload", {})
    return (
        job.get("task_type") == "sandbox_code"
        or (
            isinstance(payload, dict)
            and isinstance(payload.get("language"), str)
            and isinstance(payload.get("files"), dict)
            and isinstance(payload.get("entrypoint"), str)
        )
    )


def _write_workspace_files(workspace: Path, files: dict[str, str]) -> None:
    for relative_path, content in files.items():
        if not isinstance(relative_path, str) or not relative_path.strip():
            raise ValueError("job file paths must be non-empty strings")
        if not isinstance(content, str):
            raise ValueError(f"file '{relative_path}' content must be a string")

        normalized = Path(relative_path)
        if normalized.is_absolute() or ".." in normalized.parts:
            raise ValueError(f"invalid file path: {relative_path}")

        target = workspace / normalized
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def _write_dependency_file(workspace: Path, language: str, requirements: list[str]) -> str | None:
    if not requirements:
        return None

    file_name = LANGUAGE_CONFIG[language]["requirements_file"]
    file_path = workspace / file_name
    file_path.write_text("\n".join(requirements) + "\n", encoding="utf-8")
    return file_name


def _build_dockerfile(language: str, dependency_file: str | None, requirements: list[str]) -> str:
    config = LANGUAGE_CONFIG[language]
    lines = [
        f"FROM {config['image']}",
        "WORKDIR /workspace",
        "COPY . /workspace",
    ]

    install_command = config["install_command"](dependency_file, requirements)
    if install_command:
        lines.append(f"RUN {install_command}")

    return "\n".join(lines) + "\n"


def _run_command(command: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )


def execute_sandbox_job(
    payload: dict[str, Any],
    *,
    cpu_limit: str = "1.0",
    memory_limit: str = "512m",
) -> dict[str, Any]:
    """Build an isolated workspace and execute it inside Docker."""
    language = _require_string(payload, "language").lower()
    if language not in LANGUAGE_CONFIG:
        raise ValueError(f"unsupported sandbox language: {language}")

    files = _require_dict(payload, "files")
    entrypoint = _require_string(payload, "entrypoint")
    requirements = _optional_string_list(payload, "requirements")
    timeout_seconds = payload.get("timeout", 30)
    if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        raise ValueError("payload['timeout'] must be a positive integer")

    image_tag = f"worker-job-{uuid.uuid4().hex}"
    workspace_dir = Path(tempfile.mkdtemp(prefix="worker-job-"))

    try:
        _write_workspace_files(workspace_dir, files)
        if not (workspace_dir / entrypoint).is_file():
            raise ValueError(f"entrypoint file not found in submitted files: {entrypoint}")

        dependency_file = _write_dependency_file(workspace_dir, language, requirements)
        (workspace_dir / "Dockerfile").write_text(
            _build_dockerfile(language, dependency_file, requirements),
            encoding="utf-8",
        )

        build_result = _run_command(
            ["docker", "build", "-t", image_tag, str(workspace_dir)],
            timeout_seconds=max(timeout_seconds, 120),
        )
        if build_result.returncode != 0:
            raise RuntimeError(
                "docker build failed: "
                + (build_result.stderr or build_result.stdout or "no output").strip()
            )

        run_command = LANGUAGE_CONFIG[language]["run_command"](entrypoint)
        execution = _run_command(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--cpus",
                cpu_limit,
                "--memory",
                memory_limit,
                image_tag,
                *run_command,
            ],
            timeout_seconds=timeout_seconds,
        )
        return {
            "status": "success" if execution.returncode == 0 else "failed",
            "result": {
                "language": language,
                "stdout": execution.stdout,
                "stderr": execution.stderr,
                "exit_code": execution.returncode,
                "entrypoint": entrypoint,
            },
        }
    except FileNotFoundError as exc:
        raise RuntimeError("docker is not installed or not available on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"sandbox execution timed out after {timeout_seconds} seconds") from exc
    finally:
        try:
            _run_command(["docker", "image", "rm", "-f", image_tag], timeout_seconds=60)
        except Exception:
            pass
        shutil.rmtree(workspace_dir, ignore_errors=True)
