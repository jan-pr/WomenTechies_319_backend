from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import json
import os
from pathlib import Path, PurePosixPath
import re
import traceback
import uuid

from fastapi import Body, Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect, File, Form,UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import AliasChoices, BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from scheduler.carbon import fetch_carbon_intensity
from WT_ComputePool.db import SessionLocal, get_database_debug_info, get_db, initialize_database
from WT_ComputePool.models import JobStatus, NodeStatus
from WT_ComputePool.redis_client import (
    ASSIGNED_JOBS_KEY,
    enqueue_job,
    get_node_session,
    r,
    set_node_session,
)
from WT_ComputePool.repository import (
    clear_nonterminal_jobs,
    create_job,
    get_job,
    get_node,
    list_jobs,
    list_nodes,
    list_reassignable_jobs_for_node,
    mark_job_completed,
    mark_job_failed,
    mark_node_status,
    node_has_active_jobs,
    requeue_job,
    serialize_job,
    serialize_node,
    update_job_progress,
    upsert_node,
)
from WT_ComputePool.services import (
    assign_next_queued_job,
    monitor_cluster_state,
    poll_job_for_node,
    process_stale_nodes,
    repair_redis_state,
)

CARBON_API_KEY = os.getenv("ELECTRICITY_MAPS_API_KEY")
MAX_UPLOAD_FILES = 25
MAX_FILE_BYTES = 512 * 1024
MAX_TOTAL_UPLOAD_BYTES = 2 * 1024 * 1024
MAX_REQUIREMENTS = 100
DEFAULT_CODE_TIMEOUT_SECONDS = 30
MAX_CODE_TIMEOUT_SECONDS = 3600
SUPPORTED_CODE_LANGUAGES = {"python", "node"}
ALLOWED_SOURCE_EXTENSIONS = {
    ".py",
    ".js",
    ".mjs",
    ".cjs",
    ".json",
    ".txt",
    ".md",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".env",
}
SAFE_REQUIREMENT_PATTERN = re.compile(r"^[A-Za-z0-9._+\-<>=!~\[\]]+$")


class WebSocketManager:
    def __init__(self) -> None:
        self.connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.connections:
            self.connections.remove(websocket)

    async def broadcast(self, message: dict) -> None:
        stale_connections: list[WebSocket] = []
        for websocket in self.connections:
            try:
                await websocket.send_json(message)
            except Exception:
                stale_connections.append(websocket)

        for websocket in stale_connections:
            self.disconnect(websocket)


ws_manager = WebSocketManager()
PROJECT_ROOT = Path(__file__).resolve().parent.parent
AGENT_LOG_DIR = PROJECT_ROOT / "agent_logs"


@asynccontextmanager
async def lifespan(_: FastAPI):
    initialize_database()
    monitor_task = asyncio.create_task(monitor_cluster_state())
    try:
        yield
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class NodeMetricsPayload(BaseModel):
    success_rate: float | None = None
    uptime: float | None = None
    speed: float | None = None


class NodeRegistrationPayload(BaseModel):
    node_id: str
    status: str = NodeStatus.IDLE.value
    availability: str = NodeStatus.IDLE.value
    metrics: NodeMetricsPayload = Field(default_factory=NodeMetricsPayload)
    carbon_intensity: float | None = None
    carbon_zone: str | None = Field(
        default=None,
        validation_alias=AliasChoices("carbon_zone", "zone"),
    )
    cpu: float | None = None
    current_job_id: str | None = None


class HeartbeatPayload(BaseModel):
    status: str | None = None
    availability: str | None = None
    metrics: NodeMetricsPayload = Field(default_factory=NodeMetricsPayload)
    carbon_intensity: float | None = None
    carbon_zone: str | None = Field(
        default=None,
        validation_alias=AliasChoices("carbon_zone", "zone"),
    )
    cpu: float | None = None
    current_job_id: str | None = None


class JobResultPayload(BaseModel):
    status: str
    reason: str | None = None


class JobSubmissionPayload(BaseModel):
    task_type: str = "demo_task"
    payload: dict[str, object] = Field(default_factory=dict)
    language: str | None = None
    files: dict[str, str] | None = None
    requirements: list[str] | None = None
    entrypoint: str | None = None
    timeout: int | None = None


class JobProgressPayload(BaseModel):
    progress: int


class JobCompletionPayload(BaseModel):
    result: dict[str, object] | None = None


def _issue_node_session(node_id: str) -> str:
    session_token = uuid.uuid4().hex
    set_node_session(node_id, session_token)
    return session_token


def _require_node_session(node_id: str, node_session_token: str | None) -> None:
    expected = get_node_session(node_id)
    if expected is None:
        return
    if not node_session_token or node_session_token != expected:
        raise HTTPException(status_code=409, detail="stale agent session")


def _release_node_if_job_finished(
    db: Session,
    *,
    node_id: str | None,
    job_id: str,
) -> None:
    if not node_id:
        return

    r.hdel(ASSIGNED_JOBS_KEY, job_id)
    node = get_node(db, node_id)
    if node is None:
        return
    if node.current_job_id == job_id or not node_has_active_jobs(
        db,
        node_id=node_id,
        exclude_job_id=job_id,
    ):
        mark_node_status(db, node_id=node_id, status=NodeStatus.IDLE.value)


def _resolve_job_submission(payload: JobSubmissionPayload | None, task_type: str) -> tuple[str, dict[str, object]]:
    """Support both legacy task payloads and newer arbitrary-code submissions."""
    if payload is None:
        return task_type, {}

    if payload.language and payload.files and payload.entrypoint:
        return (
            "sandbox_code",
            {
                "language": payload.language,
                "files": payload.files,
                "requirements": payload.requirements or [],
                "entrypoint": payload.entrypoint,
                "timeout": payload.timeout or 30,
            },
        )

    nested_payload = payload.payload if isinstance(payload.payload, dict) else {}
    if payload.task_type == "demo_task":
        if isinstance(nested_payload.get("code"), str) or isinstance(nested_payload.get("script_path"), str):
            return "python_script", nested_payload
        if (
            isinstance(nested_payload.get("language"), str)
            and isinstance(nested_payload.get("files"), dict)
            and isinstance(nested_payload.get("entrypoint"), str)
        ):
            return "sandbox_code", nested_payload

    return payload.task_type, payload.payload


def _validate_relative_source_path(path_value: str) -> str:
    normalized = PurePosixPath(path_value.replace("\\", "/"))
    if normalized.is_absolute() or ".." in normalized.parts:
        raise HTTPException(status_code=400, detail=f"invalid file path: {path_value}")
    if not normalized.name:
        raise HTTPException(status_code=400, detail=f"invalid file path: {path_value}")
    suffix = normalized.suffix.lower()
    if suffix and suffix not in ALLOWED_SOURCE_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported file type for upload: {normalized.name}",
        )
    return normalized.as_posix()


def _parse_requirements_text(raw_requirements: str | None) -> list[str]:
    if not raw_requirements:
        return []

    parsed: object
    try:
        parsed = json.loads(raw_requirements)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, list):
        candidates = parsed
    else:
        separators_normalized = raw_requirements.replace("\r\n", "\n").replace(",", "\n")
        candidates = [line.strip() for line in separators_normalized.split("\n")]

    cleaned: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            raise HTTPException(status_code=400, detail="requirements must be strings")
        requirement = candidate.strip()
        if not requirement:
            continue
        if not SAFE_REQUIREMENT_PATTERN.match(requirement):
            raise HTTPException(
                status_code=400,
                detail=f"invalid requirement entry: {requirement}",
            )
        cleaned.append(requirement)

    if len(cleaned) > MAX_REQUIREMENTS:
        raise HTTPException(status_code=400, detail="too many requirements")
    return cleaned


async def _normalize_uploaded_code_job(
    *,
    language: str,
    entrypoint: str,
    timeout: int,
    requirements_text: str | None,
    uploads: list[UploadFile],
) -> dict[str, object]:
    normalized_language = language.strip().lower()
    if normalized_language not in SUPPORTED_CODE_LANGUAGES:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported language: {normalized_language}",
        )
    if timeout <= 0 or timeout > MAX_CODE_TIMEOUT_SECONDS:
        raise HTTPException(
            status_code=400,
            detail=f"timeout must be between 1 and {MAX_CODE_TIMEOUT_SECONDS} seconds",
        )
    if not uploads:
        raise HTTPException(status_code=400, detail="at least one source file is required")
    if len(uploads) > MAX_UPLOAD_FILES:
        raise HTTPException(status_code=400, detail="too many uploaded files")

    normalized_entrypoint = _validate_relative_source_path(entrypoint)
    files: dict[str, str] = {}
    total_bytes = 0

    for upload in uploads:
        if not upload.filename:
            raise HTTPException(status_code=400, detail="uploaded file is missing a filename")
        normalized_path = _validate_relative_source_path(upload.filename)
        if normalized_path in files:
            raise HTTPException(
                status_code=400,
                detail=f"duplicate uploaded file path: {normalized_path}",
            )

        content_bytes = await upload.read()
        total_bytes += len(content_bytes)
        if len(content_bytes) > MAX_FILE_BYTES:
            raise HTTPException(
                status_code=400,
                detail=f"file too large: {normalized_path}",
            )
        if total_bytes > MAX_TOTAL_UPLOAD_BYTES:
            raise HTTPException(status_code=400, detail="total upload size exceeded")

        try:
            decoded = content_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"file must be utf-8 text: {normalized_path}",
            ) from exc

        files[normalized_path] = decoded

    if normalized_entrypoint not in files:
        raise HTTPException(
            status_code=400,
            detail=f"entrypoint '{normalized_entrypoint}' was not uploaded",
        )

    requirements = _parse_requirements_text(requirements_text)
    return {
        "language": normalized_language,
        "files": files,
        "requirements": requirements,
        "entrypoint": normalized_entrypoint,
        "timeout": timeout,
    }


def _build_node_record(
    node_id: str,
    payload: NodeRegistrationPayload | HeartbeatPayload | None = None,
) -> dict:
    metrics = payload.metrics.model_dump(exclude_none=True) if payload else {}

    node_record = {
        "id": node_id,
        "metrics": metrics,
        "cpu": payload.cpu if payload else None,
        "last_seen": datetime.utcnow().isoformat(),
    }

    if payload is None:
        node_record["status"] = NodeStatus.IDLE.value
        node_record["availability"] = NodeStatus.IDLE.value
    else:
        if payload.status is not None:
            node_record["status"] = payload.status
        if payload.availability is not None:
            node_record["availability"] = payload.availability

    if payload:
        if payload.carbon_intensity is not None:
            node_record["carbon_intensity"] = payload.carbon_intensity
        if payload.carbon_zone is not None:
            node_record["carbon_zone"] = payload.carbon_zone
        if payload.current_job_id is not None:
            node_record["current_job_id"] = payload.current_job_id

    return node_record


def _enrich_carbon_intensity(node_record: dict) -> dict:
    carbon_intensity = node_record.get("carbon_intensity")
    if carbon_intensity is not None and float(carbon_intensity) > 0:
        return node_record

    carbon_zone = node_record.get("carbon_zone")
    if not carbon_zone or not CARBON_API_KEY:
        return node_record

    try:
        node_record["carbon_intensity"] = fetch_carbon_intensity(
            zone=str(carbon_zone),
            api_key=CARBON_API_KEY,
        )
    except RuntimeError:
        node_record["carbon_intensity"] = None

    return node_record


@app.get("/")
def root():
    return {"message": "Backend running"}


@app.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {
        "status": "OK",
        "database": "connected",
        "database_debug": get_database_debug_info(),
    }


@app.post("/register-node")
def register_node(
    node_id: str | None = None,
    payload: NodeRegistrationPayload | None = Body(default=None),
    db: Session = Depends(get_db),
):
    resolved_node_id = payload.node_id if payload else node_id
    if not resolved_node_id:
        raise HTTPException(status_code=400, detail="node_id is required")

    existing_node = get_node(db, resolved_node_id)
    node_data = _build_node_record(node_id=resolved_node_id, payload=payload)
    if existing_node is not None and existing_node.status == NodeStatus.BUSY.value:
        node_data.pop("status", None)
        node_data.pop("availability", None)
        node_data.pop("current_job_id", None)

    node_data = _enrich_carbon_intensity(node_data)
    node = upsert_node(db, node_data)
    db.commit()
    db.refresh(node)
    session_token = _issue_node_session(resolved_node_id)

    return {
        "message": f"Node {resolved_node_id} registered",
        "session_token": session_token,
        "node": serialize_node(node),
    }


@app.get("/debug")
def debug(db: Session = Depends(get_db)):
    jobs = list_jobs(db)
    return {
        "database_debug": get_database_debug_info(),
        "nodes": [serialize_node(node) for node in list_nodes(db)],
        "jobs": [serialize_job(job) for job in jobs],
        "queue": [
            serialize_job(job) for job in jobs if job.status == JobStatus.QUEUED.value
        ],
    }


@app.get("/nodes")
def get_nodes(db: Session = Depends(get_db)):
    return {node.id: serialize_node(node) for node in list_nodes(db)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)

@app.post("/nodes/{node_id}/stop")
def stop_node(node_id: str):
    db = SessionLocal()
    try:
        set_node_session(node_id, str(uuid.uuid4()))

        # mark node offline
        mark_node_status(db, node_id=node_id, status="offline")

        # reassign jobs
        for job in list_reassignable_jobs_for_node(db, node_id=node_id):
            requeue_job(
                db,
                job,
                reason=f"node {node_id} stopped manually"
            )

        db.commit()
        return {"message": f"node {node_id} stopped"}
    finally:
        db.close()
@app.post("/nodes/{node_id}/heartbeat")
def heartbeat(
    node_id: str,
    payload: HeartbeatPayload,
    db: Session = Depends(get_db),
    x_node_session_token: str | None = Header(default=None),
):
    _require_node_session(node_id, x_node_session_token)
    existing_node = get_node(db, node_id)
    if existing_node is None:
        raise HTTPException(status_code=404, detail="node not registered")
    node_data = _build_node_record(node_id=node_id, payload=payload)
    if existing_node is not None:
        serialized_existing = serialize_node(existing_node)
        if serialized_existing.get("carbon_zone") and "carbon_zone" not in node_data:
            node_data["carbon_zone"] = serialized_existing["carbon_zone"]
        if existing_node.status in {
            NodeStatus.BUSY.value,
            NodeStatus.OFFLINE.value,
        }:
            node_data.pop("status", None)
            node_data.pop("availability", None)

    node_data = _enrich_carbon_intensity(node_data)
    node = upsert_node(db, node_data)
    db.commit()
    db.refresh(node)
    return {"message": "Heartbeat recorded", "node": serialize_node(node)}


@app.post("/submit-job")
def submit_job(
    task_type: str = "demo_task",
    payload: JobSubmissionPayload | None = Body(default=None),
    db: Session = Depends(get_db),
):
    job_id = str(uuid.uuid4())
    resolved_task_type, resolved_payload = _resolve_job_submission(payload, task_type)
    job = create_job(
        db,
        job_id=job_id,
        task_type=resolved_task_type,
        payload=resolved_payload,
    )
    enqueue_job(job_id)
    db.commit()
    db.refresh(job)

    return {
        "message": "Job submitted",
        "job_id": job_id,
        "task_type": resolved_task_type,
        "job": serialize_job(job),
    }


@app.post("/submit-code-job")
async def submit_code_job(
    language: str = Form(...),
    entrypoint: str = Form(...),
    timeout: int = Form(DEFAULT_CODE_TIMEOUT_SECONDS),
    requirements: str | None = Form(default=None),
    files: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    normalized_payload = await _normalize_uploaded_code_job(
        language=language,
        entrypoint=entrypoint,
        timeout=timeout,
        requirements_text=requirements,
        uploads=files,
    )

    job_id = str(uuid.uuid4())
    job = create_job(
        db,
        job_id=job_id,
        task_type="sandbox_code",
        payload=normalized_payload,
    )
    enqueue_job(job_id)
    db.commit()
    db.refresh(job)

    return {
        "message": "Code job submitted",
        "job_id": job_id,
        "task_type": "sandbox_code",
        "job": serialize_job(job),
    }


@app.get("/jobs")
def get_jobs(db: Session = Depends(get_db)):
    return [serialize_job(job) for job in list_jobs(db)]


@app.post("/assign-job")
async def assign_job(db: Session = Depends(get_db)):
    result = assign_next_queued_job(db)
    if not result["assigned"]:
        return result

    await ws_manager.broadcast(
        {
            "event": "job_assigned",
            "job_id": result["job"]["id"],
            "node_id": result["node"]["id"],
        }
    )
    return result


@app.post("/nodes/{node_id}/poll-job")
def poll_job(
    node_id: str,
    db: Session = Depends(get_db),
    x_node_session_token: str | None = Header(default=None),
):
    _require_node_session(node_id, x_node_session_token)
    print(f"[poll] polling job for node {node_id}")
    try:
        node = get_node(db, node_id)
        if node is None:
            raise HTTPException(status_code=404, detail="node not registered")
        if node.status == NodeStatus.OFFLINE.value:
            return {"job": None}

        job = poll_job_for_node(db, node_id=node_id)
        if not job:
            return {"job": None}

        db.commit()
        return {"job": job}
    except Exception as exc:
        db.rollback()
        print(f"[poll] ERROR for node {node_id}: {exc}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="failed to poll job") from exc


@app.post("/jobs/{job_id}/progress")
async def progress_job(
    job_id: str,
    payload: JobProgressPayload,
    db: Session = Depends(get_db),
    x_node_id: str | None = Header(default=None),
    x_node_session_token: str | None = Header(default=None),
):
    if x_node_id:
        _require_node_session(x_node_id, x_node_session_token)
    job = get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status in {JobStatus.COMPLETED.value, JobStatus.FAILED.value}:
        _release_node_if_job_finished(
            db,
            node_id=job.assigned_node_id,
            job_id=job.id,
        )
        db.commit()
        db.refresh(job)
        return {"message": "Job already finished", "job": serialize_job(job)}
    if job.status not in {JobStatus.ASSIGNED.value, JobStatus.RUNNING.value}:
        raise HTTPException(
            status_code=409,
            detail="job must be assigned or running before progress can be reported",
        )

    update_job_progress(db, job, payload.progress)
    db.commit()
    db.refresh(job)
    await ws_manager.broadcast(
        {
            "event": "job_progress",
            "job_id": job.id,
            "progress": job.progress,
            "node_id": job.assigned_node_id,
        }
    )
    return {"message": "Progress updated", "job": serialize_job(job)}


@app.get("/debug/agent-logs/{node_id}")
def get_agent_log(node_id: str):
    log_path = AGENT_LOG_DIR / f"{node_id}.log"
    if not log_path.exists():
        raise HTTPException(status_code=404, detail="agent log not found")
    return {"node_id": node_id, "log": log_path.read_text(encoding="utf-8")}


@app.post("/jobs/{job_id}/complete")
async def complete_job(
    job_id: str,
    payload: JobCompletionPayload | None = Body(default=None),
    db: Session = Depends(get_db),
    x_node_id: str | None = Header(default=None),
    x_node_session_token: str | None = Header(default=None),
):
    if x_node_id:
        _require_node_session(x_node_id, x_node_session_token)
    job = get_job(db, job_id)
    if job is None:
        return {"message": "Job already settled", "job_id": job_id}
    if job.status == JobStatus.COMPLETED.value:
        _release_node_if_job_finished(
            db,
            node_id=job.assigned_node_id,
            job_id=job.id,
        )
        db.commit()
        db.refresh(job)
        return {"message": "Job already completed", "job": serialize_job(job)}
    if job.status not in {JobStatus.ASSIGNED.value, JobStatus.RUNNING.value}:
        raise HTTPException(
            status_code=409,
            detail="job must be assigned or running before completion",
        )

    assigned_node_id = job.assigned_node_id
    if payload is not None:
        job.result = payload.result
    mark_job_completed(db, job)
    _release_node_if_job_finished(
        db,
        node_id=assigned_node_id,
        job_id=job.id,
    )

    db.commit()
    db.refresh(job)
    await ws_manager.broadcast(
        {
            "event": "job_completed",
            "job_id": job.id,
        }
    )
    return {"message": "Job completed", "job": serialize_job(job)}


@app.post("/jobs/{job_id}/fail")
async def fail_job(
    job_id: str,
    payload: JobResultPayload,
    db: Session = Depends(get_db),
    x_node_id: str | None = Header(default=None),
    x_node_session_token: str | None = Header(default=None),
):
    if x_node_id:
        _require_node_session(x_node_id, x_node_session_token)
    job = get_job(db, job_id)
    if job is None:
        return {"message": "Job already settled", "job_id": job_id}
    if payload.status not in {JobStatus.FAILED.value, JobStatus.QUEUED.value}:
        raise HTTPException(
            status_code=400,
            detail="status must be either 'failed' or 'queued'",
        )
    if job.status in {JobStatus.COMPLETED.value, JobStatus.FAILED.value}:
        _release_node_if_job_finished(
            db,
            node_id=job.assigned_node_id,
            job_id=job.id,
        )
        db.commit()
        db.refresh(job)
        return {"message": "Job already finished", "job": serialize_job(job)}
    if job.status not in {JobStatus.ASSIGNED.value, JobStatus.RUNNING.value}:
        raise HTTPException(
            status_code=409,
            detail="job must be assigned or running before failure can be reported",
        )

    assigned_node_id = job.assigned_node_id
    reason = payload.reason or "job execution failed"
    if payload.status == JobStatus.FAILED.value:
        mark_job_failed(db, job, reason=reason)
    else:
        requeue_job(db, job, reason=reason)
        enqueue_job(job.id)

    _release_node_if_job_finished(
        db,
        node_id=assigned_node_id,
        job_id=job.id,
    )

    db.commit()
    db.refresh(job)
    if payload.status == JobStatus.FAILED.value:
        await ws_manager.broadcast(
            {
                "event": "job_failed",
                "job_id": job.id,
            }
        )
    return {"message": "Job updated", "job": serialize_job(job)}


@app.post("/scheduler/reconcile")
def reconcile_cluster(db: Session = Depends(get_db)):
    result = process_stale_nodes(db)
    db.commit()
    return result


@app.post("/debug/clear-nonterminal-jobs")
def clear_active_jobs(db: Session = Depends(get_db)):
    result = clear_nonterminal_jobs(db)
    db.commit()
    return result


@app.post("/debug/repair-redis-state")
def repair_debug_redis_state(db: Session = Depends(get_db)):
    result = repair_redis_state(db)
    db.commit()
    return result
