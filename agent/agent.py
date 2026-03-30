"""Single-threaded worker agent for the distributed compute system."""

from __future__ import annotations

import signal
import time
from typing import Any

try:
    from agent.api_client import (
        complete_job,
        fail_job,
        poll_job,
        register_node,
        report_progress,
        send_heartbeat,
    )
    from agent.config import AgentConfig, load_config, parse_args
    from agent.executor import execute_job
except ModuleNotFoundError:  # pragma: no cover - script execution fallback
    from api_client import (
        complete_job,
        fail_job,
        poll_job,
        register_node,
        report_progress,
        send_heartbeat,
    )
    from config import AgentConfig, load_config, parse_args
    from executor import execute_job


class WorkerAgent:
    """A production-style single-threaded worker that processes one job at a time."""

    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self.current_job: dict[str, Any] | None = None
        self.current_progress = 0
        self.running = True
        self.last_heartbeat_at = 0.0

    def register(self) -> None:
        """Register this worker with the backend."""
        node_data = {
            "node_id": self.config.node_id,
            "status": "idle",
            "carbon_zone": self.config.carbon_zone,
            "metrics": {
                "success_rate": 1.0,
                "uptime": 1.0,
                "speed": 1.0,
            },
            "cpu": 0,
        }
        response = register_node(self.config, node_data)
        print(f"[agent] registered node {self.config.node_id}: {response}")

    def send_status_heartbeat(self, status: str) -> None:
        """Send a heartbeat reflecting the current state."""
        current_job_id = None
        if self.current_job:
            current_job_id = self.current_job.get("id")

        response = send_heartbeat(
            self.config,
            node_id=self.config.node_id,
            status=status,
            current_job_id=current_job_id,
            progress=self.current_progress,
        )
        self.last_heartbeat_at = time.time()

        print(
            f"[agent] heartbeat status={status} "
            f"job={current_job_id} progress={self.current_progress}: {response}"
        )

    def _maybe_send_heartbeat(self, status: str) -> None:
        """Send heartbeat when the configured interval has elapsed."""
        if time.time() - self.last_heartbeat_at >= self.config.heartbeat_interval_seconds:
            self.send_status_heartbeat(status)

    def _progress_callback(self, progress: int) -> None:
        """Report real execution progress back to the backend."""
        self.current_progress = progress

        if not self.current_job:
            return

        job_id = self.current_job.get("id")
        if not job_id:
            print(f"[agent] invalid job during progress: {self.current_job}")
            return

        print(f"[agent] progress job={job_id} progress={progress}%")

        try:
            report_progress(self.config, job_id, progress)
        except Exception as exc:
            print(f"[agent] progress reporting failed for job={job_id}: {exc}")

        self._maybe_send_heartbeat("busy")

    def _report_terminal_state(self, reporter, job_id: str, payload: Any) -> None:
        """Retry terminal job reporting until it succeeds or the agent is stopped."""
        while self.running:
            try:
                reporter(self.config, job_id, payload)
                return
            except Exception as exc:
                print(f"[agent] terminal reporting failed for job={job_id}: {exc}")
                time.sleep(2)

        raise RuntimeError(f"agent stopped before reporting terminal state for job={job_id}")

    def _execute_current_job(self) -> None:
        """Execute the current job and report success or failure."""
        if self.current_job is None:
            return

        job_id = self.current_job.get("id")
        if not job_id:
            print(f"[agent] invalid job payload: {self.current_job}")
            self.current_job = None
            return

        task_type = self.current_job.get("task_type")
        self.current_progress = 0
        print(f"[agent] starting job={job_id} task_type={task_type}")

        try:
            result = execute_job(self.current_job, self._progress_callback)
            self.current_progress = 100
            self._report_terminal_state(complete_job, job_id, result)
            print(f"[agent] completed job={job_id} result={result}")
        except Exception as exc:
            error_message = str(exc)
            self._report_terminal_state(fail_job, job_id, error_message)
            print(f"[agent] failed job={job_id}: {error_message}")
        finally:
            self.current_job = None
            self.current_progress = 0
            self.send_status_heartbeat("idle")

    def run(self) -> None:
        """Run the main worker loop."""
        self.register()
        self.send_status_heartbeat("idle")

        while self.running:
            try:
                current_status = "busy" if self.current_job else "idle"
                self._maybe_send_heartbeat(current_status)

                if self.current_job is None:
                    response = poll_job(self.config, self.config.node_id)
                    job = response.get("job") if isinstance(response, dict) else None

                    if not job:
                        print("[agent] no job available")
                        time.sleep(3)
                        continue

                    print(f"[agent] received job={job.get('id')} payload={job}")
                    self.current_job = job
                    self.current_progress = 0
                    self.send_status_heartbeat("busy")
                    self._execute_current_job()
                    continue

                self._execute_current_job()
            except Exception as exc:
                print(f"[agent] main loop error: {exc}")
                time.sleep(2)

        self.shutdown()

    def shutdown(self) -> None:
        """Attempt a clean offline transition on shutdown."""
        print("[agent] shutting down")

        current_job_id = None
        if self.current_job:
            current_job_id = self.current_job.get("id")

        try:
            send_heartbeat(
                self.config,
                node_id=self.config.node_id,
                status="offline",
                current_job_id=current_job_id,
                progress=self.current_progress,
            )
        except Exception as exc:
            print(f"[agent] failed to mark node offline: {exc}")


def main() -> int:
    """CLI entrypoint for the worker agent."""
    args = parse_args()
    config = load_config(cli_node_id=args.node_id)
    agent = WorkerAgent(config)

    def _handle_signal(signum, _frame) -> None:
        print(f"[agent] received signal {signum}, stopping")
        agent.running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        agent.run()
    except KeyboardInterrupt:
        agent.running = False
        agent.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
