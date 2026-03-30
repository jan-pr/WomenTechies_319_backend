"""Root entrypoint for launching the worker agent."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def main() -> int:
    """Load and run the worker entrypoint from the agent package directory."""
    agent_path = Path(__file__).resolve().parent / "agent" / "agent.py"
    agent_dir = str(agent_path.parent)
    if agent_dir not in sys.path:
        sys.path.insert(0, agent_dir)
    spec = importlib.util.spec_from_file_location("worker_agent_main", agent_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load worker agent entrypoint")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.main()


if __name__ == "__main__":
    raise SystemExit(main())
