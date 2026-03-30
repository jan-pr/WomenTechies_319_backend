"""Configuration loading for the worker agent."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(slots=True)
class AgentConfig:
    """Runtime configuration for the worker agent."""

    backend_url: str
    node_id: str
    carbon_zone: str | None = None
    heartbeat_interval_seconds: int = 5
    request_timeout_seconds: int = 10


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the worker agent."""
    parser = argparse.ArgumentParser(description="Distributed compute worker agent")
    parser.add_argument(
        "--node-id",
        dest="node_id",
        help="Override NODE_ID from environment",
    )
    return parser.parse_args()


def load_config(cli_node_id: str | None = None) -> AgentConfig:
    """Load and validate configuration from environment and CLI."""
    backend_url = os.getenv("BACKEND_URL")
    node_id = cli_node_id or os.getenv("NODE_ID")
    carbon_zone = os.getenv("CARBON_ZONE")

    if not backend_url:
        raise ValueError("BACKEND_URL is required")
    if not node_id:
        raise ValueError("NODE_ID is required or pass --node-id")

    return AgentConfig(
        backend_url=backend_url.rstrip("/"),
        node_id=node_id,
        carbon_zone=carbon_zone,
    )
