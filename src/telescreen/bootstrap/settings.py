from __future__ import annotations

import socket

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TELESCREEN_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5433/telescreen"
    kafka_bootstrap_servers: str = "localhost:9094"
    log_level: str = "INFO"
    # Identifies this worker on the status page.
    worker_id: str = socket.gethostname()
    # How often the worker re-reads pipeline definitions.
    poll_interval: float = 2.0
    # How often a running pipeline reports its slot lag.
    status_interval: float = 5.0
