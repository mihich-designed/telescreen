from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TELESCREEN_", env_file=".env", extra="ignore")

    pipelines_file: Path = Path("pipelines.yaml")
    kafka_bootstrap_servers: str = "localhost:9094"
    log_level: str = "INFO"
