"""Application configuration loaded from environment variables."""

from __future__ import annotations

from dataclasses import dataclass

import yaml
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # CodeSystems config file
    codesystems_config: str = "/app/codesystems.yml"

    # Schedule
    poll_cron: str = "0 */4 * * *"

    # Hashing
    canonical_hash: bool = False

    # PostgreSQL
    database_url: str = "postgresql://poller:poller@db:5432/codesystem_poller"

    # RabbitMQ
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    rabbitmq_exchange: str = "codesystem.changes"

    # Outbox relay
    outbox_poll_interval: int = 5  # seconds

    # General
    log_level: str = "INFO"
    http_timeout: int = 30

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@dataclass
class CodeSystemEntry:
    url: str
    canonical_url: str


def load_codesystems() -> list[CodeSystemEntry]:
    """Load the list of CodeSystems to monitor from the YAML config file."""
    with open(settings.codesystems_config) as f:
        data = yaml.safe_load(f)

    entries = []
    for item in data.get("codesystems", []):
        url = item["url"]
        canonical_url = item.get("canonical_url", url)
        entries.append(CodeSystemEntry(url=url, canonical_url=canonical_url))

    if not entries:
        raise ValueError(f"No codesystems defined in {settings.codesystems_config}")

    return entries


settings = Settings()  # type: ignore[call-arg]
