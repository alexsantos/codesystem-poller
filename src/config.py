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
    # When false, outbox rows are still written (they are the change history)
    # but nothing is published. Re-enabling later delivers the whole backlog.
    relay_enabled: bool = True
    outbox_poll_interval: int = 5  # seconds

    # Safety: skip a CodeSystem whose `count` differs from the number of
    # concepts returned (server truncation/paging). Set true if the source's
    # `count` turns out to be unreliable.
    allow_count_mismatch: bool = False

    # General
    log_level: str = "INFO"
    http_timeout: int = 30

    # extra="ignore": .env also holds compose-only keys (POSTGRES_*, COMPOSE_PROFILES, ...)
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


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
