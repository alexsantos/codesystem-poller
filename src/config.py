"""Application configuration loaded from environment variables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

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
    # Where the relay delivers Bundles: the RabbitMQ exchange, or directly to
    # a FHIR $process-message endpoint (FHIR_TARGET_URL) without RabbitMQ.
    relay_target: Literal["rabbitmq", "fhir"] = "rabbitmq"
    relay_batch_size: int = 500        # max outbox rows (changes) per Bundle
    relay_retry_base: int = 5          # seconds; backoff doubles per failed attempt
    relay_retry_max: int = 900         # seconds; backoff cap (retries never stop)
    # What to do when the target permanently rejects a Bundle (e.g. HTTP 400/422):
    #   dead_letter: park the batch as 'dead' and keep delivering later batches
    #   block:       park it and hold back later batches of the same CodeSystem
    #                until it is requeued or discarded (strict ordering)
    relay_on_reject: Literal["dead_letter", "block"] = "dead_letter"

    # Direct FHIR target (RELAY_TARGET=fhir). Same variables as the forwarder.
    fhir_target_url: str = ""
    fhir_auth_token: str = ""
    fhir_auth_user: str = ""
    fhir_auth_password: str = ""

    # Safety: skip a CodeSystem whose `count` differs from the number of
    # concepts returned (server truncation/paging). Set true if the source's
    # `count` turns out to be unreliable.
    allow_count_mismatch: bool = False

    # General
    log_level: str = "INFO"
    http_timeout: int = 30
    http_retries: int = 3  # retries (after the first attempt) on network errors, 5xx, 429

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
