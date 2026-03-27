"""Application configuration loaded from environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # FHIR source
    fhir_codesystem_url: str
    codesystem_canonical_url: str

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


settings = Settings()  # type: ignore[call-arg]
