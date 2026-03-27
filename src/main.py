"""
Main entry point: starts the APScheduler cron job for polling
and the outbox relay loop in a background thread.
"""

from __future__ import annotations

import logging
import sys
import threading
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config import settings
from src.db import check_health
from src.outbox_relay import run_relay_loop
from src.scheduler import run_poll_cycle

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _wait_for_db(max_retries: int = 30, delay: float = 2.0) -> None:
    """Block until PostgreSQL is reachable."""
    for attempt in range(1, max_retries + 1):
        if check_health():
            logger.info("Database is ready")
            return
        logger.warning("Database not ready (attempt %d/%d), retrying in %.0fs", attempt, max_retries, delay)
        time.sleep(delay)
    logger.error("Database not reachable after %d attempts, exiting", max_retries)
    sys.exit(1)


def main() -> None:
    logger.info("CodeSystem poller starting")
    logger.info("  FHIR URL:     %s", settings.fhir_codesystem_url)
    logger.info("  Canonical URL: %s", settings.codesystem_canonical_url)
    logger.info("  Poll cron:     %s", settings.poll_cron)
    logger.info("  Canonical hash: %s", settings.canonical_hash)

    # Wait for PG to be ready
    _wait_for_db()

    # Start outbox relay in a daemon thread
    relay_thread = threading.Thread(target=run_relay_loop, daemon=True, name="outbox-relay")
    relay_thread.start()
    logger.info("Outbox relay thread started")

    # Run an immediate poll on startup, then schedule the cron
    logger.info("Running initial poll cycle on startup")
    try:
        run_poll_cycle()
    except Exception as exc:
        logger.error("Initial poll cycle failed: %s", exc, exc_info=True)

    # APScheduler cron
    scheduler = BlockingScheduler()
    cron_parts = settings.poll_cron.split()
    trigger = CronTrigger(
        minute=cron_parts[0],
        hour=cron_parts[1],
        day=cron_parts[2],
        month=cron_parts[3],
        day_of_week=cron_parts[4],
    )
    scheduler.add_job(run_poll_cycle, trigger, id="poll_cycle", name="CodeSystem poll")
    logger.info("Scheduler started with cron: %s", settings.poll_cron)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down")


if __name__ == "__main__":
    main()
