"""
Main entry point: starts the APScheduler cron job for polling
and the outbox relay loop in a background thread.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config import load_codesystems, settings
from src.db import check_health
from src.outbox_relay import run_relay_loop
from src.scheduler import run_poll_cycle

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_shutdown = threading.Event()


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


def _relay_loop_with_shutdown() -> None:
    """Wrapper around run_relay_loop that exits when _shutdown is set."""
    logger.info("Outbox relay started (interval=%ds)", settings.outbox_poll_interval)
    while not _shutdown.is_set():
        try:
            from src.outbox_relay import relay_once
            relay_once()
        except Exception as exc:
            logger.error("Relay loop error: %s", exc, exc_info=True)
        _shutdown.wait(timeout=settings.outbox_poll_interval)
    logger.info("Outbox relay stopped")


def main() -> None:
    logger.info("CodeSystem poller starting")
    codesystems = load_codesystems()
    logger.info("  Config:         %s (%d CodeSystem(s))", settings.codesystems_config, len(codesystems))
    for entry in codesystems:
        logger.info("    - %s", entry.canonical_url)
    logger.info("  Poll cron:      %s", settings.poll_cron)
    logger.info("  Canonical hash: %s", settings.canonical_hash)

    # Wait for PG to be ready
    _wait_for_db()

    # Graceful shutdown on SIGTERM (sent by Docker/Kubernetes on stop)
    def _handle_signal(signum, frame):
        logger.info("Received signal %d, shutting down", signum)
        _shutdown.set()

    signal.signal(signal.SIGTERM, _handle_signal)

    # Start outbox relay in a non-daemon thread so it can finish cleanly
    relay_thread = threading.Thread(target=_relay_loop_with_shutdown, name="outbox-relay")
    relay_thread.start()

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
    finally:
        _shutdown.set()
        relay_thread.join(timeout=10)
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
