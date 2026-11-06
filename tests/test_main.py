"""Tests for main(): the relay thread is only started when enabled."""

from unittest.mock import MagicMock, patch

import pytest

import src.main as main_mod
from src.config import CodeSystemEntry, settings


@pytest.fixture
def patched_main(monkeypatch):
    monkeypatch.setattr(settings, "poll_cron", "0 3 * * *")
    with patch.object(main_mod, "load_codesystems",
                      return_value=[CodeSystemEntry(url="http://x/CodeSystem/A", canonical_url="http://x/CodeSystem/A")]), \
         patch.object(main_mod, "_wait_for_db"), \
         patch.object(main_mod, "run_poll_cycle"), \
         patch.object(main_mod, "BlockingScheduler") as sched_cls, \
         patch.object(main_mod.signal, "signal"), \
         patch.object(main_mod.threading, "Thread") as thread_cls:
        sched_cls.return_value.start.side_effect = KeyboardInterrupt
        yield thread_cls


def test_relay_started_when_enabled(monkeypatch, patched_main):
    monkeypatch.setattr(settings, "relay_enabled", True)
    main_mod.main()
    patched_main.assert_called_once()
    patched_main.return_value.start.assert_called_once()
    patched_main.return_value.join.assert_called_once()


def test_relay_not_started_when_disabled(monkeypatch, patched_main):
    monkeypatch.setattr(settings, "relay_enabled", False)
    main_mod.main()
    patched_main.assert_not_called()
