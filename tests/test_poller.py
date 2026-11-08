"""Tests for fetch_codesystem retries (httpx.MockTransport, no real HTTP)."""

import json
from unittest.mock import patch

import httpx
import pytest

from src import poller
from src.config import settings

URL = "https://fhir.example/fhir/CodeSystem/Test"
BODY = {"resourceType": "CodeSystem", "concept": []}


def transport(*responses):
    """MockTransport returning the given statuses (or exceptions) in order."""
    calls = []
    seq = list(responses)

    def handler(request):
        calls.append(request)
        item = seq.pop(0)
        if isinstance(item, Exception):
            raise item
        if item == 200:
            return httpx.Response(200, json=BODY)
        return httpx.Response(item, text="error")

    return httpx.MockTransport(handler), calls


@pytest.fixture(autouse=True)
def no_sleep():
    with patch.object(poller.time, "sleep") as s:
        yield s


def test_success_first_try():
    t, calls = transport(200)
    raw, parsed = poller.fetch_codesystem(URL, transport=t)
    assert parsed == BODY
    assert json.loads(raw) == BODY
    assert len(calls) == 1


def test_503_503_200_succeeds_with_backoff(no_sleep):
    t, calls = transport(503, 503, 200)
    assert poller.fetch_codesystem(URL, transport=t) is not None
    assert len(calls) == 3
    assert [c.args[0] for c in no_sleep.call_args_list] == [2, 8]


def test_429_and_network_error_are_retried():
    t, calls = transport(429, httpx.ConnectError("boom"), 200)
    assert poller.fetch_codesystem(URL, transport=t) is not None
    assert len(calls) == 3


def test_404_not_retried(no_sleep):
    t, calls = transport(404)
    assert poller.fetch_codesystem(URL, transport=t) is None
    assert len(calls) == 1
    no_sleep.assert_not_called()


def test_gives_up_after_http_retries(monkeypatch, no_sleep):
    monkeypatch.setattr(settings, "http_retries", 3)
    t, calls = transport(500, 500, 500, 500)
    assert poller.fetch_codesystem(URL, transport=t) is None
    assert len(calls) == 4
    assert [c.args[0] for c in no_sleep.call_args_list] == [2, 8, 30]


def test_zero_retries(monkeypatch):
    monkeypatch.setattr(settings, "http_retries", 0)
    t, calls = transport(503)
    assert poller.fetch_codesystem(URL, transport=t) is None
    assert len(calls) == 1


def test_non_json_body_returns_none():
    t = httpx.MockTransport(lambda r: httpx.Response(200, text="<html>"))
    assert poller.fetch_codesystem(URL, transport=t) is None
