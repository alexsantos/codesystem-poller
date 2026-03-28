"""
Poller: fetches the FHIR CodeSystem resource and checks whether
the content has changed by comparing SHA-256 hashes.
"""

from __future__ import annotations

import hashlib
import json
import logging

import httpx

from src.config import settings

logger = logging.getLogger(__name__)


def fetch_codesystem(url: str) -> tuple[bytes, dict] | None:
    """
    GET the FHIR CodeSystem endpoint.
    Returns (raw_bytes, parsed_json) or None on failure.
    """
    try:
        with httpx.Client(timeout=settings.http_timeout) as client:
            resp = client.get(
                url,
                headers={"Accept": "application/fhir+json"},
            )
            resp.raise_for_status()
            raw = resp.content
            parsed = resp.json()
            logger.info(
                "Fetched CodeSystem: %d bytes, resourceType=%s",
                len(raw),
                parsed.get("resourceType"),
            )
            return raw, parsed
    except httpx.HTTPStatusError as exc:
        logger.error("FHIR API returned %s: %s", exc.response.status_code, exc)
        return None
    except httpx.RequestError as exc:
        logger.error("FHIR API request failed: %s", exc)
        return None


def compute_hash(raw: bytes, parsed: dict | None = None) -> str:
    """
    Compute SHA-256 of the CodeSystem content.

    If CANONICAL_HASH is enabled, parses JSON, sorts keys, and hashes the
    canonical form (eliminates false positives from non-deterministic JSON).
    Otherwise, hashes the raw bytes (faster, skips parsing on no-change cycles).
    """
    if settings.canonical_hash and parsed is not None:
        canonical = json.dumps(
            parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()
    return hashlib.sha256(raw).hexdigest()


def get_stored_hash(cur, system_url: str) -> str | None:
    """Fetch the last stored resource_hash from PG."""
    cur.execute(
        "SELECT resource_hash FROM poller.codesystem_sync_state WHERE system_url = %s",
        (system_url,),
    )
    row = cur.fetchone()
    return row["resource_hash"] if row else None
