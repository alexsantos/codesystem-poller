"""
Differ: flattens a FHIR CodeSystem concept hierarchy and diffs it
against the stored concept state in PostgreSQL.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


# ─── Flattening ──────────────────────────────────────────────────────────

def flatten_concepts(
    concepts: list[dict],
    parent_code: str | None = None,
) -> dict[str, dict]:
    """
    Recursively flatten the concept[] hierarchy into a dict keyed by code.

    Each entry has:
        code, display, definition, properties (merged property[] + designation[]),
        parent_code, concept_hash
    """
    result: dict[str, dict] = {}

    for concept in concepts:
        code = concept.get("code")
        if not code:
            logger.warning("Concept without code encountered, skipping: %s", concept)
            continue

        # Merge property[] and designation[] into a single dict
        properties: dict[str, Any] = {}
        for prop in concept.get("property", []):
            key = prop.get("code", "unknown")
            # FHIR property values come in value[x] fields
            value = (
                prop.get("valueCode")
                or prop.get("valueString")
                or prop.get("valueCoding")
                or prop.get("valueInteger")
                or prop.get("valueBoolean")
                or prop.get("valueDateTime")
                or prop.get("valueDecimal")
            )
            properties[key] = value

        designations = []
        for des in concept.get("designation", []):
            designations.append({
                "language": des.get("language"),
                "use": des.get("use"),
                "value": des.get("value"),
            })
        if designations:
            properties["_designations"] = designations

        flat = {
            "code": code,
            "display": concept.get("display"),
            "definition": concept.get("definition"),
            "properties": properties,
            "parent_code": parent_code,
        }
        flat["concept_hash"] = _hash_concept(flat)
        result[code] = flat

        # Recurse into children
        children = concept.get("concept", [])
        if children:
            child_flat = flatten_concepts(children, parent_code=code)
            result.update(child_flat)

    return result


def _hash_concept(flat: dict) -> str:
    """SHA-256 of the canonical JSON of a flattened concept."""
    canonical = json.dumps(
        {
            "code": flat["code"],
            "display": flat.get("display"),
            "definition": flat.get("definition"),
            "properties": flat.get("properties", {}),
            "parent_code": flat.get("parent_code"),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ─── Loading stored state ───────────────────────────────────────────────

def load_stored_concepts(cur, system_url: str) -> dict[str, dict]:
    """Load all concept rows for a given system_url into a dict keyed by code."""
    cur.execute(
        """
        SELECT code, display, definition, concept_hash, properties, parent_code
        FROM poller.codesystem_concept_state
        WHERE system_url = %s
        """,
        (system_url,),
    )
    result = {}
    for row in cur.fetchall():
        result[row["code"]] = {
            "code": row["code"],
            "display": row["display"],
            "definition": row["definition"],
            "concept_hash": row["concept_hash"],
            "properties": row["properties"] or {},
            "parent_code": row["parent_code"],
        }
    return result


# ─── Diffing ─────────────────────────────────────────────────────────────

def diff_concepts(
    fresh: dict[str, dict],
    stored: dict[str, dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Compare fresh (from API) against stored (from PG).

    Returns (added, modified, removed) where:
      - added: list of flat concept dicts (new codes)
      - modified: list of dicts with code, old_concept, new_concept, changed_fields
      - removed: list of flat concept dicts (codes no longer present)
    """
    fresh_codes = set(fresh.keys())
    stored_codes = set(stored.keys())

    added = [fresh[c] for c in sorted(fresh_codes - stored_codes)]
    removed = [stored[c] for c in sorted(stored_codes - fresh_codes)]

    modified = []
    for code in sorted(fresh_codes & stored_codes):
        f = fresh[code]
        s = stored[code]
        if f["concept_hash"] != s["concept_hash"]:
            changed_fields = _field_diff(s, f)
            if changed_fields:
                modified.append({
                    "code": code,
                    "old_concept": s,
                    "new_concept": f,
                    "changed_fields": changed_fields,
                })

    logger.info(
        "Diff result: %d added, %d modified, %d removed",
        len(added), len(modified), len(removed),
    )
    return added, modified, removed


def _field_diff(old: dict, new: dict) -> dict[str, tuple[Any, Any]]:
    """Compare individual fields, return {field: (old_val, new_val)} for differences."""
    fields = ["display", "definition", "properties", "parent_code"]
    changes = {}
    for field in fields:
        old_val = old.get(field)
        new_val = new.get(field)
        if old_val != new_val:
            changes[field] = (old_val, new_val)
    return changes


# ─── Persisting new state ───────────────────────────────────────────────

def persist_state(
    cur,
    system_url: str,
    version: str | None,
    resource_hash: str,
    resource_json: dict,
    fresh_concepts: dict[str, dict],
    added: list[dict],
    modified: list[dict],
    removed: list[dict],
) -> None:
    """
    In the CURRENT transaction (caller manages commit):
      1. UPSERT codesystem_sync_state
      2. UPSERT/DELETE codesystem_concept_state
      3. INSERT change_outbox rows
    """
    # 1. Sync state
    cur.execute(
        """
        INSERT INTO poller.codesystem_sync_state (system_url, version, resource_hash, resource_json, synced_at)
        VALUES (%s, %s, %s, %s::jsonb, now())
        ON CONFLICT (system_url) DO UPDATE SET
            version = EXCLUDED.version,
            resource_hash = EXCLUDED.resource_hash,
            resource_json = EXCLUDED.resource_json,
            synced_at = now()
        """,
        (system_url, version, resource_hash, json.dumps(resource_json)),
    )

    # 2a. Upsert all fresh concepts
    for concept in fresh_concepts.values():
        cur.execute(
            """
            INSERT INTO poller.codesystem_concept_state
                (system_url, code, display, definition, concept_hash, properties, parent_code, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, now())
            ON CONFLICT (system_url, code) DO UPDATE SET
                display = EXCLUDED.display,
                definition = EXCLUDED.definition,
                concept_hash = EXCLUDED.concept_hash,
                properties = EXCLUDED.properties,
                parent_code = EXCLUDED.parent_code,
                updated_at = now()
            """,
            (
                system_url,
                concept["code"],
                concept.get("display"),
                concept.get("definition"),
                concept["concept_hash"],
                json.dumps(concept.get("properties", {})),
                concept.get("parent_code"),
            ),
        )

    # 2b. Delete removed concepts
    for concept in removed:
        cur.execute(
            "DELETE FROM poller.codesystem_concept_state WHERE system_url = %s AND code = %s",
            (system_url, concept["code"]),
        )

    # 3. Outbox rows
    for concept in added:
        cur.execute(
            """
            INSERT INTO poller.change_outbox (system_url, change_type, code, old_value, new_value)
            VALUES (%s, 'concept_added', %s, NULL, %s::jsonb)
            """,
            (system_url, concept["code"], json.dumps(concept)),
        )

    for change in modified:
        cur.execute(
            """
            INSERT INTO poller.change_outbox (system_url, change_type, code, old_value, new_value)
            VALUES (%s, 'concept_modified', %s, %s::jsonb, %s::jsonb)
            """,
            (
                system_url,
                change["code"],
                json.dumps(change["old_concept"]),
                json.dumps(change["new_concept"]),
            ),
        )

    for concept in removed:
        cur.execute(
            """
            INSERT INTO poller.change_outbox (system_url, change_type, code, old_value, new_value)
            VALUES (%s, 'concept_removed', %s, %s::jsonb, NULL)
            """,
            (system_url, concept["code"], json.dumps(concept)),
        )

    total = len(added) + len(modified) + len(removed)
    logger.info("Persisted state + %d outbox rows in transaction", total)
