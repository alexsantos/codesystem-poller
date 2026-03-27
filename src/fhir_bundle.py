"""
FHIR R4 Message Bundle builder for CodeSystem change notifications.

Produces a Bundle of type 'message' containing:
  - A MessageHeader with an event coding describing the change batch
  - One Parameters resource per detected change (added/modified/removed concept)
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

ChangeType = Literal["concept_added", "concept_modified", "concept_removed"]

# ── Configuration (override these for your organisation) ─────────────────
EVENT_SYSTEM = "https://your-org.example/fhir/events"
EVENT_CODE = "codesystem-change"
EVENT_DISPLAY = "CodeSystem Change Notification"
SOURCE_NAME = "codesystem-polling-service"
SOURCE_ENDPOINT = "https://your-org.example/fhir/polling"


def _uuid_urn() -> str:
    return f"urn:uuid:{uuid.uuid4()}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_compact(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return _json_compact(value)
    return str(value)


# ── Parameter helpers ────────────────────────────────────────────────────

def _param(name: str, value: Any, value_type: str = "valueString") -> dict | None:
    if value is None:
        return None
    return {"name": name, value_type: str(value) if value_type == "valueString" else value}


def _concept_params_added(system_url: str, version: str | None, concept: dict) -> tuple[str, dict]:
    resource_id = _uuid_urn()
    params = [
        _param("changeType", "concept_added"),
        _param("system", system_url, "valueUri"),
        _param("version", version),
        _param("code", concept.get("code"), "valueCode"),
        _param("display", concept.get("display")),
        _param("definition", concept.get("definition")),
    ]
    if concept.get("properties"):
        params.append(_param("properties", _json_compact(concept["properties"])))
    if concept.get("parent_code"):
        params.append(_param("parentCode", concept["parent_code"], "valueCode"))
    return resource_id, {
        "resourceType": "Parameters",
        "id": resource_id,
        "parameter": [p for p in params if p is not None],
    }


def _concept_params_removed(system_url: str, version: str | None, concept: dict) -> tuple[str, dict]:
    resource_id = _uuid_urn()
    params = [
        _param("changeType", "concept_removed"),
        _param("system", system_url, "valueUri"),
        _param("version", version),
        _param("code", concept.get("code"), "valueCode"),
        _param("display", concept.get("display")),
    ]
    return resource_id, {
        "resourceType": "Parameters",
        "id": resource_id,
        "parameter": [p for p in params if p is not None],
    }


def _concept_params_modified(
    system_url: str,
    version: str | None,
    code: str,
    old_concept: dict,
    new_concept: dict,
    changed_fields: dict[str, tuple[Any, Any]],
) -> tuple[str, dict]:
    resource_id = _uuid_urn()
    params = [
        _param("changeType", "concept_modified"),
        _param("system", system_url, "valueUri"),
        _param("version", version),
        _param("code", code, "valueCode"),
    ]
    for field, (old_val, new_val) in changed_fields.items():
        params.append({
            "name": "change",
            "part": [
                p for p in [
                    _param("field", field),
                    _param("oldValue", _to_str(old_val)),
                    _param("newValue", _to_str(new_val)),
                ] if p is not None
            ],
        })
    return resource_id, {
        "resourceType": "Parameters",
        "id": resource_id,
        "parameter": [p for p in params if p is not None],
    }


# ── MessageHeader ────────────────────────────────────────────────────────

def _build_message_header(
    focus_references: list[str],
    system_url: str,
    timestamp: str,
) -> tuple[str, dict]:
    resource_id = _uuid_urn()
    return resource_id, {
        "resourceType": "MessageHeader",
        "id": resource_id,
        "eventCoding": {
            "system": EVENT_SYSTEM,
            "code": EVENT_CODE,
            "display": EVENT_DISPLAY,
        },
        "source": {
            "name": SOURCE_NAME,
            "endpoint": SOURCE_ENDPOINT,
        },
        "focus": [{"reference": ref} for ref in focus_references],
        "definition": system_url,
        "meta": {"lastUpdated": timestamp},
    }


# ── Bundle assembler ────────────────────────────────────────────────────

def build_change_bundle(
    system_url: str,
    version: str | None,
    added: list[dict],
    modified: list[dict],
    removed: list[dict],
) -> dict | None:
    """
    Build a FHIR R4 message Bundle for a batch of CodeSystem concept changes.

    Parameters
    ----------
    system_url : Canonical URL of the CodeSystem
    version : CodeSystem version (if available)
    added : Flattened concepts that are new
    modified : Dicts with code, old_concept, new_concept, changed_fields
    removed : Flattened concepts that were removed

    Returns None if there are no changes.
    """
    timestamp = _now_iso()
    focus_entries: list[tuple[str, dict]] = []

    for concept in added:
        focus_entries.append(_concept_params_added(system_url, version, concept))

    for change in modified:
        focus_entries.append(_concept_params_modified(
            system_url, version,
            change["code"], change["old_concept"], change["new_concept"], change["changed_fields"],
        ))

    for concept in removed:
        focus_entries.append(_concept_params_removed(system_url, version, concept))

    if not focus_entries:
        return None

    focus_refs = [entry_id for entry_id, _ in focus_entries]
    header_id, header_resource = _build_message_header(focus_refs, system_url, timestamp)

    entries = [{"fullUrl": header_id, "resource": header_resource}]
    for entry_id, resource in focus_entries:
        entries.append({"fullUrl": entry_id, "resource": resource})

    return {
        "resourceType": "Bundle",
        "id": str(uuid.uuid4()),
        "type": "message",
        "timestamp": timestamp,
        "meta": {"lastUpdated": timestamp},
        "entry": entries,
    }
