"""Tests for the fhir_bundle module: Bundle and Parameters construction."""

import json
import re

import pytest

from src.fhir_bundle import (
    _build_message_header,
    _concept_params_added,
    _concept_params_modified,
    _concept_params_removed,
    _json_compact,
    _param,
    _to_str,
    _uuid_urn,
    build_change_bundle,
)

# ── Fixtures ─────────────────────────────────────────────────────────────

SYSTEM_URL = "https://example.org/fhir/CodeSystem/Test"
VERSION = "1.0.0"

ADDED_CONCEPT = {
    "code": "A001",
    "display": "Alpha",
    "definition": "First concept",
    "properties": {"category": "general"},
    "parent_code": None,
    "concept_hash": "abc123",
}

REMOVED_CONCEPT = {
    "code": "R001",
    "display": "Removed",
    "definition": "Was here",
    "properties": {},
    "parent_code": None,
    "concept_hash": "def456",
}

MODIFIED_CHANGE = {
    "code": "M001",
    "old_concept": {"code": "M001", "display": "Old Name"},
    "new_concept": {"code": "M001", "display": "New Name"},
    "changed_fields": {"display": ("Old Name", "New Name")},
}


def _get_params(bundle: dict, resource_type: str = "Parameters") -> list[dict]:
    """Extract all entries of a given resourceType from a Bundle."""
    return [
        e["resource"]
        for e in bundle["entry"]
        if e["resource"]["resourceType"] == resource_type
    ]


def _param_value(params: list[dict], name: str) -> object:
    """Find a parameter by name and return its value (any value[x] key)."""
    for p in params:
        if p["name"] == name:
            for key, val in p.items():
                if key != "name":
                    return val
    return None


# ── Helper functions ─────────────────────────────────────────────────────

class TestUuidUrn:
    def test_format(self):
        result = _uuid_urn()
        assert result.startswith("urn:uuid:")
        uuid_part = result[len("urn:uuid:"):]
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            uuid_part,
        )

    def test_unique(self):
        assert _uuid_urn() != _uuid_urn()


class TestJsonCompact:
    def test_compact_output(self):
        result = _json_compact({"b": 2, "a": 1})
        assert result == '{"a":1,"b":2}'

    def test_no_spaces(self):
        assert " " not in _json_compact({"key": "value"})


class TestToStr:
    def test_none_returns_none(self):
        assert _to_str(None) is None

    def test_string_passthrough(self):
        assert _to_str("hello") == "hello"

    def test_int_to_str(self):
        assert _to_str(42) == "42"

    def test_dict_to_compact_json(self):
        result = _to_str({"b": 2, "a": 1})
        assert result == '{"a":2,"b":1}' or json.loads(result) == {"b": 2, "a": 1}

    def test_list_to_compact_json(self):
        result = _to_str([1, 2, 3])
        assert json.loads(result) == [1, 2, 3]


class TestParam:
    def test_none_value_returns_none(self):
        assert _param("name", None) is None

    def test_string_value(self):
        p = _param("display", "Hello")
        assert p == {"name": "display", "valueString": "Hello"}

    def test_custom_value_type(self):
        p = _param("code", "A001", "valueCode")
        assert p == {"name": "code", "valueCode": "A001"}

    def test_uri_value_type(self):
        p = _param("system", "http://example.org", "valueUri")
        assert p == {"name": "system", "valueUri": "http://example.org"}


# ── Parameters builders ──────────────────────────────────────────────────

class TestConceptParamsAdded:
    def setup_method(self):
        self.resource_id, self.resource = _concept_params_added(
            SYSTEM_URL, VERSION, ADDED_CONCEPT
        )
        self.params = self.resource["parameter"]

    def test_resource_type(self):
        assert self.resource["resourceType"] == "Parameters"

    def test_id_is_urn(self):
        assert self.resource["id"].startswith("urn:uuid:")

    def test_change_type(self):
        assert _param_value(self.params, "changeType") == "concept_added"

    def test_system(self):
        assert _param_value(self.params, "system") == SYSTEM_URL

    def test_version(self):
        assert _param_value(self.params, "version") == VERSION

    def test_code(self):
        assert _param_value(self.params, "code") == "A001"

    def test_display(self):
        assert _param_value(self.params, "display") == "Alpha"

    def test_definition(self):
        assert _param_value(self.params, "definition") == "First concept"

    def test_properties_included_when_present(self):
        assert _param_value(self.params, "properties") is not None

    def test_no_properties_when_empty(self):
        _, resource = _concept_params_added(SYSTEM_URL, VERSION, {
            "code": "X", "display": "X", "properties": {}
        })
        names = [p["name"] for p in resource["parameter"]]
        assert "properties" not in names

    def test_parent_code_included_when_present(self):
        _, resource = _concept_params_added(SYSTEM_URL, VERSION, {
            **ADDED_CONCEPT, "parent_code": "PARENT"
        })
        names = [p["name"] for p in resource["parameter"]]
        assert "parentCode" in names

    def test_no_none_params(self):
        assert all(p is not None for p in self.params)

    def test_version_none_omitted(self):
        _, resource = _concept_params_added(SYSTEM_URL, None, ADDED_CONCEPT)
        names = [p["name"] for p in resource["parameter"]]
        assert "version" not in names


class TestConceptParamsRemoved:
    def setup_method(self):
        _, self.resource = _concept_params_removed(SYSTEM_URL, VERSION, REMOVED_CONCEPT)
        self.params = self.resource["parameter"]

    def test_change_type(self):
        assert _param_value(self.params, "changeType") == "concept_removed"

    def test_code_present(self):
        assert _param_value(self.params, "code") == "R001"

    def test_display_present(self):
        assert _param_value(self.params, "display") == "Removed"

    def test_no_none_params(self):
        assert all(p is not None for p in self.params)


class TestConceptParamsModified:
    def setup_method(self):
        _, self.resource = _concept_params_modified(
            SYSTEM_URL, VERSION,
            MODIFIED_CHANGE["code"],
            MODIFIED_CHANGE["old_concept"],
            MODIFIED_CHANGE["new_concept"],
            MODIFIED_CHANGE["changed_fields"],
        )
        self.params = self.resource["parameter"]

    def test_change_type(self):
        assert _param_value(self.params, "changeType") == "concept_modified"

    def test_code_present(self):
        assert _param_value(self.params, "code") == "M001"

    def test_change_entry_present(self):
        change_params = [p for p in self.params if p["name"] == "change"]
        assert len(change_params) == 1

    def test_change_parts_have_field_old_new(self):
        change = next(p for p in self.params if p["name"] == "change")
        part_names = [part["name"] for part in change["part"]]
        assert "field" in part_names
        assert "oldValue" in part_names
        assert "newValue" in part_names

    def test_change_field_value(self):
        change = next(p for p in self.params if p["name"] == "change")
        field_part = next(p for p in change["part"] if p["name"] == "field")
        assert field_part["valueString"] == "display"

    def test_multiple_changed_fields(self):
        _, resource = _concept_params_modified(
            SYSTEM_URL, VERSION, "X",
            {"display": "Old", "definition": "Old def"},
            {"display": "New", "definition": "New def"},
            {"display": ("Old", "New"), "definition": ("Old def", "New def")},
        )
        change_params = [p for p in resource["parameter"] if p["name"] == "change"]
        assert len(change_params) == 2


# ── MessageHeader ─────────────────────────────────────────────────────────

class TestBuildMessageHeader:
    def setup_method(self):
        self.focus_refs = ["urn:uuid:aaa", "urn:uuid:bbb"]
        self.timestamp = "2026-01-01T00:00:00+00:00"
        _, self.header = _build_message_header(
            self.focus_refs, SYSTEM_URL, self.timestamp
        )

    def test_resource_type(self):
        assert self.header["resourceType"] == "MessageHeader"

    def test_event_coding_present(self):
        assert "eventCoding" in self.header
        assert "system" in self.header["eventCoding"]
        assert "code" in self.header["eventCoding"]

    def test_source_present(self):
        assert "source" in self.header
        assert "name" in self.header["source"]
        assert "endpoint" in self.header["source"]

    def test_focus_references(self):
        refs = [f["reference"] for f in self.header["focus"]]
        assert refs == self.focus_refs

    def test_definition_is_system_url(self):
        assert self.header["definition"] == SYSTEM_URL

    def test_meta_last_updated(self):
        assert self.header["meta"]["lastUpdated"] == self.timestamp


# ── build_change_bundle ───────────────────────────────────────────────────

class TestBuildChangeBundle:
    def test_returns_none_when_no_changes(self):
        assert build_change_bundle(SYSTEM_URL, VERSION, [], [], []) is None

    def test_bundle_resource_type(self):
        bundle = build_change_bundle(SYSTEM_URL, VERSION, [ADDED_CONCEPT], [], [])
        assert bundle["resourceType"] == "Bundle"

    def test_bundle_type_is_message(self):
        bundle = build_change_bundle(SYSTEM_URL, VERSION, [ADDED_CONCEPT], [], [])
        assert bundle["type"] == "message"

    def test_first_entry_is_message_header(self):
        bundle = build_change_bundle(SYSTEM_URL, VERSION, [ADDED_CONCEPT], [], [])
        assert bundle["entry"][0]["resource"]["resourceType"] == "MessageHeader"

    def test_added_concept_entry_count(self):
        bundle = build_change_bundle(SYSTEM_URL, VERSION, [ADDED_CONCEPT], [], [])
        # 1 MessageHeader + 1 Parameters
        assert len(bundle["entry"]) == 2

    def test_removed_concept_entry_count(self):
        bundle = build_change_bundle(SYSTEM_URL, VERSION, [], [], [REMOVED_CONCEPT])
        assert len(bundle["entry"]) == 2

    def test_modified_concept_entry_count(self):
        bundle = build_change_bundle(SYSTEM_URL, VERSION, [], [MODIFIED_CHANGE], [])
        assert len(bundle["entry"]) == 2

    def test_all_three_change_types(self):
        bundle = build_change_bundle(
            SYSTEM_URL, VERSION,
            [ADDED_CONCEPT],
            [MODIFIED_CHANGE],
            [REMOVED_CONCEPT],
        )
        # 1 MessageHeader + 3 Parameters
        assert len(bundle["entry"]) == 4

    def test_message_header_focus_count(self):
        bundle = build_change_bundle(
            SYSTEM_URL, VERSION,
            [ADDED_CONCEPT],
            [MODIFIED_CHANGE],
            [REMOVED_CONCEPT],
        )
        header = bundle["entry"][0]["resource"]
        assert len(header["focus"]) == 3

    def test_all_focus_refs_resolve_to_entries(self):
        bundle = build_change_bundle(
            SYSTEM_URL, VERSION,
            [ADDED_CONCEPT],
            [MODIFIED_CHANGE],
            [REMOVED_CONCEPT],
        )
        header = bundle["entry"][0]["resource"]
        focus_refs = {f["reference"] for f in header["focus"]}
        entry_urls = {e["fullUrl"] for e in bundle["entry"][1:]}
        assert focus_refs == entry_urls

    def test_bundle_has_timestamp(self):
        bundle = build_change_bundle(SYSTEM_URL, VERSION, [ADDED_CONCEPT], [], [])
        assert "timestamp" in bundle

    def test_bundle_has_id(self):
        bundle = build_change_bundle(SYSTEM_URL, VERSION, [ADDED_CONCEPT], [], [])
        assert "id" in bundle

    def test_multiple_added_concepts(self):
        concepts = [
            {**ADDED_CONCEPT, "code": "X1"},
            {**ADDED_CONCEPT, "code": "X2"},
            {**ADDED_CONCEPT, "code": "X3"},
        ]
        bundle = build_change_bundle(SYSTEM_URL, VERSION, concepts, [], [])
        params = _get_params(bundle)
        assert len(params) == 3

    def test_version_none_accepted(self):
        bundle = build_change_bundle(SYSTEM_URL, None, [ADDED_CONCEPT], [], [])
        assert bundle is not None
