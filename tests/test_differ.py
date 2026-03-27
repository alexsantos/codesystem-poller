"""Tests for the differ module: flattening and diffing logic."""

import pytest

from src.differ import diff_concepts, flatten_concepts


# ─── Fixtures ────────────────────────────────────────────────────────────

SAMPLE_CONCEPTS = [
    {
        "code": "GLUC",
        "display": "Glucose, Serum",
        "definition": "Fasting glucose measurement",
        "property": [
            {"code": "category", "valueString": "chemistry"},
        ],
    },
    {
        "code": "CBC",
        "display": "Complete Blood Count",
        "definition": "Full haematology panel",
        "concept": [
            {
                "code": "WBC",
                "display": "White Blood Cell Count",
                "definition": "Leucocyte count",
            },
            {
                "code": "RBC",
                "display": "Red Blood Cell Count",
                "definition": "Erythrocyte count",
            },
        ],
    },
    {
        "code": "HBA1C",
        "display": "Hemoglobin A1c",
        "definition": "HbA1c test",
    },
]


# ─── Flattening ──────────────────────────────────────────────────────────

class TestFlattenConcepts:
    def test_flat_codes_extracted(self):
        result = flatten_concepts(SAMPLE_CONCEPTS)
        assert set(result.keys()) == {"GLUC", "CBC", "WBC", "RBC", "HBA1C"}

    def test_child_parent_relationship(self):
        result = flatten_concepts(SAMPLE_CONCEPTS)
        assert result["WBC"]["parent_code"] == "CBC"
        assert result["RBC"]["parent_code"] == "CBC"
        assert result["CBC"]["parent_code"] is None
        assert result["GLUC"]["parent_code"] is None

    def test_properties_extracted(self):
        result = flatten_concepts(SAMPLE_CONCEPTS)
        assert result["GLUC"]["properties"]["category"] == "chemistry"

    def test_hash_deterministic(self):
        r1 = flatten_concepts(SAMPLE_CONCEPTS)
        r2 = flatten_concepts(SAMPLE_CONCEPTS)
        for code in r1:
            assert r1[code]["concept_hash"] == r2[code]["concept_hash"]

    def test_empty_list(self):
        assert flatten_concepts([]) == {}

    def test_concept_without_code_skipped(self):
        concepts = [{"display": "No code here"}]
        assert flatten_concepts(concepts) == {}

    def test_designations_captured(self):
        concepts = [
            {
                "code": "TEST",
                "display": "Test",
                "designation": [
                    {"language": "pt", "value": "Teste"},
                    {"language": "fr", "value": "Essai"},
                ],
            }
        ]
        result = flatten_concepts(concepts)
        desigs = result["TEST"]["properties"]["_designations"]
        assert len(desigs) == 2
        assert desigs[0]["language"] == "pt"


# ─── Diffing ─────────────────────────────────────────────────────────────

class TestDiffConcepts:
    def test_no_changes(self):
        fresh = flatten_concepts(SAMPLE_CONCEPTS)
        stored = flatten_concepts(SAMPLE_CONCEPTS)
        added, modified, removed = diff_concepts(fresh, stored)
        assert added == []
        assert modified == []
        assert removed == []

    def test_added_concept(self):
        stored = flatten_concepts(SAMPLE_CONCEPTS)
        new_concepts = SAMPLE_CONCEPTS + [
            {"code": "TSH", "display": "Thyroid Stimulating Hormone"}
        ]
        fresh = flatten_concepts(new_concepts)
        added, modified, removed = diff_concepts(fresh, stored)
        assert len(added) == 1
        assert added[0]["code"] == "TSH"
        assert modified == []
        assert removed == []

    def test_removed_concept(self):
        fresh = flatten_concepts(SAMPLE_CONCEPTS[:1])  # only GLUC
        stored = flatten_concepts(SAMPLE_CONCEPTS)
        added, modified, removed = diff_concepts(fresh, stored)
        assert added == []
        assert modified == []
        removed_codes = {c["code"] for c in removed}
        assert "CBC" in removed_codes
        assert "HBA1C" in removed_codes

    def test_modified_display(self):
        stored = flatten_concepts(SAMPLE_CONCEPTS)

        modified_concepts = [
            {
                "code": "GLUC",
                "display": "Glucose, Serum (Updated)",  # changed
                "definition": "Fasting glucose measurement",
                "property": [
                    {"code": "category", "valueString": "chemistry"},
                ],
            },
            SAMPLE_CONCEPTS[1],
            SAMPLE_CONCEPTS[2],
        ]
        fresh = flatten_concepts(modified_concepts)

        added, modified, removed = diff_concepts(fresh, stored)
        assert added == []
        assert removed == []
        assert len(modified) == 1
        assert modified[0]["code"] == "GLUC"
        assert "display" in modified[0]["changed_fields"]
        old_display, new_display = modified[0]["changed_fields"]["display"]
        assert old_display == "Glucose, Serum"
        assert new_display == "Glucose, Serum (Updated)"

    def test_all_three_change_types(self):
        stored_concepts = [
            {"code": "A", "display": "Alpha"},
            {"code": "B", "display": "Beta"},
            {"code": "C", "display": "Charlie"},
        ]
        fresh_concepts = [
            {"code": "A", "display": "Alpha Modified"},  # modified
            {"code": "C", "display": "Charlie"},           # unchanged
            {"code": "D", "display": "Delta"},             # added
        ]
        stored = flatten_concepts(stored_concepts)
        fresh = flatten_concepts(fresh_concepts)

        added, modified, removed = diff_concepts(fresh, stored)
        assert len(added) == 1 and added[0]["code"] == "D"
        assert len(modified) == 1 and modified[0]["code"] == "A"
        assert len(removed) == 1 and removed[0]["code"] == "B"
