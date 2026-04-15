"""Regression tests for the SIF translator's JOIN building.

The main hazard is many-to-many relations that go through a junction table:
the same stored RelationMap must produce a valid JOIN chain regardless of
which end the query starts from.
"""

from __future__ import annotations

import pytest

from agentcore.sif import SchemaMap, TranslationError, build_sif_tool, validate_operations
from agentcore.sif_sql import translate


# ── Minimal inline fixture: Loan <-> Cosigner via sl_loan_cosigners ───────────

_ONTOLOGY = {
    "classes": [
        {"iri": "x#Loan",     "local_name": "Loan",     "comment": "loans"},
        {"iri": "x#Cosigner", "local_name": "Cosigner", "comment": "cosigners"},
    ],
    "object_properties": [
        {
            "iri": "x#hasCosigner",
            "local_name": "hasCosigner",
            "domain_iri": "x#Loan",
            "range_iri":  "x#Cosigner",
        },
    ],
}

_SCHEMA = {
    "tables": [
        {
            "name": "sl_loans",
            "primary_key": "sl_loan_id",
            "comment": "loans",
            "ontology_iri": "x#Loan",
            "columns": [{"name": "loan_number"}],
            "foreign_keys": [],
        },
        {
            "name": "sl_cosigners",
            "primary_key": "sl_cosigner_id",
            "comment": "cosigners",
            "ontology_iri": "x#Cosigner",
            "columns": [{"name": "first_name"}, {"name": "last_name"}],
            "foreign_keys": [],
        },
        {
            "name": "sl_loan_cosigners",
            "primary_key": "sl_loan_cosigner_id",
            "columns": [],  # junction: no data columns, only FKs
            "foreign_keys": [
                {
                    "column": "sl_loan_id",
                    "references_table": "sl_loans",
                    "references_column": "sl_loan_id",
                },
                {
                    "column": "sl_cosigner_id",
                    "references_table": "sl_cosigners",
                    "references_column": "sl_cosigner_id",
                },
            ],
        },
    ],
}


@pytest.fixture
def smap() -> SchemaMap:
    return SchemaMap(_ONTOLOGY, _SCHEMA)


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


# ── Junction detection ───────────────────────────────────────────────────────

def test_junction_table_detected(smap):
    assert "sl_loan_cosigners" in smap.junction_tables


def test_relation_is_two_step(smap):
    rel = smap.relations["hasCosigner"]
    assert len(rel.steps) == 2
    assert rel.is_direct is False
    assert rel.junction_table == "sl_loan_cosigners"


# ── Regression: M2M query from BOTH directions ───────────────────────────────

def test_query_cosigner_by_loan_number(smap):
    """Start from sl_cosigners, reach sl_loans via the junction."""
    op = {
        "op": "query",
        "entity": "Cosigner",
        "relations": [
            {"rel": "hasCosigner", "entity": "Loan",
             "filters": {"loan_number": "SLN-2026-00001"}},
        ],
    }
    stmt = translate(op, smap)
    sql = _normalize(stmt.sql)

    # The first JOIN must anchor on sl_cosigners (the FROM table). If the
    # translator walked the steps in storage order naively, the first JOIN
    # would be "JOIN sl_loans ON sl_loan_cosigners..." which references an
    # unjoined table — that's the bug we're guarding against.
    assert "FROM sl_cosigners JOIN sl_loan_cosigners ON" in sql
    assert "JOIN sl_loans ON sl_loan_cosigners.sl_loan_id = sl_loans.sl_loan_id" in sql
    assert "WHERE sl_loans.loan_number = :p1" in sql
    assert stmt.params == {"p1": "SLN-2026-00001"}


def test_query_loan_by_cosigner_name(smap):
    """Start from sl_loans, reach sl_cosigners via the junction."""
    op = {
        "op": "query",
        "entity": "Loan",
        "relations": [
            {"rel": "hasCosigner", "entity": "Cosigner",
             "filters": {"last_name": "Gallasova"}},
        ],
    }
    stmt = translate(op, smap)
    sql = _normalize(stmt.sql)

    assert "FROM sl_loans JOIN sl_loan_cosigners ON" in sql
    assert "JOIN sl_cosigners ON sl_loan_cosigners.sl_cosigner_id = sl_cosigners.sl_cosigner_id" in sql
    assert "WHERE sl_cosigners.last_name = :p1" in sql
    assert stmt.params == {"p1": "Gallasova"}


# ── Guard: broken paths surface as TranslationError, not silent bad SQL ──────

def test_unknown_relation_raises(smap):
    op = {
        "op": "query",
        "entity": "Cosigner",
        "relations": [{"rel": "nonexistent", "entity": "Loan"}],
    }
    with pytest.raises(TranslationError, match="Unknown relation"):
        translate(op, smap)


# ── Link / unlink validation ─────────────────────────────────────────────────

def _link_op(**overrides):
    base = {
        "op": "link",
        "relation": "hasCosigner",
        "from": {"entity": "Loan",     "filters": {"loan_number": "SLN-2026-00001"}},
        "to":   {"entity": "Cosigner", "filters": {"last_name": "Gallasova"}},
    }
    base.update(overrides)
    return base


def test_link_valid_passes_validation(smap):
    errors = validate_operations([_link_op()], smap)
    assert errors == []


def test_unlink_valid_passes_validation(smap):
    errors = validate_operations([_link_op(op="unlink")], smap)
    assert errors == []


def test_link_endpoints_swapped_passes_validation(smap):
    """Endpoint order should not matter — {Loan, Cosigner} either way."""
    op = _link_op(
        **{
            "from": {"entity": "Cosigner", "filters": {"last_name": "Gallasova"}},
            "to":   {"entity": "Loan",     "filters": {"loan_number": "SLN-2026-00001"}},
        }
    )
    assert validate_operations([op], smap) == []


def test_link_unknown_relation_flagged(smap):
    errors = validate_operations([_link_op(relation="bogus")], smap)
    assert any("unknown relation" in e.lower() for e in errors)


def test_link_wrong_endpoint_class_flagged(smap):
    op = _link_op(
        **{
            "to": {"entity": "Loan", "filters": {"loan_number": "X"}},
        }
    )
    # Two Loans, no Cosigner → endpoint mismatch
    errors = validate_operations([op], smap)
    assert any("endpoints must be" in e for e in errors)


def test_link_missing_filters_flagged(smap):
    op = _link_op(
        **{
            "from": {"entity": "Loan", "filters": {}},
        }
    )
    errors = validate_operations([op], smap)
    assert any("requires 'filters'" in e for e in errors)


def test_link_unknown_filter_field_flagged(smap):
    op = _link_op(
        **{
            "to": {"entity": "Cosigner", "filters": {"does_not_exist": "x"}},
        }
    )
    errors = validate_operations([op], smap)
    assert any("unknown filter field 'does_not_exist'" in e for e in errors)


# ── Tool schema wiring ───────────────────────────────────────────────────────

def test_build_sif_tool_injects_link_enums(smap):
    tool = build_sif_tool(smap)
    op_props = tool["input_schema"]["properties"]["operations"]["items"]["properties"]

    assert "link" in op_props["op"]["enum"]
    assert "unlink" in op_props["op"]["enum"]
    assert op_props["relation"]["enum"] == ["hasCosigner"]
    assert op_props["from"]["properties"]["entity"]["enum"] == ["Cosigner", "Loan"]
    assert op_props["to"]["properties"]["entity"]["enum"] == ["Cosigner", "Loan"]
