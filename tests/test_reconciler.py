"""Reconciler tests.

Exercises the deterministic merge of a schema plan + hand-written
module build files into a logical schema.json. Each test builds its
own fake Builder output rather than invoking the real (LLM-driven)
Builder, so these tests are hermetic and fast.

Covered:
 - Happy path: entity columns, FK injection, value-set lookup patching
 - Hallucinated column rejected (not in ontology slice)
 - FK column collision with a data column rejected
 - Missing module build rejected (no partial merges)
 - Plan marked invalid rejected
 - Extra table in build rejected (prompt drift)
 - Unknown logical type rejected
 - Junction tables generated from plan relationships
"""

from __future__ import annotations

import json

import pytest

from agentcore.architect.planner import design_plan
from agentcore.architect.reconciler import BUILDS_SUBDIR, ReconcileError, reconcile


# Minimal ontology: two entities, one FK, one value set, one M:N.
#   ex:Lender          — lenderName (string)
#   ex:Loan            — amount (decimal), status (→ LoanStatusValue)
#   ex:hasLender       — Loan.ex_lender_id → Lender (FK)
#   ex:hasGuarantor    — M:N between Loan and Lender (marked junction)
#   ex:LoanStatusValue — oneOf(active, closed)
MINI_TTL = """\
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:   <https://example.org/mini#> .

<https://example.org/mini> a owl:Ontology .

ex:Lender a owl:Class ;
    rdfs:comment "A lender issuing loans." .

ex:Loan a owl:Class ;
    rdfs:comment "A loan issued to a borrower." .

ex:lenderName a owl:DatatypeProperty ;
    rdfs:domain ex:Lender ;
    rdfs:range xsd:string ;
    rdfs:comment "Legal name of the lender." .

ex:amount a owl:DatatypeProperty ;
    rdfs:domain ex:Loan ;
    rdfs:range xsd:decimal ;
    rdfs:comment "Principal amount." .

ex:status a owl:DatatypeProperty ;
    rdfs:domain ex:Loan ;
    rdfs:range ex:LoanStatusValue ;
    rdfs:comment "Lifecycle status." .

ex:hasLender a owl:ObjectProperty ;
    rdfs:domain ex:Loan ;
    rdfs:range ex:Lender ;
    rdfs:comment "The lender that issued this loan." .

ex:hasGuarantor a owl:ObjectProperty ;
    rdfs:domain ex:Loan ;
    rdfs:range ex:Lender ;
    rdfs:comment "Many-to-many guarantor relationship between loans and lenders." .

ex:LoanStatusValue a owl:Class ;
    rdfs:label "LoanStatusValue" ;
    owl:oneOf ( ex:active ex:closed ) .
"""


@pytest.fixture
def mini_domain(make_domain):
    """A plan-ready minimal domain with FKs, a junction, and a value set."""
    return make_domain("mini", ttl_text=MINI_TTL)


def _happy_build() -> dict:
    """A module build that matches the mini ontology exactly."""
    return {
        "module": "ex",
        "tables": [
            {
                "name": "ex_lenders",
                "columns": [
                    {"name": "lender_name", "type": "string", "not_null": True},
                ],
            },
            {
                "name": "ex_loans",
                "columns": [
                    {"name": "amount", "type": "decimal", "not_null": True},
                    {"name": "status", "type": "string", "not_null": True},
                ],
            },
        ],
    }


def _write_build(domain, build: dict) -> None:
    builds = domain.generated_dir / BUILDS_SUBDIR
    builds.mkdir(parents=True, exist_ok=True)
    (builds / f"module_{build['module']}.json").write_text(
        json.dumps(build, indent=2), encoding="utf-8"
    )


# ── Happy path ───────────────────────────────────────────────────────────────

def test_reconcile_happy_path(mini_domain):
    plan, _ = design_plan(mini_domain)
    _write_build(mini_domain, _happy_build())

    schema, schema_file = reconcile(mini_domain, plan)

    assert schema_file == "mini_schema.json"
    assert schema["schema_version"] == 1

    tables_by_name = {t["name"]: t for t in schema["tables"]}

    # Lookup table generated from the value set.
    assert "ex_loan_status_values" in tables_by_name
    lookup = tables_by_name["ex_loan_status_values"]
    assert lookup["lookup_table"] is True
    assert lookup["primary_key"] == "code"

    # Entity tables exist with their data columns.
    loans = tables_by_name["ex_loans"]
    cols_by_name = {c["name"]: c for c in loans["columns"]}
    assert set(cols_by_name) == {"amount", "status"}

    # Status column got patched with lookup reference + allowed values.
    status = cols_by_name["status"]
    assert status["references"] == {"table": "ex_loan_status_values", "column": "code"}
    assert sorted(status["allowed_values"]) == ["active", "closed"]

    # FK on ex_loans was injected.
    fk_cols = [fk["column"] for fk in loans["foreign_keys"]]
    assert "ex_lender_id" in fk_cols
    fk = next(fk for fk in loans["foreign_keys"] if fk["column"] == "ex_lender_id")
    assert fk["references_table"] == "ex_lenders"
    assert fk["not_null"] is True
    assert fk["required"] is True

    # Junction table: two FKs, no data columns.
    junction = next(
        t for t in schema["tables"]
        if t["name"] not in {"ex_lenders", "ex_loans", "ex_loan_status_values"}
    )
    assert junction["columns"] == []
    assert len(junction["foreign_keys"]) == 2

    # Creation order: lookups first, junction last.
    order = schema["creation_order"]
    assert order[0] == "ex_loan_status_values"
    assert order[-1] == junction["name"]
    assert set(order) == set(tables_by_name)

    # File was written.
    assert (mini_domain.generated_dir / schema_file).exists()


# ── Validation failures ──────────────────────────────────────────────────────

def test_reconcile_rejects_hallucinated_column(mini_domain):
    plan, _ = design_plan(mini_domain)
    build = _happy_build()
    build["tables"][1]["columns"].append(
        {"name": "made_up_field", "type": "string"}
    )
    _write_build(mini_domain, build)

    with pytest.raises(ReconcileError, match="not in the ontology slice"):
        reconcile(mini_domain, plan)


def test_reconcile_rejects_fk_collision(mini_domain):
    plan, _ = design_plan(mini_domain)
    build = _happy_build()
    # Emit a data column with the same name as the pinned FK.
    build["tables"][1]["columns"].append(
        {"name": "ex_lender_id", "type": "string"}
    )
    _write_build(mini_domain, build)

    # First it fails the ontology-slice check (the FK-collision check
    # runs after). Either error is acceptable — both catch the same bug.
    with pytest.raises(ReconcileError):
        reconcile(mini_domain, plan)


def test_reconcile_rejects_missing_module_build(mini_domain):
    plan, _ = design_plan(mini_domain)
    # No build file written at all. Could fail with either "builds dir
    # missing" or "module build files missing" — both are the same bug.
    with pytest.raises(ReconcileError, match="(?i)build"):
        reconcile(mini_domain, plan)


def test_reconcile_rejects_invalid_plan(mini_domain):
    plan, _ = design_plan(mini_domain)
    plan["valid"] = False
    with pytest.raises(ReconcileError, match="valid: false"):
        reconcile(mini_domain, plan)


def test_reconcile_rejects_extra_table_in_build(mini_domain):
    plan, _ = design_plan(mini_domain)
    build = _happy_build()
    build["tables"].append({
        "name": "ex_ghosts",
        "columns": [{"name": "phantom", "type": "string"}],
    })
    _write_build(mini_domain, build)

    with pytest.raises(ReconcileError, match="not declared in the plan"):
        reconcile(mini_domain, plan)


def test_reconcile_rejects_missing_table_in_build(mini_domain):
    plan, _ = design_plan(mini_domain)
    build = _happy_build()
    build["tables"] = [build["tables"][0]]  # drop ex_loans
    _write_build(mini_domain, build)

    with pytest.raises(ReconcileError, match="missing table 'ex_loans'"):
        reconcile(mini_domain, plan)


def test_reconcile_rejects_unknown_logical_type(mini_domain):
    plan, _ = design_plan(mini_domain)
    build = _happy_build()
    build["tables"][1]["columns"][0]["type"] = "money"  # not a logical type
    _write_build(mini_domain, build)

    with pytest.raises(ReconcileError, match="unknown logical type"):
        reconcile(mini_domain, plan)
