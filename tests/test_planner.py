"""Golden-file and behavior tests for the deterministic schema planner.

The student_loans snapshot is the load-bearing regression test: any change
to planner output that isn't reflected in the committed golden YAML fails
the suite. Regenerate it deliberately via `--update-golden`.

Smaller fixture tests cover specific features in isolation: cycles, union
domains, multi-namespace prefixes, junction naming.
"""

from __future__ import annotations

import shutil
import warnings
from pathlib import Path

import pytest
import yaml

from agentcore.domain import load_domain
from agentcore.architect.planner import (
    PlanValidationError,
    _junction_name,
    design_plan,
)

REPO_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = Path(__file__).parent / "fixtures"
GOLDEN_DIR = Path(__file__).parent / "golden"
STUDENT_LOANS_DIR = REPO_ROOT / "domains" / "student_loans"


# ── Golden-file snapshot ─────────────────────────────────────────────────────

def test_student_loans_matches_golden(tmp_path, request):
    """End-to-end: run the planner against student_loans and diff the
    YAML output against a committed golden file. Any unintended change
    in plan shape will surface here first.

    Pass `--update-golden` to pytest to rewrite the golden file from the
    current planner output (use sparingly — review the diff before committing).
    """
    staging = tmp_path / "student_loans"
    shutil.copytree(STUDENT_LOANS_DIR, staging, ignore=shutil.ignore_patterns("_generated"))
    domain = load_domain("student_loans", tmp_path)

    design_plan(domain)

    actual = (staging / "_generated" / "student_loans_schema_plan.yaml").read_text(encoding="utf-8")
    golden_path = GOLDEN_DIR / "student_loans_schema_plan.yaml"

    if request.config.getoption("--update-golden", default=False):
        golden_path.parent.mkdir(exist_ok=True)
        golden_path.write_text(actual, encoding="utf-8")
        pytest.skip(f"Updated golden file: {golden_path}")

    assert golden_path.exists(), (
        f"Golden file missing: {golden_path}\n"
        f"Run: pytest tests/test_planner.py --update-golden"
    )
    assert actual == golden_path.read_text(encoding="utf-8"), (
        "Planner output drifted from golden. If the change is intended, "
        "re-run with --update-golden and review the diff."
    )


# ── Cycle detection ──────────────────────────────────────────────────────────

def test_cycle_fixture_fails_validation(make_domain):
    """A 2-class loop (A↔B) must raise PlanValidationError with a
    descriptive cycle path, and the written plan must be marked invalid."""
    domain = make_domain("cycle", ttl_path=FIXTURES_DIR / "cycle.ttl")

    with pytest.raises(PlanValidationError) as exc:
        design_plan(domain)

    msg = str(exc.value)
    assert "ONTOLOGY ERROR" in msg
    assert "FK cycle" in msg
    assert "ex_as" in msg and "ex_bs" in msg

    # Plan file must exist and be flagged valid: false so downstream
    # tools don't consume it.
    plan_path = domain.generated_dir / "cycle_schema_plan.yaml"
    assert plan_path.exists()
    plan = yaml.safe_load(plan_path.read_text(encoding="utf-8"))
    assert plan["valid"] is False
    assert plan["errors"]
    assert "cycles" in plan


# ── Union domain/range ───────────────────────────────────────────────────────

def test_union_domain_warns_and_drops(make_domain):
    """Object property with owl:unionOf domain must emit a warning and
    be dropped from the plan (rather than silently vanishing)."""
    domain = make_domain("union", ttl_path=FIXTURES_DIR / "union.ttl")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        plan, _ = design_plan(domain)

    union_warnings = [w for w in caught if "owl:unionOf" in str(w.message)]
    assert union_warnings, "expected a warning about the dropped union property"

    # Three entity tables, zero relationships (the only property was dropped).
    assert len(plan["tables"]) == 3
    assert plan["relationships"] == []


# ── Multi-namespace ──────────────────────────────────────────────────────────

def test_multi_namespace_grouping(make_domain):
    """Two prefixes (crm + pol) must produce two modules; cross-namespace
    FK names use the target namespace prefix."""
    domain = make_domain("multi_ns", ttl_path=FIXTURES_DIR / "multi_ns.ttl")

    plan, _ = design_plan(domain)

    module_names = {m["name"] for m in plan["modules"]}
    assert module_names == {"crm", "pol"}

    fks = [r for r in plan["relationships"] if r["kind"] == "fk"]
    assert len(fks) == 1
    (fk,) = fks
    assert fk["child_table"] == "pol_policies"
    assert fk["parent_table"] == "crm_customers"
    assert fk["fk_column"] == "crm_customer_id"


# ── Override typo detection ──────────────────────────────────────────────────

def test_override_typo_fails_fast(make_domain):
    """Unknown top-level keys in schema_overrides.yaml must raise
    PlanValidationError with the allowed-keys list in the message."""
    domain = make_domain(
        "multi_ns",
        ttl_path=FIXTURES_DIR / "multi_ns.ttl",
        overrides={"fk_parents": {"typo": "range"}},  # plural typo
    )

    with pytest.raises(PlanValidationError) as exc:
        design_plan(domain)

    assert "unknown top-level keys" in str(exc.value)
    assert "fk_parents" in str(exc.value)


# ── Junction naming ──────────────────────────────────────────────────────────

def test_junction_name_collision_is_disambiguated():
    """Two M:N properties between the same class pair must produce
    distinct junction table names — the second one gets suffixed with
    the property's local name."""
    namespaces = {"ex": "https://example.org/ex#"}
    prop_a = {
        "iri": "https://example.org/ex#primaryLink",
        "local_name": "primaryLink",
    }
    prop_b = {
        "iri": "https://example.org/ex#secondaryLink",
        "local_name": "secondaryLink",
    }
    domain_iri = "https://example.org/ex#Foo"
    range_iri = "https://example.org/ex#Bar"

    used: set[str] = set()
    first = _junction_name(prop_a, domain_iri, range_iri, namespaces, used)
    used.add(first)
    second = _junction_name(prop_b, domain_iri, range_iri, namespaces, used)

    assert first == "ex_foo_bars"
    assert second == "ex_foo_bars_secondary_link"
    assert first != second


# ── Composition override (fk_parent: domain) ─────────────────────────────────

def test_fk_parent_override_flips_direction(make_domain):
    """`fk_parent: domain` must place the FK on the range side. This is
    the `hasPayment` case — Payment is a child of Loan."""
    ttl = """\
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:   <https://example.org/comp#> .

<https://example.org/comp> a owl:Ontology .

ex:Loan a owl:Class ; rdfs:comment "parent" .
ex:Payment a owl:Class ; rdfs:comment "dependent child" .

ex:hasPayment a owl:ObjectProperty ;
    rdfs:domain ex:Loan ;
    rdfs:range  ex:Payment .
"""
    domain = make_domain(
        "comp",
        ttl_text=ttl,
        overrides={"fk_parent": {"hasPayment": "domain"}},
    )

    plan, _ = design_plan(domain)

    fks = [r for r in plan["relationships"] if r["kind"] == "fk"]
    assert len(fks) == 1
    (fk,) = fks
    assert fk["child_table"] == "ex_payments"
    assert fk["parent_table"] == "ex_loans"
    assert fk["fk_column"] == "ex_loan_id"
