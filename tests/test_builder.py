"""Builder tests.

Exercises the per-module slice assembly and the async fan-out, with
the Anthropic client fully stubbed so nothing touches the network.

Covered:
 - Slice assembly produces the expected tables/datatype/value-set shape
 - Happy-path end-to-end: all modules succeed, build files land on disk
 - LLM returning non-JSON surfaces as ok=False
 - LLM response missing `tables` key surfaces as ok=False
 - Builder output is consumable by the reconciler (round-trip)
"""

from __future__ import annotations

import asyncio
import json

import pytest

from agentcore.architect.builder import (
    GRANULARITY_TABLE,
    ModuleBuildResult,
    _build_input,
    _build_module_input,
    build_modules,
)
from agentcore.architect.planner import design_plan
from agentcore.architect.reconciler import BUILDS_SUBDIR, reconcile


# Same minimal TTL the reconciler tests use — one module, two entities,
# one FK, one M:N, one value set.
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
    return make_domain("mini", ttl_text=MINI_TTL)


# ── Fake AsyncAnthropic client ───────────────────────────────────────────────

class _FakeTextBlock:
    def __init__(self, text: str) -> None:
        self.type = "text"
        self.text = text


class _FakeUsage:
    def __init__(self) -> None:
        self.input_tokens = 100
        self.output_tokens = 50


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.content = [_FakeTextBlock(text)]
        self.usage = _FakeUsage()


class _FakeMessages:
    def __init__(self, responder) -> None:
        self._responder = responder  # callable(first_user_text) -> str

    async def create(self, *, model, max_tokens, system, messages):
        # Route on the FIRST user turn (the original payload). Later
        # turns are validation feedback messages and don't carry the
        # module/table markers we key on.
        first_user = next(m["content"] for m in messages if m["role"] == "user")
        return _FakeResponse(self._responder(first_user))


class _FakeAsyncClient:
    """Stub that mimics the slice of AsyncAnthropic the builder uses."""
    def __init__(self, responder) -> None:
        self.messages = _FakeMessages(responder)

    async def close(self) -> None:
        pass


def _responder_from_map(reply_by_module: dict[str, str]):
    """Return a responder that picks a reply based on 'module: <name>' in
    the YAML user message."""
    def respond(user_text: str) -> str:
        for name, reply in reply_by_module.items():
            if f"module: {name}\n" in user_text:
                return reply
        raise AssertionError(f"No canned reply for user message: {user_text[:120]!r}")
    return respond


# ── Slice assembly ───────────────────────────────────────────────────────────

def test_build_module_input_shape(mini_domain):
    plan, _ = design_plan(mini_domain)
    model = mini_domain.ontology_model
    (module,) = plan["modules"]

    payload = _build_module_input(module, plan, model)

    assert payload["module"] == "ex"

    # Two entity tables, sorted by name. Value set class is NOT a table.
    table_names = [t["name"] for t in payload["tables"]]
    assert table_names == ["ex_lenders", "ex_loans"]

    # ex_loans carries the pinned FK to ex_lenders.
    loans = next(t for t in payload["tables"] if t["name"] == "ex_loans")
    pinned_cols = [p["column"] for p in loans["pinned_fk_columns"]]
    assert pinned_cols == ["ex_lender_id"]

    # ex_lenders has no pinned FKs.
    lenders = next(t for t in payload["tables"] if t["name"] == "ex_lenders")
    assert lenders["pinned_fk_columns"] == []

    # Datatype slice: three properties, each tagged with `on_table`.
    slice_ = payload["ontology_slice"]
    by_snake = {d["snake_name"]: d for d in slice_["datatype_properties"]}
    assert set(by_snake) == {"lender_name", "amount", "status"}
    assert by_snake["lender_name"]["on_table"] == "ex_lenders"
    assert by_snake["amount"]["on_table"] == "ex_loans"
    assert by_snake["status"]["on_table"] == "ex_loans"
    assert by_snake["status"]["range"].startswith("value_set:")

    # One value set referenced.
    assert len(slice_["value_sets"]) == 1
    vs = slice_["value_sets"][0]
    assert vs["name"] == "LoanStatusValue"
    assert sorted(vs["members"]) == ["active", "closed"]


# ── End-to-end with stubbed client ───────────────────────────────────────────

_GOOD_REPLY_EX = json.dumps({
    "module": "ex",
    "tables": [
        {
            "name": "ex_lenders",
            "columns": [
                {"name": "lender_name", "type": "string", "not_null": True,
                 "required": True, "unique": True},
            ],
        },
        {
            "name": "ex_loans",
            "columns": [
                {"name": "amount", "type": "decimal", "not_null": True,
                 "required": True, "unique": False},
                {"name": "status", "type": "string", "not_null": True,
                 "required": True, "unique": False},
            ],
        },
    ],
})


def test_build_modules_happy_path(mini_domain):
    plan, _ = design_plan(mini_domain)
    fake = _FakeAsyncClient(_responder_from_map({"ex": _GOOD_REPLY_EX}))

    results = build_modules(
        mini_domain, plan, api_key="unused",
        client=fake, verbose=False,
    )

    assert len(results) == 1
    (result,) = results
    assert result.ok is True
    assert result.name == "ex"
    assert result.path is not None
    assert result.path.exists()

    written = json.loads(result.path.read_text(encoding="utf-8"))
    assert written["module"] == "ex"
    assert {t["name"] for t in written["tables"]} == {"ex_lenders", "ex_loans"}


def test_build_modules_output_reconciles_end_to_end(mini_domain):
    """The builder's output must be immediately consumable by the
    reconciler — this proves the two halves agree on column shapes."""
    plan, _ = design_plan(mini_domain)
    fake = _FakeAsyncClient(_responder_from_map({"ex": _GOOD_REPLY_EX}))

    build_modules(mini_domain, plan, api_key="unused", client=fake, verbose=False)
    schema, _ = reconcile(mini_domain, plan)

    tables_by_name = {t["name"]: t for t in schema["tables"]}
    # Lookup table exists and the status column was patched.
    assert "ex_loan_status_values" in tables_by_name
    loans = tables_by_name["ex_loans"]
    status = next(c for c in loans["columns"] if c["name"] == "status")
    assert status["references"]["table"] == "ex_loan_status_values"


def test_build_modules_bad_json_fails_module(mini_domain):
    plan, _ = design_plan(mini_domain)
    fake = _FakeAsyncClient(_responder_from_map({"ex": "I am not JSON, sorry."}))

    results = build_modules(
        mini_domain, plan, api_key="unused",
        client=fake, verbose=False,
    )

    (result,) = results
    assert result.ok is False
    assert "JSON" in (result.error or "")
    # No file should have been written for the failed module.
    builds_dir = mini_domain.generated_dir / BUILDS_SUBDIR
    assert not (builds_dir / "module_ex.json").exists()


def test_build_modules_missing_tables_key_fails_module(mini_domain):
    plan, _ = design_plan(mini_domain)
    fake = _FakeAsyncClient(_responder_from_map({"ex": json.dumps({"module": "ex"})}))

    results = build_modules(
        mini_domain, plan, api_key="unused",
        client=fake, verbose=False,
    )

    (result,) = results
    assert result.ok is False
    assert "tables" in (result.error or "")


# ── Per-table granularity ────────────────────────────────────────────────────

def test_build_input_single_table_slice(mini_domain):
    """Per-table slice must contain exactly one table and only the
    datatype properties that apply to it — no sibling-table noise."""
    plan, _ = design_plan(mini_domain)
    model = mini_domain.ontology_model

    payload = _build_input("ex", ["ex_loans"], plan, model)

    assert [t["name"] for t in payload["tables"]] == ["ex_loans"]
    # Pinned FK on ex_loans still present.
    assert payload["tables"][0]["pinned_fk_columns"][0]["column"] == "ex_lender_id"
    # Only Loan's datatype properties, not Lender's.
    snakes = {d["snake_name"] for d in payload["ontology_slice"]["datatype_properties"]}
    assert snakes == {"amount", "status"}
    # Value set carried through because `status` is on this table.
    assert len(payload["ontology_slice"]["value_sets"]) == 1


def test_build_modules_table_granularity(mini_domain):
    """Per-table mode: one LLM call per table, results re-assembled
    into a single module build file the reconciler can consume."""
    plan, _ = design_plan(mini_domain)

    # Canned per-table replies, keyed by the YAML substring we can spot
    # in the user message for routing.
    def respond(user_text: str) -> str:
        if "- name: ex_lenders" in user_text and "- name: ex_loans" not in user_text:
            return json.dumps({
                "module": "ex",
                "tables": [{
                    "name": "ex_lenders",
                    "columns": [
                        {"name": "lender_name", "type": "string",
                         "not_null": True, "required": True, "unique": True},
                    ],
                }],
            })
        if "- name: ex_loans" in user_text and "- name: ex_lenders" not in user_text:
            return json.dumps({
                "module": "ex",
                "tables": [{
                    "name": "ex_loans",
                    "columns": [
                        {"name": "amount", "type": "decimal",
                         "not_null": True, "required": True, "unique": False},
                        {"name": "status", "type": "string",
                         "not_null": True, "required": True, "unique": False},
                    ],
                }],
            })
        raise AssertionError(f"Unexpected single-table payload: {user_text[:200]!r}")

    fake = _FakeAsyncClient(respond)
    results = build_modules(
        mini_domain, plan, api_key="unused",
        client=fake, granularity=GRANULARITY_TABLE, verbose=False,
    )

    (result,) = results
    assert result.ok is True
    assert result.path is not None

    written = json.loads(result.path.read_text(encoding="utf-8"))
    assert [t["name"] for t in written["tables"]] == ["ex_lenders", "ex_loans"]

    # Reconciler must still accept it.
    schema, _ = reconcile(mini_domain, plan)
    tables_by_name = {t["name"]: t for t in schema["tables"]}
    assert "ex_loans" in tables_by_name
    assert "ex_loan_status_values" in tables_by_name


def test_build_modules_table_granularity_partial_failure(mini_domain):
    """If one table in a module fails, the whole module fails — the
    reconciler refuses partial merges, so we surface that up front."""
    plan, _ = design_plan(mini_domain)

    def respond(user_text: str) -> str:
        if "- name: ex_lenders" in user_text and "- name: ex_loans" not in user_text:
            return json.dumps({
                "module": "ex",
                "tables": [{"name": "ex_lenders", "columns": []}],
            })
        if "- name: ex_loans" in user_text and "- name: ex_lenders" not in user_text:
            return "garbage not json"
        raise AssertionError(f"Unexpected: {user_text[:100]!r}")

    fake = _FakeAsyncClient(respond)
    results = build_modules(
        mini_domain, plan, api_key="unused",
        client=fake, granularity=GRANULARITY_TABLE, verbose=False,
    )

    (result,) = results
    assert result.ok is False
    assert "ex_loans" in (result.error or "")


def test_build_modules_self_heals_after_correction(mini_domain):
    """First LLM response emits a hallucinated column; the validation
    loop re-prompts and the second response is clean. The module build
    must succeed — this is the whole point of the feedback loop."""
    plan, _ = design_plan(mini_domain)

    attempts = {"n": 0}

    def responder(first_user_text: str) -> str:
        attempts["n"] += 1
        if attempts["n"] == 1:
            # Hallucinated column on ex_loans.
            return json.dumps({
                "module": "ex",
                "tables": [
                    {
                        "name": "ex_lenders",
                        "columns": [
                            {"name": "lender_name", "type": "string",
                             "not_null": True, "required": True, "unique": True},
                        ],
                    },
                    {
                        "name": "ex_loans",
                        "columns": [
                            {"name": "amount", "type": "decimal",
                             "not_null": True, "required": True, "unique": False},
                            {"name": "status", "type": "string",
                             "not_null": True, "required": True, "unique": False},
                            {"name": "bogus_ghost_column", "type": "string",
                             "not_null": True, "required": True, "unique": False},
                        ],
                    },
                ],
            })
        # Second attempt: clean.
        return _GOOD_REPLY_EX

    fake = _FakeAsyncClient(responder)
    results = build_modules(
        mini_domain, plan, api_key="unused", client=fake, verbose=False,
    )

    (result,) = results
    assert result.ok is True, f"unexpected failure: {result.error}"
    assert attempts["n"] == 2

    written = json.loads(result.path.read_text(encoding="utf-8"))
    cols = {c["name"] for t in written["tables"] for c in t["columns"]}
    assert "bogus_ghost_column" not in cols
    assert {"lender_name", "amount", "status"} <= cols


def test_build_modules_resumes_from_cached_build(mini_domain):
    """Second run with unchanged plan skips the LLM entirely."""
    plan, _ = design_plan(mini_domain)
    calls = {"n": 0}

    def responder(user_text: str) -> str:
        calls["n"] += 1
        return _GOOD_REPLY_EX

    # First run populates the cache.
    build_modules(
        mini_domain, plan, api_key="unused",
        client=_FakeAsyncClient(responder), verbose=False,
    )
    assert calls["n"] == 1

    # Second run must hit the cache — no LLM calls.
    results = build_modules(
        mini_domain, plan, api_key="unused",
        client=_FakeAsyncClient(responder), verbose=False,
    )
    assert calls["n"] == 1
    (result,) = results
    assert result.ok is True
    assert result.skipped is True


def test_build_modules_force_bypasses_cache(mini_domain):
    """--force rebuilds even when a valid cached build exists."""
    plan, _ = design_plan(mini_domain)
    calls = {"n": 0}

    def responder(user_text: str) -> str:
        calls["n"] += 1
        return _GOOD_REPLY_EX

    build_modules(
        mini_domain, plan, api_key="unused",
        client=_FakeAsyncClient(responder), verbose=False,
    )
    build_modules(
        mini_domain, plan, api_key="unused",
        client=_FakeAsyncClient(responder), verbose=False,
        force=True,
    )
    assert calls["n"] == 2


def test_build_modules_plan_change_invalidates_cache(mini_domain):
    """If the plan slice changes between runs, cached module is rebuilt."""
    plan, _ = design_plan(mini_domain)
    calls = {"n": 0}

    def responder(user_text: str) -> str:
        calls["n"] += 1
        return _GOOD_REPLY_EX

    build_modules(
        mini_domain, plan, api_key="unused",
        client=_FakeAsyncClient(responder), verbose=False,
    )
    assert calls["n"] == 1

    # Mutate the plan so the hashed payload changes. Renaming a table
    # flows into `_build_module_input` and therefore the plan hash.
    plan["tables"][0]["comment"] = "mutated comment — cache should miss"

    results = build_modules(
        mini_domain, plan, api_key="unused",
        client=_FakeAsyncClient(responder), verbose=False,
    )
    assert calls["n"] == 2
    (result,) = results
    assert result.ok is True
    assert result.skipped is False


def test_build_modules_strips_markdown_fences(mini_domain):
    """The LLM sometimes wraps JSON in ```json ... ``` — must be handled."""
    plan, _ = design_plan(mini_domain)
    fenced = f"```json\n{_GOOD_REPLY_EX}\n```"
    fake = _FakeAsyncClient(_responder_from_map({"ex": fenced}))

    results = build_modules(
        mini_domain, plan, api_key="unused",
        client=fake, verbose=False,
    )

    (result,) = results
    assert result.ok is True, f"unexpected failure: {result.error}"
