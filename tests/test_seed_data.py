"""Seed data generator tests.

Exercises:
 - Deterministic lookup seeding (no LLM)
 - Deterministic junction seeding (no LLM)
 - Entity table seeding via stubbed async client
 - Validation feedback loop (wrong table name → corrective reprompt)
 - FK parent-PK-range plumbed into the entity input payload
 - End-to-end seed_schema output ordered by creation_order
"""

from __future__ import annotations

import json

import pytest

from agentcore.architect.seed_data import (
    _build_entity_input,
    _collect_lookup_codes,
    _seed_junction_table,
    _seed_lookup_table,
    _validate_entity_sql,
    seed_schema,
)
from agentcore.config import ArchitectConfig

_TEST_ARCH_CFG = ArchitectConfig(
    max_tokens=4096,
    max_concurrency=5,
    sdk_max_retries=5,
    max_validation_attempts=3,
    rows_per_table=10,
    junction_rows=5,
)


# Minimal reconciled schema — 2 entity tables, 1 junction, 1 lookup.
def _fake_schema() -> dict:
    return {
        "schema_version": 1,
        "tables": [
            {
                "name": "ex_loan_status_values",
                "lookup_table": True,
                "primary_key": "code",
                "columns": [
                    {"name": "label", "type": "string"},
                    {"name": "sort_order", "type": "integer"},
                    {"name": "active", "type": "boolean"},
                ],
                "foreign_keys": [],
            },
            {
                "name": "ex_lenders",
                "primary_key": "ex_lender_id",
                "columns": [
                    {"name": "lender_name", "type": "string",
                     "not_null": True, "unique": True},
                ],
                "foreign_keys": [],
            },
            {
                "name": "ex_loans",
                "primary_key": "ex_loan_id",
                "columns": [
                    {"name": "amount", "type": "decimal", "not_null": True},
                    {"name": "status", "type": "string", "not_null": True,
                     "references": {"table": "ex_loan_status_values", "column": "code"},
                     "allowed_values": ["active", "closed"]},
                ],
                "foreign_keys": [
                    {"column": "ex_lender_id",
                     "references_table": "ex_lenders",
                     "references_column": "ex_lender_id"},
                ],
            },
            {
                "name": "ex_loan_guarantors",
                "primary_key": "ex_loan_guarantors_id",
                "columns": [],
                "foreign_keys": [
                    {"column": "ex_loan_id",
                     "references_table": "ex_loans",
                     "references_column": "ex_loan_id"},
                    {"column": "ex_lender_id",
                     "references_table": "ex_lenders",
                     "references_column": "ex_lender_id"},
                ],
            },
        ],
        "creation_order": [
            "ex_loan_status_values",
            "ex_lenders",
            "ex_loans",
            "ex_loan_guarantors",
        ],
    }


# ── Deterministic seeders ────────────────────────────────────────────────────

def test_collect_lookup_codes():
    schema = _fake_schema()
    codes = _collect_lookup_codes(schema)
    assert codes == {"ex_loan_status_values": ["active", "closed"]}


def test_seed_lookup_table_emits_one_insert_per_code():
    table = {
        "name": "ex_loan_status_values",
        "lookup_table": True,
        "_seed_codes": ["active", "closed"],
    }
    sql = _seed_lookup_table(table)
    assert sql.count("INSERT INTO ex_loan_status_values") == 2
    assert "'active'" in sql and "'closed'" in sql
    # sort_order assigned sequentially
    assert ", 1, TRUE" in sql
    assert ", 2, TRUE" in sql


def test_seed_junction_table_pairs_within_range():
    junction = {
        "name": "ex_loan_guarantors",
        "foreign_keys": [
            {"column": "ex_loan_id", "references_table": "ex_loans",
             "references_column": "ex_loan_id"},
            {"column": "ex_lender_id", "references_table": "ex_lenders",
             "references_column": "ex_lender_id"},
        ],
    }
    sql = _seed_junction_table(junction, rows_per_table=10, pair_count=5)
    assert sql.count("INSERT INTO ex_loan_guarantors") == 5
    assert "(ex_loan_id, ex_lender_id)" in sql


# ── Entity payload assembly ──────────────────────────────────────────────────

def test_build_entity_input_carries_fk_ranges():
    schema = _fake_schema()
    loans = next(t for t in schema["tables"] if t["name"] == "ex_loans")
    parent_ranges = {"ex_lenders": (1, 10), "ex_loans": (1, 10)}

    payload = _build_entity_input(loans, parent_ranges, rows_per_table=10)

    assert payload["rows_per_table"] == 10
    assert payload["table"]["name"] == "ex_loans"
    # FK column carries the parent range.
    assert len(payload["table"]["fk_columns"]) == 1
    fk = payload["table"]["fk_columns"][0]
    assert fk["name"] == "ex_lender_id"
    assert fk["parent_pk_range"] == [1, 10]
    # Value-set column carries allowed_values.
    status_col = next(c for c in payload["table"]["data_columns"] if c["name"] == "status")
    assert status_col["allowed_values"] == ["active", "closed"]


# ── SQL validator ────────────────────────────────────────────────────────────

def test_validate_entity_sql_happy_path():
    sql = (
        "INSERT INTO ex_loans (amount, status, ex_lender_id) "
        "VALUES (1000, 'active', 1);\n"
        "INSERT INTO ex_loans (amount, status, ex_lender_id) "
        "VALUES (2000, 'closed', 2);"
    )
    assert _validate_entity_sql(sql, "ex_loans", 2) == []


def test_validate_entity_sql_wrong_table():
    sql = "INSERT INTO ex_other (amount) VALUES (1);"
    errors = _validate_entity_sql(sql, "ex_loans", 1)
    assert any("wrong table" in e for e in errors)


def test_validate_entity_sql_wrong_row_count():
    sql = "INSERT INTO ex_loans (amount) VALUES (1);"
    errors = _validate_entity_sql(sql, "ex_loans", 5)
    assert any("expected exactly 5" in e for e in errors)


def test_validate_entity_sql_prose_leak():
    errors = _validate_entity_sql("Sure, here's the data!", "ex_loans", 1)
    assert errors and "no INSERT" in errors[0]


def test_validate_entity_sql_strips_fences():
    sql = (
        "```sql\n"
        "INSERT INTO ex_loans (amount) VALUES (1);\n"
        "```"
    )
    assert _validate_entity_sql(sql, "ex_loans", 1) == []


# ── End-to-end with stubbed async client ─────────────────────────────────────

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
        self._responder = responder

    async def create(self, *, model, max_tokens, system, messages):
        first_user = next(m["content"] for m in messages if m["role"] == "user")
        return _FakeResponse(self._responder(first_user))


class _FakeAsyncClient:
    def __init__(self, responder) -> None:
        self.messages = _FakeMessages(responder)

    async def close(self) -> None:
        pass


def _reply_for(table_name: str, n: int) -> str:
    rows = []
    if table_name == "ex_lenders":
        for i in range(1, n + 1):
            rows.append(f"INSERT INTO ex_lenders (lender_name) VALUES ('Lender {i}');")
    elif table_name == "ex_loans":
        for i in range(1, n + 1):
            status = "active" if i % 2 else "closed"
            rows.append(
                f"INSERT INTO ex_loans (amount, status, ex_lender_id) "
                f"VALUES ({1000 * i}, '{status}', {i});"
            )
    return "\n".join(rows)


def test_seed_schema_happy_path(tmp_path):
    """End-to-end seeding: entity tables via fake LLM, lookups and
    junctions emitted deterministically, output ordered by creation_order."""
    schema = _fake_schema()

    def responder(user_text: str) -> str:
        if "name: ex_lenders" in user_text:
            return _reply_for("ex_lenders", 10)
        if "name: ex_loans" in user_text:
            return _reply_for("ex_loans", 10)
        raise AssertionError(f"Unexpected call: {user_text[:100]!r}")

    fake = _FakeAsyncClient(responder)

    class _FakeDomain:
        dir_name = "ex"
        generated_dir = tmp_path

    sql, results = seed_schema(
        _FakeDomain(),  # type: ignore[arg-type]
        schema,
        api_key="unused", arch_cfg=_TEST_ARCH_CFG,
        client=fake,
        verbose=False,
        llm_model="test",
    )

    assert all(r.ok for r in results), [r.error for r in results if not r.ok]
    assert len(results) == 2  # two entity tables

    # Creation order: lookup, then lenders, then loans, then junction.
    idx_lookup = sql.index("-- ex_loan_status_values")
    idx_lenders = sql.index("-- ex_lenders")
    idx_loans = sql.index("-- ex_loans")
    idx_junction = sql.index("-- ex_loan_guarantors")
    assert idx_lookup < idx_lenders < idx_loans < idx_junction

    # Entity SQL is present.
    assert sql.count("INSERT INTO ex_lenders") == 10
    assert sql.count("INSERT INTO ex_loans") == 10
    # Lookup SQL.
    assert sql.count("INSERT INTO ex_loan_status_values") == 2
    # Junction SQL (default 5 pairs).
    assert sql.count("INSERT INTO ex_loan_guarantors") == 5


def test_seed_schema_retries_on_wrong_table(tmp_path):
    """First LLM call uses the wrong table name; second call corrects it."""
    schema = _fake_schema()
    attempts = {"lenders": 0}

    def responder(user_text: str) -> str:
        if "name: ex_lenders" in user_text:
            attempts["lenders"] += 1
            if attempts["lenders"] == 1:
                # Wrong table — should trigger validation reprompt.
                return "\n".join(
                    f"INSERT INTO wrong_table (x) VALUES ({i});"
                    for i in range(1, 11)
                )
            return _reply_for("ex_lenders", 10)
        if "name: ex_loans" in user_text:
            return _reply_for("ex_loans", 10)
        raise AssertionError(f"Unexpected: {user_text[:100]!r}")

    fake = _FakeAsyncClient(responder)

    class _FakeDomain:
        dir_name = "ex"
        generated_dir = tmp_path

    sql, results = seed_schema(
        _FakeDomain(),  # type: ignore[arg-type]
        schema,
        api_key="unused", arch_cfg=_TEST_ARCH_CFG,
        client=fake,
        verbose=False,
        llm_model="test",
    )

    assert all(r.ok for r in results)
    assert attempts["lenders"] == 2
    assert "wrong_table" not in sql
