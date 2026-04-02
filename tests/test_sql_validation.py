"""Tests for _reject_non_parameterized in sql_tool."""

import pytest
from agentcore.tools.sql_tool import _reject_non_parameterized


# ── Valid queries — should return None ────────────────────────────────────────

def test_valid_single_param():
    assert _reject_non_parameterized(
        "SELECT * FROM clients WHERE email = :email",
        {"email": "john@example.com"},
    ) is None


def test_valid_multiple_params():
    assert _reject_non_parameterized(
        "INSERT INTO clients (name, age) VALUES (:name, :age)",
        {"name": "John", "age": 30},
    ) is None


def test_valid_no_placeholders_empty_params():
    assert _reject_non_parameterized(
        "SELECT * FROM clients",
        {},
    ) is None


def test_valid_same_param_used_twice():
    assert _reject_non_parameterized(
        "SELECT * FROM t WHERE start = :date OR end = :date",
        {"date": "2024-01-01"},
    ) is None


def test_valid_postgres_cast_not_flagged():
    # ::text is a PostgreSQL type cast — should not be treated as a placeholder
    assert _reject_non_parameterized(
        "SELECT id::text FROM clients WHERE age = :age",
        {"age": 30},
    ) is None


# ── Embedded string literals ───────────────────────────────────────────────────

def test_rejects_embedded_string_in_where():
    result = _reject_non_parameterized(
        "SELECT * FROM clients WHERE email = 'john@example.com'",
        {},
    )
    assert result["error"] == "non_parameterized_sql"


def test_rejects_embedded_string_in_insert():
    result = _reject_non_parameterized(
        "INSERT INTO clients (name) VALUES ('John')",
        {},
    )
    assert result["error"] == "non_parameterized_sql"


def test_rejects_empty_string_literal():
    result = _reject_non_parameterized(
        "SELECT * FROM t WHERE name = ''",
        {},
    )
    assert result["error"] == "non_parameterized_sql"


# ── Placeholder / params mismatch ─────────────────────────────────────────────

def test_rejects_placeholder_missing_from_params():
    result = _reject_non_parameterized(
        "SELECT * FROM clients WHERE email = :email",
        {},
    )
    assert result["error"] == "params_mismatch"
    assert "email" in result["detail"]


def test_rejects_extra_param_not_in_query():
    result = _reject_non_parameterized(
        "SELECT * FROM clients",
        {"email": "john@example.com"},
    )
    assert result["error"] == "params_mismatch"
    assert "email" in result["detail"]


def test_rejects_partial_params_missing():
    result = _reject_non_parameterized(
        "INSERT INTO clients (name, email) VALUES (:name, :email)",
        {"name": "John"},          # email missing
    )
    assert result["error"] == "params_mismatch"
    assert "email" in result["detail"]
