"""Unit tests for the shared SQL text state machine."""

from __future__ import annotations

import pytest

from agentcore.sql_text import read_paren_group, split_top_level


# ── split_top_level ──────────────────────────────────────────────────────────

def test_split_simple():
    assert split_top_level("a;b;c", ";") == ["a", "b", "c"]


def test_split_strips_whitespace():
    assert split_top_level(" a ; b ; c ", ";") == ["a", "b", "c"]


def test_split_empty_tail_dropped():
    assert split_top_level("a;b;", ";") == ["a", "b"]


def test_split_ignores_sep_in_quoted_string():
    sql = "INSERT INTO t VALUES ('one; two'); INSERT INTO t VALUES ('three')"
    parts = split_top_level(sql, ";")
    assert len(parts) == 2
    assert "one; two" in parts[0]


def test_split_handles_doubled_quote_escape():
    sql = "INSERT INTO t VALUES ('O''Reilly;Jr'); INSERT INTO t VALUES ('x')"
    parts = split_top_level(sql, ";")
    assert len(parts) == 2
    assert "O''Reilly;Jr" in parts[0]


def test_split_ignores_sep_inside_parens():
    sql = "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 2)"
    parts = split_top_level(sql, ";")
    assert len(parts) == 2


def test_split_values_with_paren_in_string():
    # The blocker case: `)` inside a string must not close the VALUES
    # body early, and `,` inside the string must not split values.
    body = "1, 'Smith (Jr.), Esq.', 'ok'"
    values = split_top_level(body, ",")
    assert values == ["1", "'Smith (Jr.), Esq.'", "'ok'"]


def test_split_values_with_apostrophe():
    body = "1, 'O''Reilly', 2"
    values = split_top_level(body, ",")
    assert values == ["1", "'O''Reilly'", "2"]


# ── read_paren_group ─────────────────────────────────────────────────────────

def test_read_paren_simple():
    body, end = read_paren_group("(a, b, c)", 0)
    assert body == "a, b, c"
    assert end == 9


def test_read_paren_nested():
    body, end = read_paren_group("(a, (b, c), d)xyz", 0)
    assert body == "a, (b, c), d"
    assert end == 14


def test_read_paren_respects_strings():
    body, _ = read_paren_group("('a)b', 2)", 0)
    assert body == "'a)b', 2"


def test_read_paren_doubled_quote_escape():
    body, _ = read_paren_group("('O''Reilly', 1)", 0)
    assert body == "'O''Reilly', 1"


def test_read_paren_unterminated_raises():
    with pytest.raises(ValueError, match="unterminated"):
        read_paren_group("(a, b", 0)


def test_read_paren_wrong_start_raises():
    with pytest.raises(ValueError, match="expected '\\('"):
        read_paren_group("abc", 0)
