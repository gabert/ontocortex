"""Unit tests for collect_build_errors — the shared validator that
guards Builder output before it reaches the Reconciler."""

from __future__ import annotations

from agentcore.build_validation import collect_build_errors


_EXPECTED = ["ex_lenders", "ex_loans"]
_ACCEPTED = {
    "ex_lenders": {"lender_name"},
    "ex_loans": {"amount", "status"},
}


def _col(name: str, type_: str, **overrides) -> dict:
    """Build a column dict with the three required boolean flags.

    Tests use this so every fixture satisfies the tightened validator
    by default; individual tests override fields to exercise specific
    failure modes.
    """
    return {
        "name": name,
        "type": type_,
        "not_null": True,
        "required": True,
        "unique": False,
        **overrides,
    }


def _check(output: object) -> list[str]:
    return collect_build_errors(
        output,
        expected_table_names=_EXPECTED,
        accepted_columns_by_table=_ACCEPTED,
    )


def test_valid_build_has_no_errors():
    output = {
        "module": "ex",
        "tables": [
            {"name": "ex_lenders", "columns": [_col("lender_name", "string")]},
            {"name": "ex_loans", "columns": [
                _col("amount", "decimal"),
                _col("status", "string"),
            ]},
        ],
    }
    assert _check(output) == []


def test_non_dict_output():
    assert _check(["not", "a", "dict"]) == [
        "response is not a JSON object (got list)"
    ]


def test_missing_tables_key():
    assert _check({"module": "ex"}) == ["response is missing a 'tables' array"]


def test_missing_requested_table():
    output = {
        "tables": [
            {"name": "ex_lenders", "columns": [_col("lender_name", "string")]},
        ],
    }
    errors = _check(output)
    assert any("missing tables" in e and "ex_loans" in e for e in errors)


def test_extra_unrequested_table():
    output = {
        "tables": [
            {"name": "ex_ghosts", "columns": []},
        ],
    }
    errors = _check(output)
    assert any("'ex_ghosts' was not requested" in e for e in errors)


def test_hallucinated_column_name():
    output = {
        "tables": [
            {"name": "ex_lenders", "columns": [_col("lender_name", "string")]},
            {"name": "ex_loans", "columns": [
                _col("amount", "decimal"),
                _col("status", "string"),
                _col("made_up_field", "string"),
            ]},
        ],
    }
    errors = _check(output)
    assert any("made_up_field" in e and "ontology slice" in e for e in errors)


def test_unknown_logical_type():
    output = {
        "tables": [
            {"name": "ex_lenders", "columns": [_col("lender_name", "money")]},
            {"name": "ex_loans", "columns": [
                _col("amount", "decimal"),
                _col("status", "string"),
            ]},
        ],
    }
    errors = _check(output)
    assert any("'money'" in e and "invalid type" in e for e in errors)


def test_duplicate_columns_in_table():
    output = {
        "tables": [
            {"name": "ex_lenders", "columns": [_col("lender_name", "string")]},
            {"name": "ex_loans", "columns": [
                _col("amount", "decimal"),
                _col("amount", "decimal"),
                _col("status", "string"),
            ]},
        ],
    }
    errors = _check(output)
    assert any("duplicate column 'amount'" in e for e in errors)


def test_duplicate_table_entries():
    output = {
        "tables": [
            {"name": "ex_lenders", "columns": [_col("lender_name", "string")]},
            {"name": "ex_lenders", "columns": [_col("lender_name", "string")]},
            {"name": "ex_loans", "columns": [
                _col("amount", "decimal"),
                _col("status", "string"),
            ]},
        ],
    }
    errors = _check(output)
    assert any("'ex_lenders' appears more than once" in e for e in errors)


def test_missing_required_flag():
    bad_col = _col("lender_name", "string")
    del bad_col["not_null"]
    output = {
        "tables": [
            {"name": "ex_lenders", "columns": [bad_col]},
            {"name": "ex_loans", "columns": [
                _col("amount", "decimal"),
                _col("status", "string"),
            ]},
        ],
    }
    errors = _check(output)
    assert any(
        "lender_name" in e and "not_null" in e and "missing" in e
        for e in errors
    )


def test_non_boolean_flag_rejected():
    output = {
        "tables": [
            {"name": "ex_lenders", "columns": [
                _col("lender_name", "string", unique="true"),
            ]},
            {"name": "ex_loans", "columns": [
                _col("amount", "decimal"),
                _col("status", "string"),
            ]},
        ],
    }
    errors = _check(output)
    assert any(
        "lender_name" in e and "unique" in e and "boolean" in e
        for e in errors
    )
