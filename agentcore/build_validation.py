"""Shared validation for Builder output.

Used by the Builder to catch prompt drift *before* a build file is
written (so the LLM can be re-prompted with specific corrections) and
by the Reconciler as a second-line defense at merge time.

The rules here are the subset of checks that depend only on the
module/table build output + the ontology slice. FK injection and
creation-order checks live in the Reconciler because they need the
full plan, not just one module's slice.
"""

from __future__ import annotations

# Same logical type set the downstream schema.py understands.
LOGICAL_TYPES = frozenset({
    "string", "text", "integer", "decimal", "boolean", "date", "datetime",
})


def collect_build_errors(
    build_output: object,
    *,
    expected_table_names: list[str],
    accepted_columns_by_table: dict[str, set[str]],
) -> list[str]:
    """Return a list of human-readable problems in `build_output`.

    An empty list means the output is structurally valid and every
    column name maps to a real datatype property in the slice. The
    Builder feeds each message back to the LLM verbatim as correction
    instructions.

    Parameters:
        build_output: the parsed JSON dict the LLM returned.
        expected_table_names: the table names this call was supposed
            to cover — either every table in a module (per-module
            granularity) or a single table (per-table granularity).
        accepted_columns_by_table: for each expected table, the set
            of snake_case property names that are allowed as columns.
    """
    errors: list[str] = []

    if not isinstance(build_output, dict):
        return [f"response is not a JSON object (got {type(build_output).__name__})"]

    tables = build_output.get("tables")
    if not isinstance(tables, list):
        return ["response is missing a 'tables' array"]

    seen_names: set[str] = set()
    expected_set = set(expected_table_names)

    for idx, table in enumerate(tables):
        if not isinstance(table, dict):
            errors.append(f"tables[{idx}] is not a JSON object")
            continue

        name = table.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"tables[{idx}] is missing a 'name' string")
            continue

        if name in seen_names:
            errors.append(f"table '{name}' appears more than once in the response")
            continue
        seen_names.add(name)

        if name not in expected_set:
            errors.append(
                f"table '{name}' was not requested. Expected only: "
                f"{sorted(expected_set)}"
            )
            continue

        accepted = accepted_columns_by_table.get(name, set())
        col_names_seen: set[str] = set()

        columns = table.get("columns") or []
        if not isinstance(columns, list):
            errors.append(f"table '{name}': 'columns' must be an array")
            continue

        for col_idx, col in enumerate(columns):
            if not isinstance(col, dict):
                errors.append(
                    f"table '{name}' columns[{col_idx}] is not a JSON object"
                )
                continue
            col_name = col.get("name")
            col_type = col.get("type")
            if not isinstance(col_name, str) or not col_name:
                errors.append(
                    f"table '{name}' columns[{col_idx}] is missing a 'name' string"
                )
                continue
            if col_name in col_names_seen:
                errors.append(
                    f"table '{name}' has duplicate column '{col_name}'"
                )
                continue
            col_names_seen.add(col_name)

            if col_type not in LOGICAL_TYPES:
                errors.append(
                    f"table '{name}' column '{col_name}' has invalid type "
                    f"'{col_type}'. Allowed types: {sorted(LOGICAL_TYPES)}"
                )
            if col_name not in accepted:
                errors.append(
                    f"table '{name}' column '{col_name}' is not a datatype "
                    f"property in the ontology slice. Allowed column names "
                    f"for '{name}' are: {sorted(accepted)}"
                )

            # The Builder prompt demands these three boolean flags on
            # every column. They must be present and actual booleans —
            # "true"/"yes"/1 all pass downstream JSON schemas silently
            # but break the reconciler's `.get("required")` / REQUIRED
            # tags in the runtime schema description.
            for flag in ("not_null", "required", "unique"):
                if flag not in col:
                    errors.append(
                        f"table '{name}' column '{col_name}' is missing "
                        f"required boolean field '{flag}'"
                    )
                elif not isinstance(col[flag], bool):
                    errors.append(
                        f"table '{name}' column '{col_name}' field "
                        f"'{flag}' must be a JSON boolean (true/false), "
                        f"got {type(col[flag]).__name__}: {col[flag]!r}"
                    )

    missing = expected_set - seen_names
    if missing:
        errors.append(
            f"response is missing tables that were requested: {sorted(missing)}"
        )

    return errors
