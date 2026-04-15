"""Quote- and paren-aware SQL text utilities.

A small state machine, reused by:

- `schema.load_seed_sql` / `schema.validate_seed_data` — split a
  multi-statement script on `;` at the top level (outside strings
  and outside parenthesized groups).
- `seed_data._parse_inserts` — walk an INSERT's column list and
  VALUES body while treating `(...)` as atomic and respecting
  single-quoted strings with doubled-quote escapes.

One implementation so there's one place to get the escaping right.
"""

from __future__ import annotations


def split_top_level(sql: str, sep: str) -> list[str]:
    """Split `sql` on `sep` at the top level.

    "Top level" means: not inside a single-quoted string, and not
    inside a parenthesized group. Single quotes are escaped by
    doubling them (`''`) per standard SQL. Each returned chunk is
    stripped of leading/trailing whitespace; an empty tail chunk
    (trailing separator) is dropped, but empty mid-chunks are kept
    so callers can detect malformed input like `(1,,3)`.
    """
    out: list[str] = []
    buf: list[str] = []
    in_str = False
    depth = 0
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    buf.append(sql[i + 1])
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            buf.append(ch)
        elif ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            if depth > 0:
                depth -= 1
            buf.append(ch)
        elif ch == sep and depth == 0:
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def read_paren_group(sql: str, start: int) -> tuple[str, int]:
    """Read a `(...)` group starting at `sql[start]` (which must be `(`).

    Walks the body quote- and nesting-aware. Returns the inner body
    (no outer parens) and the index just past the closing `)`.
    Raises ValueError if the group is unterminated or `sql[start]`
    is not an opening paren.
    """
    if start >= len(sql) or sql[start] != "(":
        raise ValueError(f"expected '(' at position {start}")
    i = start + 1
    depth = 1
    in_str = False
    buf: list[str] = []
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    buf.append(sql[i + 1])
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            buf.append(ch)
        elif ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return "".join(buf), i + 1
            buf.append(ch)
        else:
            buf.append(ch)
        i += 1
    raise ValueError("unterminated parenthesized group")
