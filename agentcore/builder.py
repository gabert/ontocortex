"""Schema Builder — Phase 2 of the scaled architect pipeline.

Per-module LLM calls that run in parallel. Each module receives a
tight ontology slice and returns only data-column detail. The
Reconciler (Phase 3) merges the results deterministically.

Why per-module: every hard structural decision — table set, PKs, FK
placement, junction tables — is already pinned by the Planner. The
Builder's job is narrow and embarrassingly parallel. See
NOTES_builder_contract.md for the full input/output contract.

Concurrency is capped at 5 simultaneous calls (plenty for real
domains, polite to the Anthropic API). Failures per module are
surfaced as `ModuleBuildResult(ok=False)` rather than raised — the
caller decides whether to retry, bail, or proceed. The Reconciler
will refuse to merge a partial set, so retries must be complete
before reconciliation.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from anthropic import APIStatusError, AsyncAnthropic

from agentcore.build_validation import collect_build_errors
from agentcore.domain import DomainConfig
from agentcore.reconciler import BUILDS_SUBDIR

_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS = 4096
_MAX_CONCURRENCY = 5

# Anthropic SDK handles 429/529 transparently with exponential backoff
# and `retry-after` awareness when we set max_retries on the client.
# We also run a validation feedback loop on top — the LLM gets up to
# _MAX_VALIDATION_ATTEMPTS tries to produce a response that passes
# `collect_build_errors`. First attempt + 2 corrective re-prompts.
_SDK_MAX_RETRIES = 5
_MAX_VALIDATION_ATTEMPTS = 3

Granularity = Literal["module", "table"]
GRANULARITY_MODULE: Granularity = "module"
GRANULARITY_TABLE: Granularity = "table"


_SYSTEM_PROMPT = """\
You are a database column designer. You will be given a small slice \
of an ontology describing one module's tables and the datatype \
properties that apply to them. Your job: emit the list of data \
columns for each table.

RULES:
- For each table, emit one column per datatype property in \
`ontology_slice.datatype_properties` whose `on_table` equals that \
table's name. The column name MUST equal the property's `snake_name` \
exactly — do not rename.
- Use only these logical types: string, text, integer, decimal, \
boolean, date, datetime. Pick the closest match to the property's \
`range`. `value_set:*` ranges map to `string`.
- Set `not_null: true` and `required: true` for every column unless \
the property's comment clearly implies optionality.
- Set `unique: true` for obvious business identifiers (names, codes, \
numbers, license/policy/loan numbers). Default is `unique: false`.
- DO NOT emit: primary keys, foreign keys, `ontology_iri`, table \
definitions not in `tables`, or columns whose snake_name is not in \
`ontology_slice.datatype_properties`. The reconciler injects PKs and \
FKs and will REJECT any column it does not recognize.

OUTPUT:
Return a single JSON object, no prose, no markdown fences:

{
  "module": "<module name>",
  "tables": [
    {
      "name": "<table_name>",
      "columns": [
        {"name": "...", "type": "...", "not_null": true, "required": true, "unique": false}
      ]
    }
  ]
}
"""


class BuildError(RuntimeError):
    """Raised inside a single module call when the LLM response cannot
    be turned into a valid module build. Caught by the caller and
    converted into a ModuleBuildResult(ok=False)."""


@dataclass
class ModuleBuildResult:
    name: str
    ok: bool
    path: Path | None = None
    error: str | None = None
    skipped: bool = False  # True when resumed from existing build file


def _module_plan_hash(module_input: dict) -> str:
    """Canonical hash of the exact LLM-input payload for one module.

    Used as the resume cache key: if an existing `module_<name>.json`
    carries the same `plan_hash`, we know the next LLM call would send
    byte-identical input, so the stored output is reusable. Any plan
    edit that touches this module — renamed table, new FK, changed
    datatype property, added value set member — changes the payload
    and therefore the hash.
    """
    canonical = json.dumps(module_input, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _load_cached_build(path: Path, expected_hash: str) -> list[dict] | None:
    """Return cached `tables` iff the file exists and its hash matches.

    Any parse error or hash mismatch returns None — the caller then
    rebuilds. We never raise from the cache path so a corrupt leftover
    file can't block a fresh build.
    """
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("plan_hash") != expected_hash:
        return None
    tables = data.get("tables")
    if not isinstance(tables, list):
        return None
    return tables


# ── Slice assembly ───────────────────────────────────────────────────────────

def _local_from_iri(iri: str) -> str:
    return iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def _range_label(dp: dict, value_set_iris: set[str]) -> str:
    rng = dp.get("range_iri") or ""
    if rng in value_set_iris:
        return f"value_set:{_local_from_iri(rng)}"
    return _local_from_iri(rng) or "string"


def _build_input(
    module_name: str,
    table_names: list[str],
    plan: dict,
    model: dict,
) -> dict:
    """Assemble the user-message payload for a subset of tables.

    Parameterized on the exact tables to include so the same helper
    powers both per-module calls (pass all tables in the module) and
    per-table calls (pass one). Pure function — no I/O, no LLM.
    """
    table_name_set = set(table_names)
    plan_tables_by_name = {t["name"]: t for t in plan["tables"]}

    pinned_by_table: dict[str, list[dict]] = {name: [] for name in table_name_set}
    for rel in plan["relationships"]:
        if rel["kind"] != "fk":
            continue
        child = rel["child_table"]
        if child in table_name_set:
            pinned_by_table[child].append({
                "column": rel["fk_column"],
                "references_table": rel["parent_table"],
                "property_iri": rel.get("iri"),
                "not_null": True,
            })

    tables_payload: list[dict] = []
    table_class_iris: set[str] = set()
    for name in sorted(table_name_set):
        t = plan_tables_by_name[name]
        table_class_iris.add(t["ontology_iri"])
        tables_payload.append({
            "name": name,
            "ontology_iri": t["ontology_iri"],
            "primary_key": t["primary_key"],
            "comment": t.get("comment", ""),
            "pinned_fk_columns": pinned_by_table[name],
        })

    name_by_iri = {t["ontology_iri"]: t["name"] for t in tables_payload}
    value_set_iris = {vs["iri"] for vs in model.get("value_sets") or []}

    datatype_slice: list[dict] = []
    vs_iris_referenced: set[str] = set()
    for dp in model["datatype_properties"]:
        hit = False
        for class_iri in dp.get("domain_iris") or []:
            if class_iri not in table_class_iris:
                continue
            hit = True
            datatype_slice.append({
                "name": dp["local_name"],
                "snake_name": dp["snake_name"],
                "on_table": name_by_iri[class_iri],
                "range": _range_label(dp, value_set_iris),
                "comment": dp.get("comment", "") or "",
            })
        # Only carry value sets for properties that actually land on
        # one of the tables in this payload — keeps per-table calls tight.
        if hit and dp.get("range_iri") in value_set_iris:
            vs_iris_referenced.add(dp["range_iri"])

    value_sets_payload: list[dict] = []
    for vs in model.get("value_sets") or []:
        if vs["iri"] not in vs_iris_referenced:
            continue
        value_sets_payload.append({
            "name": vs["local_name"],
            "members": [_local_from_iri(m) for m in vs["members"]],
        })

    return {
        "module": module_name,
        "tables": tables_payload,
        "ontology_slice": {
            "datatype_properties": datatype_slice,
            "value_sets": value_sets_payload,
        },
    }


def _build_module_input(module: dict, plan: dict, model: dict) -> dict:
    """Per-module payload: every table in the module, one LLM call.
    Thin wrapper around `_build_input` kept for test-surface stability.
    """
    return _build_input(
        module["name"],
        list(module.get("tables") or []),
        plan,
        model,
    )


# ── Response parsing ─────────────────────────────────────────────────────────

def _extract_json(text: str) -> dict:
    """Extract the first JSON object from a possibly fenced response."""
    stripped = text.strip()
    if stripped.startswith("```"):
        # ```json ... ``` or ``` ... ```
        lines = stripped.split("\n")
        stripped = "\n".join(lines[1:-1] if lines[-1].strip().startswith("```") else lines[1:])
    try:
        obj, _ = json.JSONDecoder().raw_decode(stripped.lstrip())
    except json.JSONDecodeError as e:
        raise BuildError(
            f"Response is not valid JSON: {e}. First 200 chars: {text[:200]!r}"
        ) from e
    if not isinstance(obj, dict):
        raise BuildError(f"Response JSON is not an object: {type(obj).__name__}")
    return obj


# ── LLM call with validation feedback loop ───────────────────────────────────

def _accepted_columns_by_table(module_input: dict) -> dict[str, set[str]]:
    """Derive the `accepted_columns_by_table` map validator needs.

    The ontology slice in the user payload already has `on_table`
    tagged on every datatype property, so this is just a grouping.
    """
    accepted: dict[str, set[str]] = {
        t["name"]: set() for t in module_input["tables"]
    }
    for dp in module_input["ontology_slice"]["datatype_properties"]:
        accepted.setdefault(dp["on_table"], set()).add(dp["snake_name"])
    return accepted


def _extract_text(response) -> str:
    return "".join(
        block.text for block in response.content
        if getattr(block, "type", None) == "text"
    )


async def _call_module(
    client: AsyncAnthropic,
    module_input: dict,
    sem: asyncio.Semaphore,
    verbose: bool,
) -> dict:
    """Run the LLM call with a validation feedback loop.

    On the first attempt we send the module/table slice as a single
    user turn. If the response fails JSON parse or the validation
    check, we continue the conversation: append the bad response as
    the assistant turn and a corrective user turn that spells out the
    errors, then call again. Up to _MAX_VALIDATION_ATTEMPTS tries
    total.

    429/529 throttling is handled transparently by the Anthropic SDK
    (the client is constructed with max_retries=5), so this function
    only concerns itself with semantic validation.
    """
    user_message = yaml.safe_dump(
        module_input, sort_keys=False, default_flow_style=False, allow_unicode=True
    )
    expected_tables = [t["name"] for t in module_input["tables"]]
    accepted_cols = _accepted_columns_by_table(module_input)
    module_name = module_input["module"]

    messages: list[dict] = [{"role": "user", "content": user_message}]
    last_errors: list[str] = []

    async with sem:
        for attempt in range(_MAX_VALIDATION_ATTEMPTS):
            try:
                response = await client.messages.create(
                    model=_MODEL,
                    max_tokens=_MAX_TOKENS,
                    system=_SYSTEM_PROMPT,
                    messages=messages,
                )
            except APIStatusError as e:
                # SDK already retried 429/529; anything arriving here
                # is a non-transient failure.
                raise BuildError(f"LLM call failed: {e}") from e

            text = _extract_text(response)
            if verbose:
                usage = response.usage
                tag = f"attempt {attempt + 1}" if attempt else ""
                suffix = f" [{tag}]" if tag else ""
                print(
                    f"  [{module_name}] {usage.input_tokens} in / "
                    f"{usage.output_tokens} out tokens{suffix}"
                )

            try:
                output = _extract_json(text)
            except BuildError as e:
                last_errors = [str(e)]
                if attempt == _MAX_VALIDATION_ATTEMPTS - 1:
                    break
                messages.append({"role": "assistant", "content": text})
                messages.append({
                    "role": "user",
                    "content": (
                        f"Your previous response was not valid JSON: {e}. "
                        "Return ONLY a single JSON object matching the schema "
                        "in the system prompt — no prose, no markdown fences."
                    ),
                })
                continue

            errors = collect_build_errors(
                output,
                expected_table_names=expected_tables,
                accepted_columns_by_table=accepted_cols,
            )
            if not errors:
                return output

            last_errors = errors
            if attempt == _MAX_VALIDATION_ATTEMPTS - 1:
                break
            if verbose:
                print(
                    f"  [{module_name}] validation failed "
                    f"({len(errors)} issue(s)) — re-prompting"
                )

            messages.append({"role": "assistant", "content": text})
            messages.append({
                "role": "user",
                "content": (
                    "Your previous response has the following issues:\n- "
                    + "\n- ".join(errors)
                    + "\n\nFix ALL of them and return the corrected JSON "
                    "object only. Remember: column names must match "
                    "`snake_name` values from the ontology slice exactly, "
                    "and you may only emit tables that were requested."
                ),
            })

    raise BuildError(
        f"Module '{module_name}': build failed after "
        f"{_MAX_VALIDATION_ATTEMPTS} attempts. Final issues: {last_errors}"
    )


# ── Public API ───────────────────────────────────────────────────────────────

async def build_modules_async(
    domain: DomainConfig,
    plan: dict,
    api_key: str,
    *,
    granularity: Granularity = GRANULARITY_MODULE,
    client: AsyncAnthropic | None = None,
    model: dict | None = None,
    concurrency: int = _MAX_CONCURRENCY,
    verbose: bool = True,
    force: bool = False,
) -> list[ModuleBuildResult]:
    """Fan out LLM calls according to `granularity`, then write one
    `module_<name>.json` per module. Returns one result per module
    regardless of granularity, so the Reconciler contract is unchanged.

    granularity="module" — one LLM call per module containing every
    table in that module. Lower API round-trip count; the LLM sees
    sibling tables together and can keep naming consistent across
    them. Preferred default for small-to-mid domains.

    granularity="table" — one LLM call per table. Smallest per-call
    footprint; essential for large domains (100+ tables) where a
    single module would overflow context or drift. The per-module
    output file is re-assembled from the per-table responses.

    Resume: every `module_<name>.json` is stamped with a `plan_hash`
    computed from the canonical module-level payload. On rerun, if an
    existing file's hash matches what we'd send today, the module is
    reused as-is and no LLM call is made — only modules whose plan
    slice changed (or whose previous build failed) are rebuilt. Pass
    `force=True` to invalidate the cache and rebuild everything. The
    cache key is the module-level payload even in table mode, so
    switching granularity does not force a rebuild by itself.

    Tests pass an explicit `client` (a stub) to skip the real API.
    """
    if model is None:
        model = domain.ontology_model

    builds_dir = domain.generated_dir / BUILDS_SUBDIR
    builds_dir.mkdir(parents=True, exist_ok=True)

    owns_client = client is None
    if owns_client:
        # SDK handles 429/529 with exponential backoff + `retry-after`
        # when max_retries is set — no manual loop needed on our side.
        client = AsyncAnthropic(api_key=api_key, max_retries=_SDK_MAX_RETRIES)

    sem = asyncio.Semaphore(concurrency)

    def _write_module_file(name: str, tables: list[dict], plan_hash: str) -> Path:
        path = builds_dir / f"module_{name}.json"
        payload = {"module": name, "plan_hash": plan_hash, "tables": tables}
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        return path

    def _try_resume(name: str, plan_hash: str) -> ModuleBuildResult | None:
        if force:
            return None
        path = builds_dir / f"module_{name}.json"
        cached = _load_cached_build(path, plan_hash)
        if cached is None:
            return None
        if verbose:
            print(f"  [{name}] cached — plan slice unchanged, skipping LLM call")
        return ModuleBuildResult(name=name, ok=True, path=path, skipped=True)

    async def run_module_level(module: dict) -> ModuleBuildResult:
        name = module["name"]
        try:
            payload = _build_module_input(module, plan, model)
            plan_hash = _module_plan_hash(payload)
            cached = _try_resume(name, plan_hash)
            if cached is not None:
                return cached
            output = await _call_module(client, payload, sem, verbose)
            # _call_module already ran collect_build_errors — if we're
            # here, `output["tables"]` is present and structurally sound.
            path = _write_module_file(name, output["tables"], plan_hash)
            return ModuleBuildResult(name=name, ok=True, path=path)
        except Exception as e:
            return ModuleBuildResult(name=name, ok=False, error=str(e))

    async def run_table_level(module: dict) -> ModuleBuildResult:
        name = module["name"]
        table_names = list(module.get("tables") or [])
        # Module-level payload is the canonical cache key even in
        # table mode, so flipping --module/--table does not invalidate
        # existing builds.
        module_payload = _build_module_input(module, plan, model)
        plan_hash = _module_plan_hash(module_payload)
        cached = _try_resume(name, plan_hash)
        if cached is not None:
            return cached
        if not table_names:
            # Empty module — still write an empty build file so the
            # reconciler doesn't complain about a missing module.
            path = _write_module_file(name, [], plan_hash)
            return ModuleBuildResult(name=name, ok=True, path=path)

        async def run_one_table(
            table_name: str,
        ) -> tuple[str, dict | None, str | None]:
            try:
                payload = _build_input(name, [table_name], plan, model)
                output = await _call_module(client, payload, sem, verbose)
                tables = output.get("tables") or []
                if len(tables) != 1 or tables[0].get("name") != table_name:
                    raise BuildError(
                        f"Table '{table_name}': response must contain exactly "
                        f"one table named '{table_name}', got "
                        f"{[t.get('name') for t in tables]}"
                    )
                return table_name, tables[0], None
            except Exception as e:
                return table_name, None, str(e)

        table_results = await asyncio.gather(
            *(run_one_table(tn) for tn in table_names)
        )

        failed = [(tn, err) for tn, tbl, err in table_results if tbl is None]
        if failed:
            joined = "; ".join(f"{tn}: {err}" for tn, err in failed)
            return ModuleBuildResult(
                name=name, ok=False,
                error=f"{len(failed)}/{len(table_names)} table calls failed — {joined}",
            )

        # Preserve plan order within the module, not asyncio.gather order.
        by_name = {tn: tbl for tn, tbl, _ in table_results}
        ordered = [by_name[tn] for tn in table_names]
        path = _write_module_file(name, ordered, plan_hash)
        return ModuleBuildResult(name=name, ok=True, path=path)

    runner = run_module_level if granularity == GRANULARITY_MODULE else run_table_level

    try:
        results = await asyncio.gather(
            *(runner(m) for m in plan["modules"])
        )
    finally:
        if owns_client:
            await client.close()

    return results


def build_modules(
    domain: DomainConfig,
    plan: dict,
    api_key: str,
    *,
    granularity: Granularity = GRANULARITY_MODULE,
    client: AsyncAnthropic | None = None,
    model: dict | None = None,
    concurrency: int = _MAX_CONCURRENCY,
    verbose: bool = True,
    force: bool = False,
) -> list[ModuleBuildResult]:
    """Synchronous wrapper around build_modules_async for CLI use."""
    return asyncio.run(build_modules_async(
        domain, plan, api_key,
        granularity=granularity, client=client, model=model, force=force,
        concurrency=concurrency, verbose=verbose,
    ))
