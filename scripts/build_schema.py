"""
CLI: Phase 2 + 3 of the scaled architect pipeline.

Given an existing schema plan (from `scripts/design_plan.py`), run the
Builder in parallel, then the deterministic Reconciler, then render
the resulting logical schema as PostgreSQL DDL.

Usage:
    python scripts/build_schema.py                            # pick domain interactively
    python scripts/build_schema.py student_loans              # build for specific domain
    python scripts/build_schema.py student_loans --table      # per-table parallelism
    python scripts/build_schema.py big_domain --table -c 20   # with concurrency 20

Options:
    --module             Per-module LLM calls (default; one call per module)
    --table              Per-table LLM calls (one call per table; use for large domains)
    -c N, --concurrency  Max simultaneous LLM calls (default 5)

Outputs under `domains/<name>/_generated/`:
    _builds/module_<name>.json    — per-module Builder output (one per module)
    <name>_schema.json            — merged logical schema
    <name>_schema.sql             — rendered DDL for inspection

The plan file must already exist. Run `scripts/design_plan.py` first.
"""

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from agentcore.builder import GRANULARITY_MODULE, GRANULARITY_TABLE, build_modules
from agentcore.config import ConfigError, load_config
from agentcore.domain import list_domains, load_domain, update_domain_manifest
from agentcore.planner import PLAN_SCHEMA_VERSION, ontology_hash
from agentcore.reconciler import ReconcileError, reconcile
from agentcore.schema import render_ddl
from agentcore.seed_data import (
    DEFAULT_ROWS_PER_TABLE,
    seed_schema,
    write_seed_file,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a logical schema from an existing plan (Phase 2 + 3).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "domain", nargs="?",
        help="Domain name (prompted if omitted and multiple domains exist)",
    )
    granularity = parser.add_mutually_exclusive_group()
    granularity.add_argument(
        "--module", dest="granularity", action="store_const",
        const=GRANULARITY_MODULE,
        help="One LLM call per module (default)",
    )
    granularity.add_argument(
        "--table", dest="granularity", action="store_const",
        const=GRANULARITY_TABLE,
        help="One LLM call per table (for large domains)",
    )
    parser.set_defaults(granularity=GRANULARITY_MODULE)
    parser.add_argument(
        "-c", "--concurrency", type=int, default=5,
        help="Max simultaneous LLM calls (default 5)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Ignore cached module builds and rebuild every module from "
             "scratch. By default, modules whose plan slice is unchanged "
             "are reused from `_builds/module_<name>.json`.",
    )
    parser.add_argument(
        "--seed", action="store_true",
        help="Also generate demo seed data (one LLM call per entity table). "
             "Off by default — seed data is for demo/dev only and scales with "
             "table count, so skip it for production or very large domains.",
    )
    parser.add_argument(
        "--seed-rows", type=int, default=DEFAULT_ROWS_PER_TABLE,
        help=f"Rows per entity table when --seed is set (default {DEFAULT_ROWS_PER_TABLE})",
    )
    return parser.parse_args()


def _pick_domain(domains_dir: Path, provided: str | None) -> str:
    available = list_domains(domains_dir)
    if not available:
        print(f"ERROR: No domains found in {domains_dir}")
        sys.exit(1)
    if provided:
        return provided
    if len(available) == 1:
        return available[0]
    print("Available domains:")
    for i, name in enumerate(available, 1):
        d = load_domain(name, domains_dir)
        print(f"  {i}. {name:12s}  {d.description}")
    print()
    choice = input("Select domain (number or name): ").strip()
    if choice.isdigit() and 1 <= int(choice) <= len(available):
        return available[int(choice) - 1]
    if choice in available:
        return choice
    print("Invalid choice.")
    sys.exit(1)


def _load_plan(domain) -> dict:
    plan_path = domain.schema_plan_path
    if plan_path is None or not plan_path.exists():
        print(
            "ERROR: No schema plan found for this domain.\n"
            f"Run: python scripts/design_plan.py {domain.dir_name}"
        )
        sys.exit(1)
    plan = yaml.safe_load(plan_path.read_text(encoding="utf-8"))
    plan_version = plan.get("schema_version")
    if plan_version != PLAN_SCHEMA_VERSION:
        print(
            f"ERROR: Plan at {plan_path} has schema_version={plan_version!r} "
            f"but this build tool expects {PLAN_SCHEMA_VERSION}. The plan "
            f"file is stale — re-run: python scripts/design_plan.py {domain.dir_name}"
        )
        sys.exit(1)
    if not plan.get("valid", True):
        print(
            f"ERROR: Plan at {plan_path} is marked invalid. "
            "Fix the ontology or overrides and re-run design_plan."
        )
        sys.exit(1)

    # Fail fast on ontology drift so we don't burn LLM budget on a
    # stale plan. The reconciler does the same check, but that's after
    # the Builder has already run.
    plan_hash = plan.get("ontology_hash")
    if plan_hash:
        current_hash = ontology_hash(domain.ontology_text)
        if current_hash != plan_hash:
            print(
                f"ERROR: Ontology has changed since the plan was built.\n"
                f"  Plan hash    : {plan_hash[:16]}...\n"
                f"  Current hash : {current_hash[:16]}...\n"
                f"  Source       : {domain.ontology_path}\n"
                f"Re-run: python scripts/design_plan.py {domain.dir_name}"
            )
            sys.exit(1)

    return plan


def main() -> None:
    args = _parse_args()
    try:
        app_cfg = load_config()
    except ConfigError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    domain_name = _pick_domain(app_cfg.domains_dir, args.domain)
    domain = load_domain(domain_name, app_cfg.domains_dir)
    plan = _load_plan(domain)

    print("=" * 60)
    print(f"  {domain.name} — BUILD + RECONCILE")
    print("=" * 60 + "\n")

    # Phase 2: LLM calls in parallel at the chosen granularity.
    n_modules = len(plan["modules"])
    n_tables = sum(len(m.get("tables") or []) for m in plan["modules"])
    unit = "module" if args.granularity == GRANULARITY_MODULE else "table"
    n_units = n_modules if unit == "module" else n_tables
    print(
        f"  Building {n_units} {unit}(s) in parallel "
        f"(concurrency={args.concurrency})..."
    )
    results = build_modules(
        domain, plan,
        api_key=app_cfg.api_key,
        granularity=args.granularity,
        concurrency=args.concurrency,
        verbose=True,
        force=args.force,
    )

    failed = [r for r in results if not r.ok]
    if failed:
        print("\n  Builder failures:")
        for r in failed:
            print(f"    [{r.name}] {r.error}")
        print(
            "\n  Some modules failed. The Reconciler refuses to merge a "
            "partial set. Fix the issue(s) and re-run this command to "
            "rebuild all modules."
        )
        sys.exit(2)

    n_cached = sum(1 for r in results if r.skipped)
    n_built = n_modules - n_cached
    if n_cached:
        print(
            f"  All {n_modules} module(s) ready "
            f"({n_built} built, {n_cached} cached).\n"
        )
    else:
        print(f"  All {n_modules} module(s) built successfully.\n")

    # Phase 3: deterministic merge.
    print("  Reconciling plan + module builds into logical schema...")
    try:
        schema, schema_file = reconcile(domain, plan)
    except ReconcileError as e:
        print(f"\nERROR: Reconcile failed:\n  {e}")
        sys.exit(1)

    # Render DDL for inspection (no DB connection needed).
    ddl = render_ddl(schema)
    sql_file = f"{domain.dir_name}_schema.sql"
    sql_path = domain.generated_dir / sql_file
    sql_path.write_text(ddl + "\n", encoding="utf-8", newline="\n")
    print(f"  Rendered DDL → _generated/{sql_file}")

    update_domain_manifest(domain, schema=schema_file)

    print()
    n_tables = len(schema["tables"])
    n_fks = sum(len(t.get("foreign_keys") or []) for t in schema["tables"])
    n_cols = sum(len(t.get("columns") or []) for t in schema["tables"])
    print(f"    Tables : {n_tables}")
    print(f"    Columns: {n_cols}")
    print(f"    FKs    : {n_fks}")

    # Phase 4 (optional): seed data.
    if args.seed:
        print()
        n_entities = sum(
            1 for t in schema["tables"]
            if not t.get("lookup_table")
            and (t.get("columns") or [])
        )
        print(
            f"  Generating seed data for {n_entities} entity table(s) "
            f"at {args.seed_rows} rows each (concurrency={args.concurrency})..."
        )
        sql, seed_results = seed_schema(
            domain, schema,
            api_key=app_cfg.api_key,
            rows_per_table=args.seed_rows,
            concurrency=args.concurrency,
            verbose=True,
        )
        seed_failed = [r for r in seed_results if not r.ok]
        if seed_failed:
            print("\n  Seed failures:")
            for r in seed_failed:
                print(f"    [{r.name}] {r.error}")
            print(
                "\n  Seed generation partially failed. Schema.json and "
                "schema.sql are still valid — re-run with --seed to retry."
            )
            sys.exit(3)

        seed_file = write_seed_file(domain, sql)
        update_domain_manifest(domain, seed_data=seed_file)
        print(f"  Seed SQL → _generated/{seed_file}")

    print("\n" + "=" * 60)
    print("  Done. Next step:")
    print(f"    python scripts/setup_database.py {domain.dir_name}")
    print("=" * 60)


if __name__ == "__main__":
    main()
