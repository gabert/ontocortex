"""
CLI: deterministic schema plan from an OWL ontology.

Phase 1 of the scaled architect pipeline. Emits a topology-only schema
plan (tables + relationships + modules) without column detail. The plan
is the input to the Builder phase, which designs columns per module.

No LLM call — this is pure Python. Judgment calls (FK direction,
junctions, module grouping) can be resolved via an optional
`schema_overrides.yaml` in the domain directory.

Usage:
    python scripts/design_plan.py                  # pick domain interactively
    python scripts/design_plan.py student_loans    # plan for specific domain
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agentcore.config import ConfigError, load_config
from agentcore.domain import list_domains, load_domain, update_domain_manifest
from agentcore.planner import PlanValidationError, design_plan


def main() -> None:
    try:
        app_cfg = load_config()
    except ConfigError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    domains_dir = app_cfg.domains_dir
    available = list_domains(domains_dir)
    if not available:
        print(f"ERROR: No domains found in {domains_dir}")
        sys.exit(1)

    if len(sys.argv) > 1:
        domain_name = sys.argv[1]
    elif len(available) == 1:
        domain_name = available[0]
    else:
        print("Available domains:")
        for i, name in enumerate(available, 1):
            d = load_domain(name, domains_dir)
            print(f"  {i}. {name:12s}  {d.description}")
        print()
        choice = input("Select domain (number or name): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(available):
            domain_name = available[int(choice) - 1]
        elif choice in available:
            domain_name = choice
        else:
            print("Invalid choice.")
            sys.exit(1)

    domain = load_domain(domain_name, domains_dir)

    print("=" * 60)
    print(f"  {domain.name} — SCHEMA PLAN (deterministic)")
    print("=" * 60 + "\n")

    try:
        plan, plan_file = design_plan(domain)
    except PlanValidationError as e:
        print("  Plan validation failed:")
        print(str(e))
        sys.exit(1)

    update_domain_manifest(domain, schema_plan=plan_file)

    print()
    print(f"    Modules      : {len(plan['modules'])}")
    print(f"    Tables       : {len(plan['tables'])}")
    fks = sum(1 for r in plan["relationships"] if r.get("kind") == "fk")
    juncs = sum(1 for r in plan["relationships"] if r.get("kind") == "junction")
    print(f"    Relationships: {len(plan['relationships'])}  ({fks} FKs, {juncs} junctions)")
    order = plan.get("creation_order") or []
    if order:
        widest = max(len(lv["tables"]) for lv in order)
        print(f"    Creation order: {len(order)} levels (widest = {widest} parallel tables)")
    print()
    for m in plan["modules"]:
        print(f"    module {m['name']:15s}  {len(m.get('tables') or [])} tables")

    print("\n" + "=" * 60)
    print(f"  Done. Plan saved to:")
    print(f"    {domain.dir_name}/_generated/{plan_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
