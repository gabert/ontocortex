"""
CLI: LLM-driven schema design from OWL ontology.

Sends the domain ontology to Claude acting as a database architect.
Claude returns a normalised logical schema (JSON) and realistic seed
data (SQL).  Both files are saved next to the ontology and domain.json
is updated to reference them.

Usage:
    python scripts/design_schema.py              # pick domain interactively
    python scripts/design_schema.py insurance     # design for specific domain
"""

import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agentcore.architect import (
    design_schema,
    design_seed_data,
    fix_seed_data,
    generate_compact_ontology,
    update_domain_manifest,
)
from agentcore.config import ConfigError, load_config
from agentcore.domain import list_domains, load_domain
from agentcore.schema import render_ddl, validate_seed_data


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
    print(f"  {domain.name} — SCHEMA DESIGN")
    print("=" * 60 + "\n")

    # Wipe previous run
    generated_dir = domain.generated_dir
    if generated_dir.exists():
        shutil.rmtree(generated_dir)
        print(f"Deleted _generated/")

    manifest_path = domain.ontology_path.parent / "domain.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if "generated" in manifest:
        del manifest["generated"]
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print("Cleared 'generated' section from domain.json")

    print()
    print("Step 1/3: Building compact ontology...")
    compact, compact_file = generate_compact_ontology(domain)
    update_domain_manifest(domain, ontology_compact=compact_file)

    print("\nStep 2/4: Designing schema from compact ontology...")
    schema, schema_file = design_schema(app_cfg, domain, compact)
    for t in schema["tables"]:
        n_cols = len(t.get("columns", []))
        n_fks = len(t.get("foreign_keys", []))
        print(f"    {t['name']:20s}  {n_cols} cols  {n_fks} FKs")
    update_domain_manifest(domain, schema=schema_file)

    print("\nStep 3/4: Rendering DDL...")
    ddl = render_ddl(schema)
    ddl_file = f"{domain.dir_name}_schema.sql"
    (domain.generated_dir / ddl_file).write_text(ddl + "\n", encoding="utf-8")
    print(f"  Saved _generated/{ddl_file}")
    update_domain_manifest(domain, ddl=ddl_file)

    _MAX_FIX_ATTEMPTS = 3

    print("\nStep 4/5: Generating seed data...")
    sql, seed_file = design_seed_data(app_cfg, domain, schema, compact)
    seed_path = domain.generated_dir / seed_file
    n_inserts = sql.upper().count("INSERT INTO")
    print(f"    {n_inserts} INSERT statements generated.")
    update_domain_manifest(domain, seed_data=seed_file)

    print("\nStep 5/5: Validating seed data...")
    for attempt in range(1, _MAX_FIX_ATTEMPTS + 2):
        errors = validate_seed_data(schema, sql)
        if not errors:
            n_inserts = sql.upper().count("INSERT INTO")
            print(f"    OK — all {n_inserts} inserts passed.")
            break
        print(f"    {len(errors)} error(s) on attempt {attempt}:")
        for err in errors:
            print(f"      {err}")
        if attempt > _MAX_FIX_ATTEMPTS:
            print(f"    Giving up after {_MAX_FIX_ATTEMPTS} fix attempt(s).")
            break
        print(f"    Asking LLM to fix (attempt {attempt}/{_MAX_FIX_ATTEMPTS})...")
        sql = fix_seed_data(app_cfg, domain, schema, compact, sql, errors)
        seed_path.write_text(sql + "\n", encoding="utf-8")

    print("\n" + "=" * 60)
    print("  Done. Files generated:")
    print(f"    {domain.dir_name}/_generated/{compact_file}")
    print(f"    {domain.dir_name}/_generated/{schema_file}")
    print(f"    {domain.dir_name}/_generated/{ddl_file}")
    print(f"    {domain.dir_name}/_generated/{seed_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
