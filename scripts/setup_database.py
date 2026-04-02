"""
CLI entry point for domain database setup.

Usage:
    python scripts/setup_database.py              # pick domain interactively
    python scripts/setup_database.py insurance    # set up the insurance domain
    python scripts/setup_database.py vet          # set up the vet clinic domain
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agentcore.config import ConfigError, load_config
from agentcore.domain import list_domains, load_domain
from agentcore.setup import install_domain


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
    print(f"  {domain.name} — DATABASE SETUP")
    print("=" * 60 + "\n")

    install_domain(app_cfg, domain)

    print("\n" + "=" * 60)
    print("  Done. Run 'python main.py' to start the agent.")
    print("=" * 60)


if __name__ == "__main__":
    main()
