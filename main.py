"""Interactive session with a domain agent."""

import sys

from agentcore.pipeline import AgentPipeline
from agentcore.config import ConfigError, load_config
from agentcore.database import execute_query
from agentcore.domain import list_domains, load_domain
from agentcore.domain.install import db_config_for_domain, database_ready, install_domain
from agentcore.identity import IdentityContext


def _error_hint(e: Exception) -> str:
    """Return a short, human-friendly description of the error."""
    name = type(e).__name__
    msg = str(e).lower()

    if "overloaded" in msg or "529" in msg:
        return "the AI service is temporarily overloaded."
    if "rate" in msg or "429" in msg:
        return "rate limit reached on the AI service."
    if "authentication" in msg or "401" in msg or "api_key" in msg:
        return "API key is invalid or missing."
    if "timeout" in msg or "timed out" in msg:
        return "the request timed out."
    if "connection" in msg and ("refused" in msg or "reset" in msg or "error" in msg):
        if "5432" in msg or "postgres" in msg or "psycopg" in name.lower():
            return "cannot connect to the database."
        return "cannot connect to the AI service."
    if "psycopg" in name.lower() or "operational" in name.lower():
        return f"database error ({name}: {e})"

    return f"{name}: {e}"


def _pick_identity(db_cfg, domain) -> tuple[IdentityContext | None, dict | None]:
    """Let the user pick an identity from existing rows, or skip."""
    if not domain.identity_entity:
        return None, None

    from agentcore.sif.mapping import build_schema_map_from_mapping
    if not domain.has_mapping:
        return None, None
    smap = build_schema_map_from_mapping(domain.ontology_model, domain.mapping_data)
    table = smap.tables.get(domain.identity_entity)
    if not table:
        return None, None

    rows = execute_query(
        db_cfg,
        f"SELECT * FROM {table.table_name} ORDER BY {table.primary_key} LIMIT 20",
    )
    if not rows or isinstance(rows, dict):
        print(f"  No {domain.identity_entity} records found — running without identity scoping.")
        return None, None

    print(f"\n  Select a {domain.identity_entity} to log in as:")
    for i, row in enumerate(rows, 1):
        # Show PK + first few data columns for identification
        pk_val = row[table.primary_key]
        display_cols = [f"{k}={v}" for k, v in row.items()
                        if k != table.primary_key and v is not None][:3]
        print(f"    {i}. [{pk_val}] {', '.join(display_cols)}")
    print(f"    0. Skip (no identity scoping)")

    while True:
        try:
            choice = input("  Choice: ").strip()
            if not choice:
                continue
            idx = int(choice)
            if idx == 0:
                return None, None
            if 1 <= idx <= len(rows):
                row = rows[idx - 1]
                pk_val = row[table.primary_key]
                print(f"  Logged in as {domain.identity_entity} #{pk_val}\n")
                return IdentityContext(user_id=pk_val), row
            print(f"  Enter 0-{len(rows)}")
        except (ValueError, KeyboardInterrupt):
            return None, None


def _print_banner(domain_name: str) -> None:
    print("\n" + "=" * 60)
    print(f"  {domain_name}")
    print("=" * 60)
    print("  Type /help for available commands")
    print("=" * 60 + "\n")


def main() -> None:
    try:
        config = load_config()
    except ConfigError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    domains_dir = config.domains_dir
    agent = None
    domain = None
    db_cfg = None

    # Auto-load domain from CLI arg or prompt
    available = list_domains(domains_dir)
    if not available:
        print(f"ERROR: No domains found in {domains_dir}")
        sys.exit(1)

    identity = None
    initial = sys.argv[1] if len(sys.argv) > 1 else None
    if initial and initial not in available:
        print(f"ERROR: Unknown domain '{initial}'. Available: {', '.join(available)}")
        sys.exit(1)

    if initial:
        domain = load_domain(initial, domains_dir)
    elif len(available) == 1:
        domain = load_domain(available[0], domains_dir)

    if domain:
        db_cfg = db_config_for_domain(config, domain)
        if not database_ready(db_cfg, domain.store):
            print(f"Database '{domain.store}' not found. Installing...")
            db_cfg = install_domain(config, domain)
        config.database = db_cfg
        identity, user_data = _pick_identity(db_cfg, domain)
        agent = AgentPipeline(config, domain, identity=identity)
        if user_data:
            agent.set_user_context(user_data)
        _print_banner(domain.name)
        print(f"Agent: Hello! I'm your {domain.name.lower()} assistant.\n"
              "       How can I help you today?\n")
    else:
        print("\n  No domain loaded. Use /list and /load <domain> to get started."
              "\n  Type /help for available commands.\n")

    while True:
        try:
            prompt = "You: " if agent else "> "
            user_input = input(prompt).strip()

            if not user_input:
                continue

            if user_input.lower() in {"/quit", "/exit"}:
                print("\nGoodbye!\n")
                break

            # ── /help ─────────────────────────────────────────────────
            if user_input.lower() == "/help":
                print("\n  Available commands:")
                print("    /list            List available domains")
                print("    /load <domain>   Switch to domain (installs if needed)")
                print("    /reset           Reinstall current domain (drop + recreate DB)")
                print("    /new             Clear conversation history")
                print("    /help            Show this help")
                print("    /quit            Exit\n")
                continue

            # ── /list ─────────────────────────────────────────────────
            if user_input.lower() == "/list":
                print("\nAvailable domains:")
                for name in list_domains(domains_dir):
                    d = load_domain(name, domains_dir)
                    active = " (active)" if domain and name == domain.dir_name else ""
                    print(f"  {name:12s}  {d.name}{active}")
                print()
                continue

            # ── /load <domain> ────────────────────────────────────────
            if user_input.lower().startswith("/load"):
                parts = user_input.split(maxsplit=1)
                if len(parts) < 2:
                    print("\nUsage: /load <domain_name>  (use /list to see available)\n")
                    continue

                target = parts[1].strip()
                if target not in list_domains(domains_dir):
                    print(f"\nUnknown domain '{target}'. Use /list to see available domains.\n")
                    continue

                domain = load_domain(target, domains_dir)
                db_cfg = db_config_for_domain(config, domain)

                if database_ready(db_cfg, domain.store):
                    print(f"\nSwitching to {domain.name}...")
                else:
                    print(f"\nDatabase '{domain.store}' not found. Installing {domain.name}...")
                    db_cfg = install_domain(config, domain)

                config.database = db_cfg
                identity, user_data = _pick_identity(db_cfg, domain)
                agent = AgentPipeline(config, domain, identity=identity)
                if user_data:
                    agent.set_user_context(user_data)
                _print_banner(domain.name)
                print(f"Agent: Hello! I'm your {domain.name.lower()} assistant.\n"
                      "       How can I help you today?\n")
                continue

            # ── /reset ────────────────────────────────────────────────
            if user_input.lower() == "/reset":
                if not domain:
                    print("\nNo domain loaded. Use /load <domain> first.\n")
                    continue

                print(f"\nReinstalling {domain.name}...")
                db_cfg = install_domain(config, domain)
                config.database = db_cfg
                identity, user_data = _pick_identity(db_cfg, domain)
                agent = AgentPipeline(config, domain, identity=identity)
                if user_data:
                    agent.set_user_context(user_data)
                _print_banner(domain.name)
                print(f"Agent: {domain.name} reinstalled. Fresh session ready.\n")
                continue

            # ── /new (clear conversation) ─────────────────────────────
            if user_input.lower() == "/new":
                if agent:
                    agent.reset()
                    print("\nAgent: Fresh session started. How can I help?\n")
                else:
                    print("\nNo domain loaded. Use /load <domain> first.\n")
                continue

            # ── Chat with agent ───────────────────────────────────────
            if not agent:
                print("\nNo domain loaded. Use /list and /load <domain> to get started.\n")
                continue

            print()
            try:
                response = agent.chat(user_input)
                print(f"\nAgent: {response}\n")
            except Exception as e:
                # Remove the last user message so the conversation stays consistent
                msgs = getattr(agent, 'messages', None) or getattr(agent.conversation, 'messages', [])
                if msgs and msgs[-1]["role"] == "user":
                    msgs.pop()
                hint = _error_hint(e)
                print(f"\nAgent: I'm sorry, something went wrong — {hint}\n"
                      f"       Please try again.\n")

        except KeyboardInterrupt:
            print("\n\nGoodbye!\n")
            break


if __name__ == "__main__":
    main()
