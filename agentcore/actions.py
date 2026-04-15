"""Domain action registry.

Actions are Python callables that implement domain-specific business logic
the translator cannot express in SIF — premium calculations, appointment
booking, PDF generation, etc. They are registered per-domain at pipeline
startup and invoked by the SIF executor when the model emits an
`op: "action"` operation.

This module is intentionally tiny and I/O-free: it's imported by both the
pure translator (to inject action name enums + validate action names) and
the executor (to dispatch). Keeping the registry here avoids a circular
dependency between translator and executor.
"""

from pathlib import Path

# Each action is a callable: (params: dict, db_config, schema_map) -> str
# Registered per-domain by the pipeline at startup.
ActionFn = None  # type alias placeholder — it's Callable[[dict, Any, SchemaMap], str]

_action_registry: dict[str, dict] = {}
# Structure: {"action_name": {"fn": callable, "description": "...", "params_schema": {...}}}


def register_action(
    name: str, fn, description: str = "", params_schema: dict | None = None,
) -> None:
    """Register a domain action by name."""
    _action_registry[name] = {
        "fn": fn,
        "description": description,
        "params_schema": params_schema or {},
    }


def clear_actions() -> None:
    """Clear all registered actions (used when switching domains)."""
    _action_registry.clear()


def get_registered_actions() -> dict[str, dict]:
    """Return the current action registry (for building LLM prompts)."""
    return dict(_action_registry)


def load_domain_actions(domain_dir) -> int:
    """Load actions from a domain's actions/ directory.

    Each .py file must define:
      DEFINITION = {"name": "...", "description": "...", "params_schema": {...}}
      def execute(params: dict, db_config, schema_map) -> str: ...

    Returns the number of actions loaded.
    """
    import importlib.util

    actions_dir = Path(domain_dir) / "actions"
    if not actions_dir.is_dir():
        return 0

    count = 0
    for path in sorted(actions_dir.glob("*.py")):
        if path.name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(path.stem, path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        defn = getattr(mod, "DEFINITION", None)
        fn = getattr(mod, "execute", None)
        if defn and fn:
            register_action(
                name=defn["name"],
                fn=fn,
                description=defn.get("description", ""),
                params_schema=defn.get("params_schema", {}),
            )
            count += 1

    return count
