"""Domain configuration loaded from <domains_dir>/<name>/domain.json.

This package also holds the ontology parser (`agentcore.domain.ontology`)
and the domain installer (`agentcore.domain.install`). The public
surface — DomainConfig, load_domain, list_domains, update_domain_manifest
— is re-exported here so callers keep writing `from agentcore.domain
import DomainConfig`.
"""

import json
import os
import tempfile
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path


@dataclass
class DomainConfig:
    dir_name: str
    name: str
    description: str
    data_source: str              # Selected data source identifier (e.g. "primary")
    store: str                    # Backend-specific locator (DB name, dir path, ...)
    ontology_path: Path
    rules_path: Path
    prompt_path: Path
    source_dir: Path              # Per-data-source directory (overrides, mapping, generated)
    schema_path: Path | None = None
    seed_data_path: Path | None = None
    ontology_compact_path: Path | None = None
    schema_plan_path: Path | None = None
    mapping_path: Path | None = None
    identity_entity: str | None = None  # Ontology class representing the user

    @property
    def domain_dir(self) -> Path:
        return self.ontology_path.parent

    @property
    def generated_dir(self) -> Path:
        return self.source_dir / "_generated"

    @property
    def overrides_path(self) -> Path:
        return self.source_dir / "overrides.yaml"

    @property
    def ontology_text(self) -> str:
        return self.ontology_path.read_text(encoding="utf-8")

    @property
    def rules_text(self) -> str:
        return self.rules_path.read_text(encoding="utf-8")

    @property
    def prompt_text(self) -> str:
        return self.prompt_path.read_text(encoding="utf-8")

    @cached_property
    def ontology_model(self) -> dict:
        """Structured ontology (classes, object properties, etc.) keyed by IRI.

        Cached after first build; the .ttl is only parsed once per
        DomainConfig instance. If the file changes on disk, create a new
        instance via load_domain().
        """
        from agentcore.domain.ontology import build_ontology_model
        return build_ontology_model(self.ontology_text, source_path=self.ontology_path)

    @property
    def ontology_compact_text(self) -> str | None:
        """Return compact ontology text, or None if not generated yet."""
        if self.ontology_compact_path and self.ontology_compact_path.exists():
            return self.ontology_compact_path.read_text(encoding="utf-8")
        return None

    @property
    def has_mapping(self) -> bool:
        """Whether a mapping file is available."""
        return self.mapping_path is not None and self.mapping_path.exists()

    @property
    def mapping_data(self) -> dict:
        """Load and return the mapping file."""
        if not self.has_mapping:
            raise FileNotFoundError("No mapping file found.")
        import yaml
        return yaml.safe_load(self.mapping_path.read_text(encoding="utf-8"))

    @property
    def has_designed_schema(self) -> bool:
        """Whether an LLM-designed logical schema is available."""
        return self.schema_path is not None and self.schema_path.exists()

    @property
    def schema_data(self) -> dict:
        """Load and return the LLM-designed logical schema.

        Enforces `SCHEMA_VERSION` so a stale schema.json on disk (left
        over from a previous pipeline version) fails fast with a clear
        rebuild instruction instead of misparsing silently downstream.
        """
        if not self.has_designed_schema:
            raise FileNotFoundError(
                "No designed schema found. Run: python scripts/build_schema.py"
            )
        data = json.loads(self.schema_path.read_text(encoding="utf-8"))

        from agentcore.architect.reconciler import SCHEMA_VERSION
        version = data.get("schema_version")
        if version is not None and version != SCHEMA_VERSION:
            raise ValueError(
                f"schema.json at {self.schema_path} has schema_version={version!r} "
                f"but the runtime expects {SCHEMA_VERSION}. Regenerate with: "
                f"python scripts/build_schema.py {self.dir_name}"
            )
        return data


def load_domain(
    domain_name: str, domains_dir: Path, *, data_source: str | None = None,
) -> DomainConfig:
    """Load a domain configuration from <domains_dir>/<domain_name>/domain.json.

    ``data_source`` selects which named data source to activate. When
    omitted the first entry in ``data_sources`` is used.  If the manifest
    has no ``data_sources`` key, falls back to legacy ``database_name`` +
    ``mapping`` top-level keys (backward compatibility).
    """
    domain_dir = domains_dir / domain_name
    manifest_path = domain_dir / "domain.json"

    if not manifest_path.exists():
        available = [d.name for d in domains_dir.iterdir() if d.is_dir()]
        raise FileNotFoundError(
            f"Domain '{domain_name}' not found at {manifest_path}\n"
            f"Available domains: {', '.join(sorted(available))}"
        )

    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    # ── Resolve data source ──────────────────────────────────────────
    data_sources = manifest.get("data_sources")
    if data_sources:
        if data_source is None:
            ds_name = next(iter(data_sources))
        else:
            ds_name = data_source
        if ds_name not in data_sources:
            raise ValueError(
                f"Data source '{ds_name}' not found in domain '{domain_name}'. "
                f"Available: {', '.join(sorted(data_sources))}"
            )
        ds = data_sources[ds_name]
        store = ds["store"]
        source_dir_rel = ds.get("source_dir", f"data_sources/{ds_name}")
        mapping_file = ds.get("mapping", "mapping.yaml")
    else:
        # Legacy format: top-level database_name + mapping
        ds_name = manifest.get("database_name", domain_name)
        store = manifest["database_name"]
        source_dir_rel = f"data_sources/{ds_name}"
        mapping_file = "mapping.yaml"

    source_dir = domain_dir / source_dir_rel
    generated_dir = source_dir / "_generated"
    generated = manifest.get("generated", {})
    schema_file = generated.get("schema")
    seed_file = generated.get("seed_data")
    compact_file = generated.get("ontology_compact")
    plan_file = generated.get("schema_plan")

    return DomainConfig(
        dir_name=domain_name,
        name=manifest["name"],
        description=manifest["description"],
        data_source=ds_name,
        store=store,
        ontology_path=domain_dir / manifest["ontology"],
        rules_path=domain_dir / manifest["business_rules"],
        prompt_path=domain_dir / manifest["system_prompt"],
        source_dir=source_dir,
        schema_path=generated_dir / schema_file if schema_file else None,
        seed_data_path=generated_dir / seed_file if seed_file else None,
        ontology_compact_path=generated_dir / compact_file if compact_file else None,
        schema_plan_path=generated_dir / plan_file if plan_file else None,
        mapping_path=source_dir / mapping_file if mapping_file else None,
        identity_entity=manifest.get("identity_entity"),
    )


def list_domains(domains_dir: Path) -> list[str]:
    """Return available domain names found in the given directory."""
    return sorted(
        d.name for d in domains_dir.iterdir()
        if d.is_dir() and (d / "domain.json").exists()
    )


def update_domain_manifest(
    domain: DomainConfig,
    *,
    data_source_entry: tuple[str, dict] | None = None,
    **generated_keys: str,
) -> None:
    """Merge keys into domain.json (generated section + optional data source).

    Call after each pipeline step with just the key(s) produced by that step:
        update_domain_manifest(domain, schema_plan=plan_file)
        update_domain_manifest(domain, schema=schema_file)
        update_domain_manifest(
            domain,
            data_source_entry=("primary", {"store": "mydb", ...}),
        )

    The ``data_source_entry`` parameter is a ``(name, definition)`` tuple
    that is merged into the ``"data_sources"`` dict. All other keyword
    arguments are merged into the ``"generated"`` section.

    Written atomically: the new manifest lands in a sibling temp file
    and is then renamed into place with `os.replace`. An interrupt
    mid-write therefore leaves either the old manifest intact or the
    new one fully flushed — never a truncated file.
    """
    manifest_path = domain.domain_dir / "domain.json"
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    if data_source_entry is not None:
        ds_name, ds_def = data_source_entry
        manifest.setdefault("data_sources", {})[ds_name] = ds_def
    if generated_keys:
        manifest.setdefault("generated", {}).update(generated_keys)

    # Write to a tempfile in the same directory so `os.replace` is
    # guaranteed atomic (same filesystem) on both POSIX and Windows.
    fd, tmp_path = tempfile.mkstemp(
        prefix=".domain.",
        suffix=".json.tmp",
        dir=manifest_path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, manifest_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
