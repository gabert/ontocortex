"""Domain configuration loaded from <domains_dir>/<name>/domain.json."""

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
    database_name: str
    ontology_path: Path
    rules_path: Path
    prompt_path: Path
    schema_path: Path | None = None
    seed_data_path: Path | None = None
    ontology_compact_path: Path | None = None
    schema_plan_path: Path | None = None

    @property
    def generated_dir(self) -> Path:
        return self.ontology_path.parent / "_generated"

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
        from agentcore.ontology import build_ontology_model
        return build_ontology_model(self.ontology_text, source_path=self.ontology_path)

    @property
    def ontology_compact_text(self) -> str | None:
        """Return compact ontology text, or None if not generated yet."""
        if self.ontology_compact_path and self.ontology_compact_path.exists():
            return self.ontology_compact_path.read_text(encoding="utf-8")
        return None

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

        from agentcore.reconciler import SCHEMA_VERSION
        version = data.get("schema_version")
        if version is not None and version != SCHEMA_VERSION:
            raise ValueError(
                f"schema.json at {self.schema_path} has schema_version={version!r} "
                f"but the runtime expects {SCHEMA_VERSION}. Regenerate with: "
                f"python scripts/build_schema.py {self.dir_name}"
            )
        return data


def load_domain(domain_name: str, domains_dir: Path) -> DomainConfig:
    """Load a domain configuration from <domains_dir>/<domain_name>/domain.json."""
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

    generated = manifest.get("generated", {})
    generated_dir = domain_dir / "_generated"
    schema_file = generated.get("schema")
    seed_file = generated.get("seed_data")
    compact_file = generated.get("ontology_compact")
    plan_file = generated.get("schema_plan")

    return DomainConfig(
        dir_name=domain_name,
        name=manifest["name"],
        description=manifest["description"],
        database_name=manifest["database_name"],
        ontology_path=domain_dir / manifest["ontology"],
        rules_path=domain_dir / manifest["business_rules"],
        prompt_path=domain_dir / manifest["system_prompt"],
        schema_path=generated_dir / schema_file if schema_file else None,
        seed_data_path=generated_dir / seed_file if seed_file else None,
        ontology_compact_path=generated_dir / compact_file if compact_file else None,
        schema_plan_path=generated_dir / plan_file if plan_file else None,
    )


def list_domains(domains_dir: Path) -> list[str]:
    """Return available domain names found in the given directory."""
    return sorted(
        d.name for d in domains_dir.iterdir()
        if d.is_dir() and (d / "domain.json").exists()
    )


def update_domain_manifest(domain: DomainConfig, **generated_keys: str) -> None:
    """Merge one or more keys into the 'generated' section of domain.json.

    Call after each pipeline step with just the key(s) produced by that step:
        update_domain_manifest(domain, schema_plan=plan_file)
        update_domain_manifest(domain, schema=schema_file)
        update_domain_manifest(domain, seed_data=seed_file)

    Written atomically: the new manifest lands in a sibling temp file
    and is then renamed into place with `os.replace`. An interrupt
    mid-write therefore leaves either the old manifest intact or the
    new one fully flushed — never a truncated file.
    """
    manifest_path = domain.ontology_path.parent / "domain.json"
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

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
