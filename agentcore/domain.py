"""Domain configuration loaded from <domains_dir>/<name>/domain.json."""

import json
from dataclasses import dataclass
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
        """Load and return the LLM-designed logical schema."""
        if not self.has_designed_schema:
            raise FileNotFoundError(
                "No designed schema found. Run: python scripts/design_schema.py"
            )
        return json.loads(self.schema_path.read_text(encoding="utf-8"))


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
    )


def list_domains(domains_dir: Path) -> list[str]:
    """Return available domain names found in the given directory."""
    return sorted(
        d.name for d in domains_dir.iterdir()
        if d.is_dir() and (d / "domain.json").exists()
    )
