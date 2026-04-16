"""Shared pytest fixtures for planner tests.

`make_domain` builds a minimal DomainConfig from a TTL string or file in
tmp_path. That lets tests exercise design_plan end-to-end (including the
YAML write) without touching the real domains/ directory.
"""

import json
import shutil
from pathlib import Path

import pytest
import yaml

from agentcore.domain import DomainConfig, load_domain

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def pytest_addoption(parser):
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Rewrite golden files from current planner output.",
    )


@pytest.fixture
def make_domain(tmp_path):
    """Return a factory that materializes a fake domain under tmp_path.

    Usage:
        domain = make_domain("cycle", ttl_path=FIXTURES_DIR / "cycle.ttl")
        domain = make_domain("mydomain", ttl_text="...", overrides={...})
    """
    def _make(
        name: str,
        *,
        ttl_path: Path | None = None,
        ttl_text: str | None = None,
        overrides: dict | None = None,
    ) -> DomainConfig:
        if (ttl_path is None) == (ttl_text is None):
            raise ValueError("pass exactly one of ttl_path or ttl_text")

        domain_dir = tmp_path / name
        domain_dir.mkdir()

        ontology_file = f"{name}_ontology.ttl"
        if ttl_path is not None:
            shutil.copyfile(ttl_path, domain_dir / ontology_file)
        else:
            (domain_dir / ontology_file).write_text(ttl_text, encoding="utf-8")

        (domain_dir / f"{name}_rules.yaml").write_text("# stub\n", encoding="utf-8")
        (domain_dir / f"{name}_prompt.txt").write_text("stub\n", encoding="utf-8")

        source_dir = domain_dir / "data_sources" / "test"
        source_dir.mkdir(parents=True)

        manifest = {
            "name": name,
            "description": f"{name} test fixture",
            "ontology": ontology_file,
            "business_rules": f"{name}_rules.yaml",
            "system_prompt": f"{name}_prompt.txt",
            "data_sources": {
                "test": {
                    "store": f"{name}_test",
                    "source_dir": "data_sources/test",
                },
            },
        }
        (domain_dir / "domain.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )

        if overrides is not None:
            (source_dir / "overrides.yaml").write_text(
                yaml.safe_dump(overrides), encoding="utf-8"
            )

        return load_domain(name, tmp_path)

    return _make
