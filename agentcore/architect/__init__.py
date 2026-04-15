"""Architect: offline schema-design pipeline.

This package holds the one-time pipeline that turns an ontology into a
production schema and (optionally) seed data. It runs from the CLI
scripts in `scripts/build_schema.py` and friends, not from the live
chat pipeline.

Stages:
  1. `planner`         — deterministic topology-only plan from the ontology
  2. `builder`         — per-module LLM calls that flesh out data columns
  3. `build_validation`— pre-write + merge-time guards on builder output
  4. `reconciler`      — deterministic merge of module builds into schema.json
  5. `seed_data`       — optional demo/dev seed-SQL generator
  6. `schema`          — schema.json → concrete DDL via SQLAlchemy
  7. `sql_text`        — small quote/paren-aware SQL text utilities

Submodules are imported directly: `from agentcore.architect.planner
import design_plan`. No re-exports are made here to keep the
dependency graph visible in the import lines.
"""
