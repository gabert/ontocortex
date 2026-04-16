"""Tests for the mapping layer.

Verifies that:
  - build_schema_map_from_mapping produces correct SchemaMap
  - Column mapping is populated and physical_column() resolves correctly
  - fk_index is populated from relation definitions
  - The translator uses physical column names in SQL when column_map is set
  - Validation uses ontology names (unchanged)
  - Errors for bad mapping files are clear
  - generate_mapping_from_schema round-trips through build_schema_map_from_mapping
"""

from __future__ import annotations

import pytest

from agentcore.sif import SchemaMap, validate_operations
from agentcore.sif.mapping import (
    MappingError,
    build_schema_map_from_mapping,
    generate_mapping_from_schema,
)
from agentcore.sif_sql import translate
from agentcore.identity import OwnershipMap


# ── Ontology: Owner → Pet, with Species unrelated ──────────────────────────

_ONTOLOGY = {
    "classes": [
        {"iri": "v#Owner",   "local_name": "Owner",   "comment": "owners"},
        {"iri": "v#Pet",     "local_name": "Pet",      "comment": "pets"},
        {"iri": "v#Species", "local_name": "Species",  "comment": "species"},
    ],
    "object_properties": [
        {
            "iri": "v#ownsPet",
            "local_name": "ownsPet",
            "domain_iri": "v#Owner",
            "range_iri":  "v#Pet",
        },
        {
            "iri": "v#isSpecies",
            "local_name": "isSpecies",
            "domain_iri": "v#Pet",
            "range_iri":  "v#Species",
        },
    ],
    "datatype_properties": [
        {"iri": "v#firstName",  "local_name": "firstName",  "snake_name": "first_name",  "domain_iris": ["v#Owner"]},
        {"iri": "v#lastName",   "local_name": "lastName",   "snake_name": "last_name",   "domain_iris": ["v#Owner"]},
        {"iri": "v#email",      "local_name": "email",      "snake_name": "email",       "domain_iris": ["v#Owner"]},
        {"iri": "v#petName",    "local_name": "petName",    "snake_name": "pet_name",    "domain_iris": ["v#Pet"]},
        {"iri": "v#breed",      "local_name": "breed",      "snake_name": "breed",       "domain_iris": ["v#Pet"]},
        {"iri": "v#weightKg",   "local_name": "weightKg",   "snake_name": "weight_kg",   "domain_iris": ["v#Pet"]},
        {"iri": "v#speciesName","local_name": "speciesName","snake_name": "species_name","domain_iris": ["v#Species"]},
    ],
}

# Mapping that renames everything — ontology names ≠ physical names
_MAPPING = {
    "tables": {
        "Owner": {
            "table": "clients",
            "primary_key": "client_id",
            "columns": {
                "first_name": "fname",
                "last_name": "lname",
                "email": "email_addr",
            },
        },
        "Pet": {
            "table": "animals",
            "primary_key": "animal_id",
            "columns": {
                "pet_name": "name",
                "breed": "breed",         # same name
                "weight_kg": "weight",
            },
        },
        "Species": {
            "table": "animal_species",
            "primary_key": "sp_id",
            "columns": {
                "species_name": "sp_name",
            },
        },
    },
    "relations": {
        "ownsPet": {
            "type": "direct",
            "fk_table": "animals",
            "fk_column": "client_id",
            "ref_table": "clients",
            "ref_column": "client_id",
        },
        "isSpecies": {
            "type": "direct",
            "fk_table": "animals",
            "fk_column": "sp_id",
            "ref_table": "animal_species",
            "ref_column": "sp_id",
        },
    },
}


@pytest.fixture
def smap() -> SchemaMap:
    return build_schema_map_from_mapping(_ONTOLOGY, _MAPPING)


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


# ── SchemaMap construction ───────────────────────────────────────────────────

class TestMappingConstruction:
    def test_tables_populated(self, smap):
        assert set(smap.tables.keys()) == {"Owner", "Pet", "Species"}

    def test_table_physical_names(self, smap):
        assert smap.tables["Owner"].table_name == "clients"
        assert smap.tables["Pet"].table_name == "animals"
        assert smap.tables["Species"].table_name == "animal_species"

    def test_columns_are_ontology_names(self, smap):
        assert smap.tables["Owner"].columns == ["first_name", "last_name", "email"]
        assert smap.tables["Pet"].columns == ["pet_name", "breed", "weight_kg"]

    def test_column_map_populated(self, smap):
        owner = smap.tables["Owner"]
        assert owner.column_map == {
            "first_name": "fname",
            "last_name": "lname",
            "email": "email_addr",
        }

    def test_physical_column_resolves(self, smap):
        owner = smap.tables["Owner"]
        assert owner.physical_column("first_name") == "fname"
        assert owner.physical_column("last_name") == "lname"
        # Same-name columns resolve to themselves
        pet = smap.tables["Pet"]
        assert pet.physical_column("breed") == "breed"

    def test_physical_column_passthrough_for_unknown(self, smap):
        # Names not in column_map pass through (e.g. PK, FK columns)
        owner = smap.tables["Owner"]
        assert owner.physical_column("client_id") == "client_id"

    def test_relations_populated(self, smap):
        assert "ownsPet" in smap.relations
        rel = smap.relations["ownsPet"]
        assert rel.is_direct
        assert rel.fk_table == "animals"
        assert rel.fk_column == "client_id"

    def test_fk_index_populated(self, smap):
        # clients is referenced by animals via client_id
        assert ("animals", "client_id") in smap.fk_index["clients"]
        # animal_species is referenced by animals via sp_id
        assert ("animals", "sp_id") in smap.fk_index["animal_species"]

    def test_tables_by_iri(self, smap):
        assert smap.tables_by_iri["v#Owner"].class_name == "Owner"


# ── Translator uses physical column names ────────────────────────────────────

class TestTranslatorWithMapping:
    def test_query_uses_physical_columns(self, smap):
        op = {
            "op": "query",
            "entity": "Owner",
            "filters": {"first_name": "John"},
            "fields": ["first_name", "email"],
        }
        stmt = translate(op, smap)
        sql = _normalize(stmt.sql)
        # SELECT should use physical names
        assert "clients.fname" in sql
        assert "clients.email_addr" in sql
        # WHERE should use physical names
        assert "clients.fname = :p1" in sql

    def test_query_sort_uses_physical(self, smap):
        op = {
            "op": "query",
            "entity": "Pet",
            "sort": {"field": "pet_name", "dir": "asc"},
        }
        stmt = translate(op, smap)
        sql = _normalize(stmt.sql)
        assert "ORDER BY animals.name ASC" in sql

    def test_query_aggregate_uses_physical(self, smap):
        op = {
            "op": "query",
            "entity": "Pet",
            "aggregate": {"fn": "avg", "field": "weight_kg"},
        }
        stmt = translate(op, smap)
        sql = _normalize(stmt.sql)
        assert "avg(animals.weight)" in sql

    def test_query_relation_filter_uses_physical(self, smap):
        op = {
            "op": "query",
            "entity": "Pet",
            "relations": [
                {"rel": "ownsPet", "entity": "Owner",
                 "filters": {"last_name": "Smith"}},
            ],
        }
        stmt = translate(op, smap)
        sql = _normalize(stmt.sql)
        assert "clients.lname = :p1" in sql
        assert "JOIN" in sql

    def test_create_uses_physical_columns(self, smap):
        op = {
            "op": "create",
            "entity": "Owner",
            "data": {"first_name": "Jane", "email": "jane@x.org"},
        }
        stmt = translate(op, smap)
        sql = _normalize(stmt.sql)
        assert "INSERT INTO clients" in sql
        assert "fname" in sql
        assert "email_addr" in sql
        # Ontology names should NOT appear in the SQL
        assert "first_name" not in sql
        # (email_addr contains 'email' as substring, so skip that check)

    def test_create_resolve_uses_physical(self, smap):
        op = {
            "op": "create",
            "entity": "Pet",
            "data": {"pet_name": "Rex", "breed": "Labrador"},
            "resolve": {
                "isSpecies": {
                    "entity": "Species",
                    "filters": {"species_name": "Dog"},
                },
            },
        }
        stmt = translate(op, smap)
        sql = _normalize(stmt.sql)
        # The resolve subquery should use physical column name
        assert "sp_name = :p3" in sql

    def test_update_uses_physical_columns(self, smap):
        op = {
            "op": "update",
            "entity": "Owner",
            "data": {"email": "new@x.org"},
            "filters": {"first_name": "Jane"},
        }
        stmt = translate(op, smap)
        sql = _normalize(stmt.sql)
        assert "SET email_addr = :p1" in sql
        assert "WHERE fname = :p2" in sql

    def test_delete_uses_physical_columns(self, smap):
        op = {
            "op": "delete",
            "entity": "Owner",
            "filters": {"last_name": "Smith"},
        }
        stmt = translate(op, smap)
        sql = _normalize(stmt.sql)
        assert "WHERE lname = :p1" in sql


# ── Validation still uses ontology names ─────────────────────────────────────

class TestValidationWithMapping:
    def test_valid_op_passes(self, smap):
        op = {"op": "query", "entity": "Owner", "filters": {"first_name": "John"}}
        errors = validate_operations([op], smap)
        assert errors == []

    def test_physical_column_name_rejected(self, smap):
        """The LLM should NOT use physical names — validation catches them."""
        op = {"op": "query", "entity": "Owner", "filters": {"fname": "John"}}
        errors = validate_operations([op], smap)
        assert any("unknown filter field 'fname'" in e for e in errors)


# ── OwnershipMap works with mapping ──────────────────────────────────────────

class TestOwnershipWithMapping:
    def test_scoped_tables_from_mapping(self, smap):
        ownership = OwnershipMap("Owner", smap)
        # Owner's own table scoped by PK
        assert ownership.get_scope("clients").scope_column == "client_id"
        # Pet table scoped by FK to owner
        assert ownership.get_scope("animals").scope_column == "client_id"
        # Species not scoped (no FK to owner)
        assert ownership.get_scope("animal_species") is None


# ── Error cases ──────────────────────────────────────────────────────────────

class TestMappingErrors:
    def test_unknown_class_raises(self):
        bad = {"tables": {"Nonexistent": {"table": "t", "primary_key": "id", "columns": {}}}}
        with pytest.raises(MappingError, match="not in the ontology"):
            build_schema_map_from_mapping(_ONTOLOGY, bad)

    def test_missing_table_field_raises(self):
        bad = {"tables": {"Owner": {"primary_key": "id", "columns": {}}}}
        with pytest.raises(MappingError, match="must have 'table'"):
            build_schema_map_from_mapping(_ONTOLOGY, bad)

    def test_unknown_relation_raises(self):
        good_tables = _MAPPING["tables"]
        bad = {
            "tables": good_tables,
            "relations": {
                "nonexistent": {
                    "type": "direct",
                    "fk_table": "a", "fk_column": "b",
                    "ref_table": "c", "ref_column": "d",
                },
            },
        }
        with pytest.raises(MappingError, match="not an object property"):
            build_schema_map_from_mapping(_ONTOLOGY, bad)

    def test_bad_relation_type_raises(self):
        good_tables = _MAPPING["tables"]
        bad = {
            "tables": good_tables,
            "relations": {"ownsPet": {"type": "magic"}},
        }
        with pytest.raises(MappingError, match="unknown type"):
            build_schema_map_from_mapping(_ONTOLOGY, bad)


# ── generate_mapping_from_schema (auto path) ────────────────────────────────

# A schema.json-shaped fixture that mirrors the same ontology as _ONTOLOGY.
# Column names = ontology property names (identity mapping).
_SCHEMA_JSON = {
    "schema_version": 1,
    "tables": [
        {
            "name": "owners",
            "comment": "owners",
            "ontology_iri": "v#Owner",
            "primary_key": "owner_id",
            "columns": [
                {"name": "first_name", "type": "string"},
                {"name": "last_name", "type": "string"},
                {"name": "email", "type": "string"},
            ],
            "foreign_keys": [
                {
                    "column": "pet_id",
                    "references_table": "pets",
                    "references_column": "pet_id",
                },
            ],
        },
        {
            "name": "pets",
            "comment": "pets",
            "ontology_iri": "v#Pet",
            "primary_key": "pet_id",
            "columns": [
                {"name": "pet_name", "type": "string"},
                {"name": "breed", "type": "string"},
                {"name": "weight_kg", "type": "decimal"},
            ],
            "foreign_keys": [
                {
                    "column": "species_id",
                    "references_table": "species",
                    "references_column": "species_id",
                },
            ],
        },
        {
            "name": "species",
            "comment": "species",
            "ontology_iri": "v#Species",
            "primary_key": "species_id",
            "columns": [
                {"name": "species_name", "type": "string"},
            ],
            "foreign_keys": [],
        },
    ],
    "creation_order": ["species", "pets", "owners"],
}


class TestGenerateMapping:
    """Verify generate_mapping_from_schema produces a correct mapping dict."""

    def test_tables_present(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        assert set(m["tables"].keys()) == {"Owner", "Pet", "Species"}

    def test_table_physical_names(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        assert m["tables"]["Owner"]["table"] == "owners"
        assert m["tables"]["Pet"]["table"] == "pets"

    def test_table_iris(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        assert m["tables"]["Owner"]["iri"] == "v#Owner"
        assert m["tables"]["Pet"]["iri"] == "v#Pet"

    def test_primary_keys(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        assert m["tables"]["Owner"]["primary_key"] == "owner_id"
        assert m["tables"]["Species"]["primary_key"] == "species_id"

    def test_column_rich_format(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        cols = m["tables"]["Owner"]["columns"]
        assert cols["first_name"] == {"iri": "v#firstName", "column": "first_name"}
        assert cols["email"] == {"iri": "v#email", "column": "email"}

    def test_column_iris(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        cols = m["tables"]["Pet"]["columns"]
        assert cols["pet_name"]["iri"] == "v#petName"
        assert cols["breed"]["iri"] == "v#breed"

    def test_relations_discovered(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        assert "ownsPet" in m["relations"]
        assert "isSpecies" in m["relations"]

    def test_relation_iris(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        assert m["relations"]["ownsPet"]["iri"] == "v#ownsPet"
        assert m["relations"]["isSpecies"]["iri"] == "v#isSpecies"

    def test_direct_relation_structure(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        rel = m["relations"]["ownsPet"]
        assert rel["type"] == "direct"
        assert rel["fk_table"] == "owners"
        assert rel["fk_column"] == "pet_id"
        assert rel["ref_table"] == "pets"

    def test_lookup_tables_skipped(self):
        schema = {
            **_SCHEMA_JSON,
            "tables": _SCHEMA_JSON["tables"] + [{
                "name": "status_values",
                "ontology_iri": "v#StatusValue",
                "lookup_table": True,
                "primary_key": "code",
                "columns": [{"name": "label", "type": "string"}],
                "foreign_keys": [],
            }],
        }
        m = generate_mapping_from_schema(_ONTOLOGY, schema)
        assert "StatusValue" not in m["tables"]


class TestGenerateMappingRoundTrip:
    """Verify that generate → build_schema_map_from_mapping produces a
    SchemaMap equivalent to the one the auto path would build."""

    @pytest.fixture
    def smap(self):
        m = generate_mapping_from_schema(_ONTOLOGY, _SCHEMA_JSON)
        return build_schema_map_from_mapping(_ONTOLOGY, m)

    def test_tables_populated(self, smap):
        assert set(smap.tables.keys()) == {"Owner", "Pet", "Species"}

    def test_physical_table_names(self, smap):
        assert smap.tables["Owner"].table_name == "owners"
        assert smap.tables["Pet"].table_name == "pets"

    def test_columns_are_ontology_names(self, smap):
        assert smap.tables["Owner"].columns == ["first_name", "last_name", "email"]

    def test_physical_column_identity(self, smap):
        """Identity mapping: physical_column returns the same name."""
        owner = smap.tables["Owner"]
        assert owner.physical_column("first_name") == "first_name"

    def test_relations_populated(self, smap):
        assert "ownsPet" in smap.relations
        assert smap.relations["ownsPet"].is_direct

    def test_fk_index_populated(self, smap):
        assert ("owners", "pet_id") in smap.fk_index["pets"]

    def test_query_produces_correct_sql(self, smap):
        op = {
            "op": "query",
            "entity": "Owner",
            "filters": {"first_name": "John"},
            "fields": ["first_name", "email"],
        }
        stmt = translate(op, smap)
        sql = " ".join(stmt.sql.split())
        assert "owners.first_name" in sql
        assert "owners.email" in sql

    def test_validation_passes(self, smap):
        op = {"op": "query", "entity": "Owner", "filters": {"first_name": "John"}}
        errors = validate_operations([op], smap)
        assert errors == []

    def test_ownership_works(self, smap):
        ownership = OwnershipMap("Owner", smap)
        assert ownership.get_scope("owners").scope_column == "owner_id"
        # pets FK points TO owners → fk_index["owners"] has ("pets", ...)
        # But in _SCHEMA_JSON the FK is owners→pets, so pets is not scoped.
        # Ownership scoping of child tables is tested in TestOwnershipWithMapping.
