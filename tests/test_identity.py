"""Tests for identity injection — OwnershipMap + SQL scoping.

Verifies that:
  - OwnershipMap correctly derives scoped tables from SchemaMap
  - apply_identity produces correct SQL for each op type
  - Unscoped tables pass through unchanged
  - scope_link_plan augments endpoint filters
"""

from __future__ import annotations

import pytest

from agentcore.identity import IdentityContext, OwnershipMap
from agentcore.sif import SchemaMap, LinkPlan
from agentcore.sif.schema_map import TableMap
from agentcore.sif_sql.identity import apply_identity, scope_link_plan
from agentcore.sif_sql.translator import SQLStatement


# ── Minimal fixture: Owner → Pet (direct FK), Species (no FK to Owner) ──────

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
    ],
}

_SCHEMA = {
    "tables": [
        {
            "name": "vet_owners",
            "primary_key": "vet_owner_id",
            "comment": "owners",
            "columns": [{"name": "first_name"}, {"name": "last_name"}],
            "foreign_keys": [],
        },
        {
            "name": "vet_pets",
            "primary_key": "vet_pet_id",
            "comment": "pets",
            "columns": [{"name": "name"}, {"name": "breed"}],
            "foreign_keys": [
                {
                    "column": "vet_owner_id",
                    "references_table": "vet_owners",
                    "references_column": "vet_owner_id",
                },
            ],
        },
        {
            "name": "vet_species",
            "primary_key": "vet_species_id",
            "comment": "species",
            "columns": [{"name": "species_name"}],
            "foreign_keys": [],
        },
    ],
}


@pytest.fixture
def smap() -> SchemaMap:
    return SchemaMap(_ONTOLOGY, _SCHEMA)


@pytest.fixture
def ownership(smap) -> OwnershipMap:
    return OwnershipMap("Owner", smap)


@pytest.fixture
def identity() -> IdentityContext:
    return IdentityContext(user_id=42)


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


# ── OwnershipMap construction ────────────────────────────────────────────────

class TestOwnershipMap:
    def test_user_table_scoped_by_pk(self, ownership):
        scope = ownership.get_scope("vet_owners")
        assert scope is not None
        assert scope.scope_column == "vet_owner_id"

    def test_child_table_scoped_by_fk(self, ownership):
        scope = ownership.get_scope("vet_pets")
        assert scope is not None
        assert scope.scope_column == "vet_owner_id"

    def test_unrelated_table_not_scoped(self, ownership):
        assert ownership.get_scope("vet_species") is None

    def test_unknown_identity_entity_raises(self, smap):
        with pytest.raises(ValueError, match="not found in SchemaMap"):
            OwnershipMap("Nonexistent", smap)


# ── apply_identity: query ────────────────────────────────────────────────────

class TestQueryScoping:
    def test_query_with_existing_where(self, identity, ownership):
        stmt = SQLStatement(
            sql="SELECT vet_pets.* FROM vet_pets WHERE vet_pets.breed = :p1",
            params={"p1": "Labrador"},
            is_write=False,
            table_name="vet_pets",
            op_type="query",
        )
        result = apply_identity(stmt, identity, ownership)
        assert _normalize(result.sql) == _normalize(
            "SELECT vet_pets.* FROM vet_pets "
            "WHERE vet_pets.breed = :p1 AND vet_pets.vet_owner_id = :_identity_id"
        )
        assert result.params["_identity_id"] == 42
        assert result.params["p1"] == "Labrador"

    def test_query_without_where(self, identity, ownership):
        stmt = SQLStatement(
            sql="SELECT vet_pets.* FROM vet_pets",
            params={},
            is_write=False,
            table_name="vet_pets",
            op_type="query",
        )
        result = apply_identity(stmt, identity, ownership)
        assert "WHERE vet_pets.vet_owner_id = :_identity_id" in result.sql

    def test_query_with_order_by(self, identity, ownership):
        stmt = SQLStatement(
            sql="SELECT vet_pets.* FROM vet_pets ORDER BY vet_pets.name ASC",
            params={},
            is_write=False,
            table_name="vet_pets",
            op_type="query",
        )
        result = apply_identity(stmt, identity, ownership)
        sql = _normalize(result.sql)
        assert "WHERE vet_pets.vet_owner_id = :_identity_id ORDER BY" in sql

    def test_query_with_where_and_order_by(self, identity, ownership):
        stmt = SQLStatement(
            sql="SELECT vet_pets.* FROM vet_pets WHERE vet_pets.breed = :p1 ORDER BY vet_pets.name ASC",
            params={"p1": "Labrador"},
            is_write=False,
            table_name="vet_pets",
            op_type="query",
        )
        result = apply_identity(stmt, identity, ownership)
        sql = _normalize(result.sql)
        assert "AND vet_pets.vet_owner_id = :_identity_id ORDER BY" in sql

    def test_query_with_limit(self, identity, ownership):
        stmt = SQLStatement(
            sql="SELECT vet_pets.* FROM vet_pets LIMIT :p1",
            params={"p1": 10},
            is_write=False,
            table_name="vet_pets",
            op_type="query",
        )
        result = apply_identity(stmt, identity, ownership)
        sql = _normalize(result.sql)
        assert "WHERE vet_pets.vet_owner_id = :_identity_id LIMIT" in sql

    def test_query_user_table_scoped_by_pk(self, identity, ownership):
        stmt = SQLStatement(
            sql="SELECT vet_owners.* FROM vet_owners",
            params={},
            is_write=False,
            table_name="vet_owners",
            op_type="query",
        )
        result = apply_identity(stmt, identity, ownership)
        assert "WHERE vet_owners.vet_owner_id = :_identity_id" in result.sql

    def test_query_unscoped_passthrough(self, identity, ownership):
        stmt = SQLStatement(
            sql="SELECT vet_species.* FROM vet_species",
            params={},
            is_write=False,
            table_name="vet_species",
            op_type="query",
        )
        result = apply_identity(stmt, identity, ownership)
        assert result is stmt  # exact same object, untouched


# ── apply_identity: create ───────────────────────────────────────────────────

class TestCreateScoping:
    def test_create_injects_owner_column(self, identity, ownership):
        stmt = SQLStatement(
            sql="INSERT INTO vet_pets (name, breed) VALUES (:p1, :p2) RETURNING *",
            params={"p1": "Rex", "p2": "Labrador"},
            is_write=True,
            table_name="vet_pets",
            op_type="create",
        )
        result = apply_identity(stmt, identity, ownership)
        sql = _normalize(result.sql)
        assert "vet_owner_id) VALUES" in sql
        assert ":_identity_id) RETURNING" in sql
        assert result.params["_identity_id"] == 42

    def test_create_with_resolve_replaces_existing_scope_column(self, identity, ownership):
        """When resolve already included the scope column as a subselect,
        identity injection should replace it instead of adding a duplicate."""
        stmt = SQLStatement(
            sql=(
                "INSERT INTO vet_pets (name, breed, vet_owner_id) "
                "VALUES (:p1, :p2, (SELECT vet_owner_id FROM vet_owners WHERE first_name = :p3)) "
                "RETURNING *"
            ),
            params={"p1": "Rex", "p2": "Labrador", "p3": "John"},
            is_write=True,
            table_name="vet_pets",
            op_type="create",
        )
        result = apply_identity(stmt, identity, ownership)
        sql = _normalize(result.sql)
        # Column should appear exactly once
        assert sql.count("vet_owner_id") == 1
        # The value should be the identity param, not the subselect
        assert ":_identity_id" in sql
        assert "(SELECT" not in sql
        assert result.params["_identity_id"] == 42

    def test_create_unscoped_passthrough(self, identity, ownership):
        stmt = SQLStatement(
            sql="INSERT INTO vet_species (species_name) VALUES (:p1) RETURNING *",
            params={"p1": "Dog"},
            is_write=True,
            table_name="vet_species",
            op_type="create",
        )
        result = apply_identity(stmt, identity, ownership)
        assert result is stmt


# ── apply_identity: update ───────────────────────────────────────────────────

class TestUpdateScoping:
    def test_update_with_where(self, identity, ownership):
        stmt = SQLStatement(
            sql="UPDATE vet_pets SET name = :p1 WHERE vet_pet_id = :p2 RETURNING *",
            params={"p1": "Rex", "p2": 7},
            is_write=True,
            table_name="vet_pets",
            op_type="update",
        )
        result = apply_identity(stmt, identity, ownership)
        sql = _normalize(result.sql)
        assert "AND vet_owner_id = :_identity_id RETURNING" in sql

    def test_update_without_where(self, identity, ownership):
        stmt = SQLStatement(
            sql="UPDATE vet_pets SET name = :p1 RETURNING *",
            params={"p1": "Rex"},
            is_write=True,
            table_name="vet_pets",
            op_type="update",
        )
        result = apply_identity(stmt, identity, ownership)
        assert "WHERE vet_owner_id = :_identity_id RETURNING" in result.sql


# ── apply_identity: delete ───────────────────────────────────────────────────

class TestDeleteScoping:
    def test_delete_with_where(self, identity, ownership):
        stmt = SQLStatement(
            sql="DELETE FROM vet_pets WHERE vet_pet_id = :p1 RETURNING *",
            params={"p1": 7},
            is_write=True,
            table_name="vet_pets",
            op_type="delete",
        )
        result = apply_identity(stmt, identity, ownership)
        sql = _normalize(result.sql)
        assert "AND vet_owner_id = :_identity_id RETURNING" in sql

    def test_delete_without_where(self, identity, ownership):
        stmt = SQLStatement(
            sql="DELETE FROM vet_pets RETURNING *",
            params={},
            is_write=True,
            table_name="vet_pets",
            op_type="delete",
        )
        result = apply_identity(stmt, identity, ownership)
        assert "WHERE vet_owner_id = :_identity_id RETURNING" in result.sql


# ── scope_link_plan ──────────────────────────────────────────────────────────

class TestLinkPlanScoping:
    def test_scoped_endpoints_get_identity_filter(self, smap, identity, ownership):
        owner_table = smap.tables["Owner"]
        pet_table = smap.tables["Pet"]

        plan = LinkPlan(
            op="link",
            relation_name="ownsPet",
            from_table=owner_table,
            from_filters={"first_name": "John"},
            to_table=pet_table,
            to_filters={"name": "Rex"},
            junction_table="",
            from_fk_column="",
            to_fk_column="",
        )
        result = scope_link_plan(plan, identity, ownership)
        assert result.from_filters["vet_owner_id"] == 42
        assert result.to_filters["vet_owner_id"] == 42
        # Original filters preserved
        assert result.from_filters["first_name"] == "John"
        assert result.to_filters["name"] == "Rex"

    def test_unscoped_endpoint_not_augmented(self, smap, identity, ownership):
        owner_table = smap.tables["Owner"]
        species_table = smap.tables["Species"]

        plan = LinkPlan(
            op="link",
            relation_name="test",
            from_table=owner_table,
            from_filters={"first_name": "John"},
            to_table=species_table,
            to_filters={"species_name": "Dog"},
            junction_table="",
            from_fk_column="",
            to_fk_column="",
        )
        result = scope_link_plan(plan, identity, ownership)
        assert "vet_owner_id" in result.from_filters  # owner is scoped
        assert "vet_owner_id" not in result.to_filters  # species is not scoped
