"""Domain database setup: create, schema, seed data."""

import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

from agentcore.config import AppConfig, DatabaseConfig
from agentcore.domain import DomainConfig
from agentcore.schema import create_tables, load_seed_sql


def db_config_for_domain(app_cfg: AppConfig, domain: DomainConfig) -> DatabaseConfig:
    """Build a DatabaseConfig that targets the domain's database."""
    return DatabaseConfig(
        dbname=domain.database_name,
        user=app_cfg.database.user,
        password=app_cfg.database.password,
        host=app_cfg.database.host,
        port=app_cfg.database.port,
    )


def database_ready(cfg: DatabaseConfig, dbname: str) -> bool:
    """Check whether a PostgreSQL database exists AND has tables (schema applied)."""
    conn = psycopg2.connect(
        dbname="postgres",
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            if cur.fetchone() is None:
                return False
    finally:
        conn.close()

    try:
        with psycopg2.connect(**{**cfg.as_dict(), "dbname": dbname}) as db_conn:
            with db_conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public'"
                )
                count = cur.fetchone()[0]
        return count > 0
    except Exception:
        return False


def drop_and_create(cfg: DatabaseConfig, dbname: str) -> None:
    """Terminate connections, drop and recreate a database."""
    print(f"Resetting database '{dbname}'...")
    conn = psycopg2.connect(
        dbname="postgres",
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        n = cur.rowcount
        if n:
            print(f"  Terminated {n} active connection(s).")

        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname)))
        print("  Dropped.")

        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        print("  Created.")
    conn.close()


def apply_schema(db_cfg: DatabaseConfig, domain: DomainConfig) -> None:
    """Create tables from the LLM-designed logical schema via SQLAlchemy."""
    if not domain.has_designed_schema:
        raise FileNotFoundError(
            f"No schema.json found for domain '{domain.dir_name}'.\n"
            "Run first: python scripts/design_schema.py " + domain.dir_name
        )

    print(f"\nApplying schema from schema.json...")
    schema = domain.schema_data
    engine = create_engine(db_cfg.connection_url())
    tables = create_tables(engine, schema)
    for name in tables:
        table_def = next(t for t in schema["tables"] if t["name"] == name)
        n_cols = len(table_def.get("columns", []))
        n_fks = len(table_def.get("foreign_keys", []))
        print(f"  {name:20s}  {n_cols} cols  {n_fks} FKs")
    engine.dispose()
    print("  Schema applied.")


def load_test_data(db_cfg: DatabaseConfig, domain: DomainConfig) -> None:
    """Load seed data from the generated SQL file."""
    if not domain.seed_data_path or not domain.seed_data_path.exists():
        raise FileNotFoundError(
            f"No seed_data.sql found for domain '{domain.dir_name}'.\n"
            "Run first: python scripts/design_schema.py " + domain.dir_name
        )

    seed_sql = domain.seed_data_path.read_text(encoding="utf-8")
    print(f"\nLoading seed data from {domain.seed_data_path.name}...")
    engine = create_engine(db_cfg.connection_url())
    n = load_seed_sql(engine, seed_sql)
    engine.dispose()
    print(f"  {n} records inserted.")


def install_domain(app_cfg: AppConfig, domain: DomainConfig) -> DatabaseConfig:
    """Full install: drop/create DB, apply schema, load test data. Returns the db config."""
    db_cfg = db_config_for_domain(app_cfg, domain)
    drop_and_create(db_cfg, domain.database_name)
    apply_schema(db_cfg, domain)
    load_test_data(db_cfg, domain)
    return db_cfg
