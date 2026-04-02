"""Database access layer — database-agnostic via SQLAlchemy."""

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.pool import QueuePool

from agentcore.config import DatabaseConfig

QueryResult = list[dict[str, Any]] | dict[str, Any]

# Engine cache: one pooled engine per connection URL, shared across queries.
_engines: dict[str, Any] = {}


def _get_engine(db_config: DatabaseConfig):
    url = db_config.connection_url()
    if url not in _engines:
        _engines[url] = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,   # drop stale connections before use
        )
    return _engines[url]


def execute_query(
    db_config: DatabaseConfig,
    query: str,
    is_write: bool = False,
    params: dict | None = None,
) -> QueryResult:
    """Execute a SQL query and return results as a list of dicts (read) or a status dict (write)."""
    engine = _get_engine(db_config)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})

            if is_write:
                conn.commit()
                if result.returns_rows:
                    rows = [dict(row._mapping) for row in result.fetchall()]
                    return {"success": True, "rows_affected": len(rows), "returned_data": rows}
                return {"success": True, "rows_affected": result.rowcount}

            return [dict(row._mapping) for row in result.fetchall()]

    except IntegrityError as e:
        return {"error": "constraint_violation", "detail": str(e.orig)}
    except SQLAlchemyError as e:
        return {"error": str(e)}
