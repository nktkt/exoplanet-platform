"""Database engine, session factory, and lifecycle helpers.

The engine and sessionmaker are lazily created from application settings so
that tests can reset them between cases (`reset_engine`). Callers should
obtain a `Session` through the `get_session()` context manager, which handles
commit / rollback / close bookkeeping.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from exoplanet_platform.config import get_settings
from exoplanet_platform.exceptions import StorageError
from exoplanet_platform.logging_config import get_logger
from exoplanet_platform.storage.models import Base

logger = get_logger(__name__)

_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def _build_engine() -> Engine:
    """Construct a SQLAlchemy engine from current settings."""
    settings = get_settings()
    db_url = settings.storage.database_url

    # SQLite in-process doesn't support pool_size / max_overflow kwargs.
    engine_kwargs: dict[str, object] = {"echo": settings.storage.echo_sql, "future": True}
    if not db_url.startswith("sqlite"):
        engine_kwargs["pool_size"] = settings.storage.pool_size
        engine_kwargs["max_overflow"] = settings.storage.max_overflow

    logger.info("storage.engine.create", database_url=db_url)
    return create_engine(db_url, **engine_kwargs)


def get_engine() -> Engine:
    """Return the process-wide SQLAlchemy engine, creating it if needed."""
    global _engine
    if _engine is None:
        _engine = _build_engine()
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    """Return the process-wide session factory bound to the engine."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            future=True,
        )
    return _SessionLocal


def init_db() -> None:
    """Create all tables defined on `Base.metadata`.

    Suitable for development and tests. Production deployments should use
    Alembic migrations instead.
    """
    engine = get_engine()
    try:
        Base.metadata.create_all(bind=engine)
    except SQLAlchemyError as exc:  # pragma: no cover - defensive
        raise StorageError(f"Failed to initialize database schema: {exc}") from exc
    logger.info("storage.init_db.complete")


@contextmanager
def get_session() -> Iterator[Session]:
    """Yield a SQLAlchemy session with commit/rollback/close semantics."""
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        raise StorageError(f"Database operation failed: {exc}") from exc
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def reset_engine() -> None:
    """Dispose of the current engine and session factory (test helper)."""
    global _engine, _SessionLocal
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionLocal = None
