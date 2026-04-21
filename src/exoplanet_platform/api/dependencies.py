"""FastAPI dependency providers (sessions, repositories, clients, auth).

Route handlers should NEVER instantiate repos or clients directly - they should
declare them as `Depends(...)` arguments so that tests can override them.
"""

from __future__ import annotations

import os
from typing import Iterator

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from exoplanet_platform.logging_config import get_logger
from exoplanet_platform.storage.database import get_session
from exoplanet_platform.storage.repository import (
    LightCurveRepository,
    PlanetRepository,
    StarRepository,
    TransitSignalRepository,
)

logger = get_logger(__name__)


def get_db_session() -> Iterator[Session]:
    """Yield a SQLAlchemy session for the duration of a request."""
    with get_session() as session:
        yield session


def get_planet_repo(
    session: Session = Depends(get_db_session),
) -> PlanetRepository:
    """Provide a `PlanetRepository` bound to the request session."""
    return PlanetRepository(session)


def get_star_repo(
    session: Session = Depends(get_db_session),
) -> StarRepository:
    """Provide a `StarRepository` bound to the request session."""
    return StarRepository(session)


def get_light_curve_repo(
    session: Session = Depends(get_db_session),
) -> LightCurveRepository:
    """Provide a `LightCurveRepository` bound to the request session."""
    return LightCurveRepository(session)


def get_transit_signal_repo(
    session: Session = Depends(get_db_session),
) -> TransitSignalRepository:
    """Provide a `TransitSignalRepository` bound to the request session."""
    return TransitSignalRepository(session)


def get_nasa_client() -> object:
    """Provide a NASA Exoplanet Archive client.

    Imported lazily because the ingestion subpackage is still under active
    development alongside this one.
    """
    from exoplanet_platform.ingestion.nasa_exoplanet_archive import (
        NASAExoplanetArchiveClient,
    )

    return NASAExoplanetArchiveClient()


def get_mast_client() -> object:
    """Provide a MAST light-curve client (lazy import)."""
    from exoplanet_platform.ingestion.mast import MASTClient

    return MASTClient()


def get_gaia_client() -> object:
    """Provide a Gaia stellar-data client (lazy import)."""
    from exoplanet_platform.ingestion.gaia import GaiaClient

    return GaiaClient()


def get_transit_detector() -> object:
    """Provide a transit detection engine (lazy import)."""
    from exoplanet_platform.analysis.transit import TransitDetector

    return TransitDetector()


def get_habitability_analyzer() -> object:
    """Provide a habitability analyzer (lazy import)."""
    from exoplanet_platform.analysis.habitability import HabitabilityAnalyzer

    return HabitabilityAnalyzer()


def require_api_key(x_api_key: str | None = Header(None)) -> None:
    """Simple API-key auth.

    If the `EXOPLANET_API_KEY` env var is set, incoming requests must provide a
    matching `X-API-Key` header; otherwise any value (including none) is accepted
    so local development stays frictionless.
    """
    expected = os.environ.get("EXOPLANET_API_KEY")
    if expected is None:
        return
    if x_api_key != expected:
        logger.warning("api.auth.reject", reason="api_key_mismatch")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
