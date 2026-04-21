"""Health-check endpoint used by load balancers and uptime monitors."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from exoplanet_platform import __version__
from exoplanet_platform.api.dependencies import get_db_session
from exoplanet_platform.api.schemas import HealthResponse
from exoplanet_platform.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    status_code=200,
    summary="Service health check",
)
def health(session: Session = Depends(get_db_session)) -> HealthResponse:
    """Return service status plus per-dependency checks.

    Always returns 200; callers should inspect the `checks` map to discover
    which subsystems are unhealthy.
    """
    checks: dict[str, str] = {}
    try:
        session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        logger.warning("api.health.db_check_failed", error=str(exc))
        checks["database"] = f"error: {exc.__class__.__name__}"

    status = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    return HealthResponse(status=status, version=__version__, checks=checks)
