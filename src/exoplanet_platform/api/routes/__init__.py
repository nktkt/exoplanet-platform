"""Route aggregation for the FastAPI app.

Each sub-module exposes an `APIRouter` which is combined here and mounted by
`create_app`. Keeping a single combined router means main.py doesn't need to
know about every individual sub-module.
"""

from __future__ import annotations

from fastapi import APIRouter

from exoplanet_platform.api.routes import (
    analysis,
    health,
    light_curves,
    planets,
    stars,
)

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(planets.router)
api_router.include_router(stars.router)
api_router.include_router(light_curves.router)
api_router.include_router(analysis.router)

__all__ = ["api_router"]
