"""Planet catalog endpoints: list, fetch, and ingest from upstream catalogs."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from exoplanet_platform.api.dependencies import (
    get_nasa_client,
    get_planet_repo,
    require_api_key,
)
from exoplanet_platform.api.schemas import (
    IngestRequest,
    PlanetListResponse,
    PlanetResponse,
)
from exoplanet_platform.domain import Catalog
from exoplanet_platform.exceptions import ValidationError
from exoplanet_platform.logging_config import get_logger
from exoplanet_platform.storage.repository import PlanetRepository

logger = get_logger(__name__)

router = APIRouter(prefix="/planets", tags=["planets"])


@router.get(
    "",
    response_model=PlanetListResponse,
    status_code=200,
    summary="List planets",
)
def list_planets(
    host_star: str | None = Query(None, description="Filter by host star identifier"),
    catalog: Catalog | None = Query(None, description="Filter by source catalog"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    repo: PlanetRepository = Depends(get_planet_repo),
) -> PlanetListResponse:
    """Return a paginated list of stored planets with optional filters."""
    items = repo.list(host_star=host_star, catalog=catalog, limit=limit, offset=offset)
    total = repo.count(host_star=host_star, catalog=catalog)
    return PlanetListResponse(items=items, total=total)


@router.get(
    "/{identifier}",
    response_model=PlanetResponse,
    status_code=200,
    summary="Fetch one planet",
)
def get_planet(
    identifier: str,
    repo: PlanetRepository = Depends(get_planet_repo),
) -> PlanetResponse:
    """Return the stored planet record for `identifier`.

    Raises `DataSourceNotFoundError` (HTTP 404) if the planet is not present.
    """
    planet = repo.get(identifier)
    return PlanetResponse(planet=planet)


@router.post(
    "/ingest",
    response_model=PlanetResponse,
    status_code=201,
    summary="Ingest a planet from an upstream catalog",
    dependencies=[Depends(require_api_key)],
)
def ingest_planet(
    body: IngestRequest,
    repo: PlanetRepository = Depends(get_planet_repo),
    client: Any = Depends(get_nasa_client),
) -> PlanetResponse:
    """Fetch a planet from its upstream catalog, upsert it, and return it."""
    if body.catalog != Catalog.NASA_EXOPLANET_ARCHIVE:
        # Other catalogs will be wired up as their ingestion clients land.
        raise ValidationError(
            f"Ingestion for catalog '{body.catalog.value}' is not implemented yet"
        )

    logger.info("api.planets.ingest", identifier=body.identifier, catalog=body.catalog.value)
    planet = client.fetch_planet(body.identifier)
    repo.upsert(planet)
    # Re-read through the repository so we return the canonical stored shape.
    stored = repo.get(planet.identifier)
    return PlanetResponse(planet=stored)
