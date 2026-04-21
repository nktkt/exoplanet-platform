"""Star catalog endpoints: list, fetch, and ingest from upstream catalogs."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from exoplanet_platform.api.dependencies import (
    get_gaia_client,
    get_star_repo,
    require_api_key,
)
from exoplanet_platform.api.schemas import (
    IngestRequest,
    StarListResponse,
    StarResponse,
)
from exoplanet_platform.domain import Catalog
from exoplanet_platform.exceptions import ValidationError
from exoplanet_platform.logging_config import get_logger
from exoplanet_platform.storage.repository import StarRepository

logger = get_logger(__name__)

router = APIRouter(prefix="/stars", tags=["stars"])


@router.get(
    "",
    response_model=StarListResponse,
    status_code=200,
    summary="List stars",
)
def list_stars(
    catalog: Catalog | None = Query(None, description="Filter by source catalog"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    repo: StarRepository = Depends(get_star_repo),
) -> StarListResponse:
    """Return a paginated list of stored stars with an optional catalog filter."""
    items = repo.list(catalog=catalog, limit=limit, offset=offset)
    return StarListResponse(items=items, total=len(items))


@router.get(
    "/{identifier}",
    response_model=StarResponse,
    status_code=200,
    summary="Fetch one star",
)
def get_star(
    identifier: str,
    repo: StarRepository = Depends(get_star_repo),
) -> StarResponse:
    """Return the stored star record for `identifier`.

    Raises `DataSourceNotFoundError` (HTTP 404) if the star is not present.
    """
    star = repo.get(identifier)
    return StarResponse(star=star)


@router.post(
    "/ingest",
    response_model=StarResponse,
    status_code=201,
    summary="Ingest a star from an upstream catalog",
    dependencies=[Depends(require_api_key)],
)
def ingest_star(
    body: IngestRequest,
    repo: StarRepository = Depends(get_star_repo),
    client: Any = Depends(get_gaia_client),
) -> StarResponse:
    """Fetch a star from its upstream catalog, upsert it, and return it."""
    if body.catalog != Catalog.GAIA:
        raise ValidationError(
            f"Star ingestion for catalog '{body.catalog.value}' is not implemented yet"
        )

    logger.info("api.stars.ingest", identifier=body.identifier, catalog=body.catalog.value)
    star = client.fetch_star(body.identifier)
    repo.upsert(star)
    stored = repo.get(star.identifier)
    return StarResponse(star=stored)
