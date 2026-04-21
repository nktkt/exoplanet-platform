"""Light-curve endpoints: download via MAST and fetch persisted arrays."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from exoplanet_platform.api.dependencies import (
    get_light_curve_repo,
    get_mast_client,
    require_api_key,
)
from exoplanet_platform.api.schemas import (
    LightCurveDetailResponse,
    LightCurveDownloadRequest,
    LightCurveDownloadResponse,
)
from exoplanet_platform.logging_config import get_logger
from exoplanet_platform.storage.repository import LightCurveRepository

logger = get_logger(__name__)

# Max number of points returned inline for a detail request. Larger arrays
# should be streamed out via a dedicated bulk export endpoint (future work).
_MAX_INLINE_POINTS = 1000

router = APIRouter(prefix="/light-curves", tags=["light-curves"])


@router.post(
    "/download",
    response_model=LightCurveDownloadResponse,
    status_code=201,
    summary="Download and persist a light curve",
    dependencies=[Depends(require_api_key)],
)
def download_light_curve(
    body: LightCurveDownloadRequest,
    repo: LightCurveRepository = Depends(get_light_curve_repo),
    client: Any = Depends(get_mast_client),
) -> LightCurveDownloadResponse:
    """Download a light curve from MAST for the given target and persist it.

    Returns the new row id so the client can fetch truncated arrays via
    `GET /light-curves/{id}` afterwards.
    """
    logger.info(
        "api.light_curves.download",
        target=body.target,
        mission=body.mission,
        quarter=body.quarter,
        sector=body.sector,
    )
    lc = client.download(
        target=body.target,
        mission=body.mission,
        quarter=body.quarter,
        sector=body.sector,
    )
    new_id = repo.save(lc)
    return LightCurveDownloadResponse(id=new_id, target=lc.target, mission=lc.mission.value)


@router.get(
    "/{id}",
    response_model=LightCurveDetailResponse,
    status_code=200,
    summary="Fetch a light curve",
)
def get_light_curve(
    id: int,
    repo: LightCurveRepository = Depends(get_light_curve_repo),
) -> LightCurveDetailResponse:
    """Return light-curve metadata with the first 1000 samples inlined.

    `total_points` reports the full array length so clients can paginate or
    fetch the raw data through a dedicated export endpoint.
    """
    lc = repo.load(id)
    total = len(lc.time_bjd)
    time_slice = list(lc.time_bjd[:_MAX_INLINE_POINTS])
    flux_slice = list(lc.flux[:_MAX_INLINE_POINTS])
    err_slice = (
        list(lc.flux_err[:_MAX_INLINE_POINTS]) if lc.flux_err is not None else None
    )
    return LightCurveDetailResponse(
        id=id,
        target=lc.target,
        mission=lc.mission.value,
        quarter=lc.quarter,
        sector=lc.sector,
        cadence_minutes=lc.cadence_minutes,
        total_points=total,
        time_bjd=time_slice,
        flux=flux_slice,
        flux_err=err_slice,
    )
