"""Analysis endpoints: transit search and habitability assessment."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from exoplanet_platform.api.dependencies import (
    get_habitability_analyzer,
    get_light_curve_repo,
    get_mast_client,
    get_planet_repo,
    get_star_repo,
    get_transit_detector,
    get_transit_signal_repo,
    require_api_key,
)
from exoplanet_platform.api.schemas import (
    AnalysisRequest,
    AnalysisResponse,
    HabitabilityRequest,
    HabitabilityResponse,
)
from exoplanet_platform.logging_config import get_logger
from exoplanet_platform.storage.repository import (
    LightCurveRepository,
    PlanetRepository,
    StarRepository,
    TransitSignalRepository,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post(
    "/transit-search",
    response_model=AnalysisResponse,
    status_code=200,
    summary="Run transit search on a target's light curve",
    dependencies=[Depends(require_api_key)],
)
def transit_search(
    body: AnalysisRequest,
    lc_repo: LightCurveRepository = Depends(get_light_curve_repo),
    signal_repo: TransitSignalRepository = Depends(get_transit_signal_repo),
    mast: Any = Depends(get_mast_client),
    detector: Any = Depends(get_transit_detector),
) -> AnalysisResponse:
    """Run a BLS-style transit search against a target's light curve.

    Reuses a cached light curve from storage when one exists for the target;
    otherwise downloads a fresh one via MAST, persists it, then runs the search.
    Any detected signals are saved against the target identifier before return.
    """
    cached = lc_repo.list_for_target(body.target)
    if cached:
        lc = lc_repo.load(int(cached[0]["id"]))
        logger.info("api.analysis.transit.cache_hit", target=body.target, id=cached[0]["id"])
    else:
        logger.info("api.analysis.transit.cache_miss", target=body.target)
        lc = mast.download(target=body.target, mission=body.mission)
        lc_repo.save(lc)

    signals = detector.search(
        lc,
        min_period_days=body.min_period_days,
        max_period_days=body.max_period_days,
    )
    for sig in signals:
        signal_repo.save(body.target, sig)
    return AnalysisResponse(target=body.target, signals=list(signals))


@router.post(
    "/habitability",
    response_model=HabitabilityResponse,
    status_code=200,
    summary="Assess a planet's habitable-zone status",
    dependencies=[Depends(require_api_key)],
)
def habitability(
    body: HabitabilityRequest,
    planet_repo: PlanetRepository = Depends(get_planet_repo),
    star_repo: StarRepository = Depends(get_star_repo),
    analyzer: Any = Depends(get_habitability_analyzer),
) -> HabitabilityResponse:
    """Compute the habitable-zone assessment for a stored planet."""
    planet = planet_repo.get(body.planet_identifier)
    star = star_repo.get(planet.host_star)
    assessment = analyzer.assess(planet=planet, star=star)
    return HabitabilityResponse(assessment=assessment)
