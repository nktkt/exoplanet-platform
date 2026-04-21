"""Pydantic request/response models for the HTTP API.

These are separate from the domain models in `exoplanet_platform.domain` so
that we can evolve the wire format independently of the internal representation.
Most responses simply wrap the domain model as the payload.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from exoplanet_platform.domain import (
    Catalog,
    HabitabilityAssessment,
    Planet,
    Star,
    TransitSignal,
)


class PlanetResponse(BaseModel):
    """Single-planet response wrapper."""

    model_config = ConfigDict(from_attributes=True)

    planet: Planet


class PlanetListResponse(BaseModel):
    """Paginated list of planets."""

    items: list[Planet]
    total: int


class StarResponse(BaseModel):
    """Single-star response wrapper."""

    model_config = ConfigDict(from_attributes=True)

    star: Star


class StarListResponse(BaseModel):
    """Paginated list of stars."""

    items: list[Star]
    total: int


class IngestRequest(BaseModel):
    """Request body for upstream catalog ingestion."""

    identifier: str = Field(..., description="Upstream identifier to fetch")
    catalog: Catalog = Field(..., description="Which catalog to query")


class AnalysisRequest(BaseModel):
    """Request body for a transit search run."""

    target: str = Field(..., description="Target identifier (e.g. Kepler-10)")
    mission: str = Field(..., description="Mission/catalog name for the light curve")
    min_period_days: float | None = None
    max_period_days: float | None = None


class AnalysisResponse(BaseModel):
    """Collection of transit signals produced for a target."""

    target: str
    signals: list[TransitSignal]


class HabitabilityRequest(BaseModel):
    """Request body for a habitability assessment."""

    planet_identifier: str = Field(..., description="Planet identifier to assess")


class HabitabilityResponse(BaseModel):
    """Habitability assessment wrapper."""

    assessment: HabitabilityAssessment


class LightCurveDownloadRequest(BaseModel):
    """Request body for downloading and persisting a light curve."""

    target: str
    mission: str
    quarter: int | None = None
    sector: int | None = None


class LightCurveDownloadResponse(BaseModel):
    """Response after a successful light curve download."""

    id: int
    target: str
    mission: str


class LightCurveDetailResponse(BaseModel):
    """Light curve detail with truncated arrays for efficient transport."""

    id: int
    target: str
    mission: str
    quarter: int | None = None
    sector: int | None = None
    cadence_minutes: float | None = None
    total_points: int
    time_bjd: list[float]
    flux: list[float]
    flux_err: list[float] | None = None


class ErrorResponse(BaseModel):
    """Standardized error envelope."""

    error: str
    detail: str | None = None
    type: str


class HealthResponse(BaseModel):
    """Service health check payload."""

    status: str
    version: str
    checks: dict[str, str]
