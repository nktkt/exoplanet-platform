"""Shared domain models (pydantic) used across ingestion, analysis, storage, API.

These are the canonical in-memory shapes. Storage models (SQLAlchemy) live in
`storage/models.py` and convert to/from these.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Catalog(StrEnum):
    """Source catalog of a record."""

    NASA_EXOPLANET_ARCHIVE = "nasa_exoplanet_archive"
    KEPLER = "kepler"
    TESS = "tess"
    K2 = "k2"
    JPL_HORIZONS = "jpl_horizons"
    GAIA = "gaia"


class DetectionMethod(StrEnum):
    TRANSIT = "transit"
    RADIAL_VELOCITY = "radial_velocity"
    IMAGING = "imaging"
    MICROLENSING = "microlensing"
    ASTROMETRY = "astrometry"
    TIMING = "timing"
    UNKNOWN = "unknown"


class Star(BaseModel):
    """Stellar host properties."""

    model_config = ConfigDict(frozen=True)

    identifier: str = Field(..., description="Primary catalog identifier, e.g. 'Kepler-10'")
    ra_deg: float | None = None
    dec_deg: float | None = None
    distance_pc: float | None = None
    effective_temperature_k: float | None = None
    radius_solar: float | None = None
    mass_solar: float | None = None
    luminosity_solar: float | None = None
    metallicity_dex: float | None = None
    spectral_type: str | None = None
    catalog: Catalog | None = None


class Planet(BaseModel):
    """Planetary properties (confirmed or candidate)."""

    model_config = ConfigDict(frozen=True)

    identifier: str = Field(..., description="Planet name, e.g. 'Kepler-10 b'")
    host_star: str = Field(..., description="Host star identifier")
    discovery_method: DetectionMethod = DetectionMethod.UNKNOWN
    discovery_year: int | None = None

    orbital_period_days: float | None = None
    semi_major_axis_au: float | None = None
    eccentricity: float | None = None
    inclination_deg: float | None = None

    radius_earth: float | None = None
    mass_earth: float | None = None
    density_g_cm3: float | None = None
    equilibrium_temperature_k: float | None = None
    insolation_flux_earth: float | None = None

    transit_epoch_bjd: float | None = None
    transit_duration_hours: float | None = None
    transit_depth_ppm: float | None = None

    catalog: Catalog | None = None
    last_updated: datetime | None = None


class LightCurve(BaseModel):
    """Time-series photometry."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    target: str
    mission: Catalog
    time_bjd: list[float]
    flux: list[float]
    flux_err: list[float] | None = None
    quarter: int | None = None
    sector: int | None = None
    cadence_minutes: float | None = None


class TransitSignal(BaseModel):
    """Output of transit search (BLS etc.)."""

    model_config = ConfigDict(frozen=True)

    period_days: float
    epoch_bjd: float
    duration_hours: float
    depth_ppm: float
    snr: float
    power: float
    method: str = "bls"


class HabitabilityAssessment(BaseModel):
    """Habitable zone evaluation for a planet."""

    model_config = ConfigDict(frozen=True)

    planet: str
    in_conservative_hz: bool
    in_optimistic_hz: bool
    hz_inner_au: float
    hz_outer_au: float
    earth_similarity_index: float | None = None
    equilibrium_temperature_k: float | None = None
    notes: str | None = None
