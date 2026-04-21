"""SQLAlchemy 2.0 ORM models backing the platform's persistent state.

These objects are deliberately kept thin: the canonical domain types live in
`exoplanet_platform.domain`. Conversion helpers are provided in
`exoplanet_platform.storage.repository`.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models in the platform."""


class PlanetORM(Base):
    """Persistent representation of a `Planet` domain object."""

    __tablename__ = "planets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    identifier: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False
    )
    host_star: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    discovery_method: Mapped[str] = mapped_column(String(64), nullable=False, default="unknown")
    discovery_year: Mapped[int | None] = mapped_column(Integer, nullable=True)

    orbital_period_days: Mapped[float | None] = mapped_column(Float, nullable=True)
    semi_major_axis_au: Mapped[float | None] = mapped_column(Float, nullable=True)
    eccentricity: Mapped[float | None] = mapped_column(Float, nullable=True)
    inclination_deg: Mapped[float | None] = mapped_column(Float, nullable=True)

    radius_earth: Mapped[float | None] = mapped_column(Float, nullable=True)
    mass_earth: Mapped[float | None] = mapped_column(Float, nullable=True)
    density_g_cm3: Mapped[float | None] = mapped_column(Float, nullable=True)
    equilibrium_temperature_k: Mapped[float | None] = mapped_column(Float, nullable=True)
    insolation_flux_earth: Mapped[float | None] = mapped_column(Float, nullable=True)

    transit_epoch_bjd: Mapped[float | None] = mapped_column(Float, nullable=True)
    transit_duration_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    transit_depth_ppm: Mapped[float | None] = mapped_column(Float, nullable=True)

    catalog: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_updated: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class StarORM(Base):
    """Persistent representation of a `Star` domain object."""

    __tablename__ = "stars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    identifier: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False
    )

    ra_deg: Mapped[float | None] = mapped_column(Float, nullable=True)
    dec_deg: Mapped[float | None] = mapped_column(Float, nullable=True)
    distance_pc: Mapped[float | None] = mapped_column(Float, nullable=True)
    effective_temperature_k: Mapped[float | None] = mapped_column(Float, nullable=True)
    radius_solar: Mapped[float | None] = mapped_column(Float, nullable=True)
    mass_solar: Mapped[float | None] = mapped_column(Float, nullable=True)
    luminosity_solar: Mapped[float | None] = mapped_column(Float, nullable=True)
    metallicity_dex: Mapped[float | None] = mapped_column(Float, nullable=True)
    spectral_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    catalog: Mapped[str | None] = mapped_column(String(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class LightCurveORM(Base):
    """Persistent representation of a `LightCurve` domain object.

    Large time-series arrays are stored as JSON-encoded strings in Text columns to
    keep the schema portable across SQLite and PostgreSQL without pulling in
    dialect-specific JSON types.
    """

    __tablename__ = "light_curves"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    target: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    mission: Mapped[str] = mapped_column(String(64), nullable=False)
    quarter: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sector: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cadence_minutes: Mapped[float | None] = mapped_column(Float, nullable=True)

    time_bjd_json: Mapped[str] = mapped_column(Text, nullable=False)
    flux_json: Mapped[str] = mapped_column(Text, nullable=False)
    flux_err_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class TransitSignalORM(Base):
    """Persistent representation of a `TransitSignal` linked to a planet identifier."""

    __tablename__ = "transit_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    planet_identifier: Mapped[str] = mapped_column(String(255), index=True, nullable=False)

    period_days: Mapped[float] = mapped_column(Float, nullable=False)
    epoch_bjd: Mapped[float] = mapped_column(Float, nullable=False)
    duration_hours: Mapped[float] = mapped_column(Float, nullable=False)
    depth_ppm: Mapped[float] = mapped_column(Float, nullable=False)
    snr: Mapped[float] = mapped_column(Float, nullable=False)
    power: Mapped[float] = mapped_column(Float, nullable=False)
    method: Mapped[str] = mapped_column(String(32), nullable=False, default="bls")

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
