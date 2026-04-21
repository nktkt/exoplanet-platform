"""Repositories encapsulating persistence for each aggregate.

Each repository takes an injected `Session` and exposes a small, domain-shaped
API. SQLAlchemy exceptions are wrapped in `StorageError` so callers only need
to handle the platform's own exception hierarchy.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from exoplanet_platform.domain import (
    Catalog,
    DetectionMethod,
    LightCurve,
    Planet,
    Star,
    TransitSignal,
)
from exoplanet_platform.exceptions import DataSourceNotFoundError, StorageError
from exoplanet_platform.logging_config import get_logger
from exoplanet_platform.storage.models import (
    LightCurveORM,
    PlanetORM,
    StarORM,
    TransitSignalORM,
)

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# ORM <-> domain conversion helpers
# ---------------------------------------------------------------------------


def planet_orm_to_domain(row: PlanetORM) -> Planet:
    """Convert a `PlanetORM` row to a `Planet` domain object."""
    try:
        method = DetectionMethod(row.discovery_method)
    except ValueError:
        method = DetectionMethod.UNKNOWN
    catalog = Catalog(row.catalog) if row.catalog else None
    return Planet(
        identifier=row.identifier,
        host_star=row.host_star,
        discovery_method=method,
        discovery_year=row.discovery_year,
        orbital_period_days=row.orbital_period_days,
        semi_major_axis_au=row.semi_major_axis_au,
        eccentricity=row.eccentricity,
        inclination_deg=row.inclination_deg,
        radius_earth=row.radius_earth,
        mass_earth=row.mass_earth,
        density_g_cm3=row.density_g_cm3,
        equilibrium_temperature_k=row.equilibrium_temperature_k,
        insolation_flux_earth=row.insolation_flux_earth,
        transit_epoch_bjd=row.transit_epoch_bjd,
        transit_duration_hours=row.transit_duration_hours,
        transit_depth_ppm=row.transit_depth_ppm,
        catalog=catalog,
        last_updated=row.last_updated,
    )


def planet_domain_to_orm_kwargs(planet: Planet) -> dict[str, Any]:
    """Convert a `Planet` domain object to kwargs for `PlanetORM`."""
    return {
        "identifier": planet.identifier,
        "host_star": planet.host_star,
        "discovery_method": planet.discovery_method.value,
        "discovery_year": planet.discovery_year,
        "orbital_period_days": planet.orbital_period_days,
        "semi_major_axis_au": planet.semi_major_axis_au,
        "eccentricity": planet.eccentricity,
        "inclination_deg": planet.inclination_deg,
        "radius_earth": planet.radius_earth,
        "mass_earth": planet.mass_earth,
        "density_g_cm3": planet.density_g_cm3,
        "equilibrium_temperature_k": planet.equilibrium_temperature_k,
        "insolation_flux_earth": planet.insolation_flux_earth,
        "transit_epoch_bjd": planet.transit_epoch_bjd,
        "transit_duration_hours": planet.transit_duration_hours,
        "transit_depth_ppm": planet.transit_depth_ppm,
        "catalog": planet.catalog.value if planet.catalog else None,
        "last_updated": planet.last_updated,
    }


def star_orm_to_domain(row: StarORM) -> Star:
    """Convert a `StarORM` row to a `Star` domain object."""
    catalog = Catalog(row.catalog) if row.catalog else None
    return Star(
        identifier=row.identifier,
        ra_deg=row.ra_deg,
        dec_deg=row.dec_deg,
        distance_pc=row.distance_pc,
        effective_temperature_k=row.effective_temperature_k,
        radius_solar=row.radius_solar,
        mass_solar=row.mass_solar,
        luminosity_solar=row.luminosity_solar,
        metallicity_dex=row.metallicity_dex,
        spectral_type=row.spectral_type,
        catalog=catalog,
    )


def star_domain_to_orm_kwargs(star: Star) -> dict[str, Any]:
    """Convert a `Star` domain object to kwargs for `StarORM`."""
    return {
        "identifier": star.identifier,
        "ra_deg": star.ra_deg,
        "dec_deg": star.dec_deg,
        "distance_pc": star.distance_pc,
        "effective_temperature_k": star.effective_temperature_k,
        "radius_solar": star.radius_solar,
        "mass_solar": star.mass_solar,
        "luminosity_solar": star.luminosity_solar,
        "metallicity_dex": star.metallicity_dex,
        "spectral_type": star.spectral_type,
        "catalog": star.catalog.value if star.catalog else None,
    }


def light_curve_orm_to_domain(row: LightCurveORM) -> LightCurve:
    """Convert a `LightCurveORM` row to a `LightCurve` domain object."""
    flux_err = json.loads(row.flux_err_json) if row.flux_err_json else None
    return LightCurve(
        target=row.target,
        mission=Catalog(row.mission),
        time_bjd=json.loads(row.time_bjd_json),
        flux=json.loads(row.flux_json),
        flux_err=flux_err,
        quarter=row.quarter,
        sector=row.sector,
        cadence_minutes=row.cadence_minutes,
    )


def transit_signal_orm_to_domain(row: TransitSignalORM) -> TransitSignal:
    """Convert a `TransitSignalORM` row to a `TransitSignal` domain object."""
    return TransitSignal(
        period_days=row.period_days,
        epoch_bjd=row.epoch_bjd,
        duration_hours=row.duration_hours,
        depth_ppm=row.depth_ppm,
        snr=row.snr,
        power=row.power,
        method=row.method,
    )


# ---------------------------------------------------------------------------
# Repositories
# ---------------------------------------------------------------------------


class PlanetRepository:
    """CRUD operations for `Planet` aggregates."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, planet: Planet) -> PlanetORM:
        """Insert or update a planet keyed by `identifier`. Returns the ORM row."""
        try:
            stmt = select(PlanetORM).where(PlanetORM.identifier == planet.identifier)
            existing = self._session.execute(stmt).scalar_one_or_none()
            kwargs = planet_domain_to_orm_kwargs(planet)
            if existing is None:
                existing = PlanetORM(**kwargs)
                if existing.last_updated is None:
                    existing.last_updated = datetime.utcnow()
                self._session.add(existing)
            else:
                for key, value in kwargs.items():
                    setattr(existing, key, value)
                existing.last_updated = planet.last_updated or datetime.utcnow()
            self._session.flush()
            logger.info("storage.planet.upsert", identifier=planet.identifier)
            return existing
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to upsert planet {planet.identifier}: {exc}") from exc

    def get(self, identifier: str) -> Planet:
        """Fetch one planet by identifier or raise `DataSourceNotFoundError`."""
        try:
            stmt = select(PlanetORM).where(PlanetORM.identifier == identifier)
            row = self._session.execute(stmt).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to fetch planet {identifier}: {exc}") from exc
        if row is None:
            raise DataSourceNotFoundError(f"Planet not found: {identifier}")
        return planet_orm_to_domain(row)

    def list(
        self,
        host_star: str | None = None,
        catalog: Catalog | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Planet]:
        """List planets, optionally filtered by host star or source catalog."""
        try:
            stmt = select(PlanetORM)
            if host_star is not None:
                stmt = stmt.where(PlanetORM.host_star == host_star)
            if catalog is not None:
                stmt = stmt.where(PlanetORM.catalog == catalog.value)
            stmt = stmt.order_by(PlanetORM.identifier).limit(limit).offset(offset)
            rows = self._session.execute(stmt).scalars().all()
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to list planets: {exc}") from exc
        return [planet_orm_to_domain(r) for r in rows]

    def count(
        self, host_star: str | None = None, catalog: Catalog | None = None
    ) -> int:
        """Count planets matching the same filters as `list`."""
        try:
            stmt = select(func.count()).select_from(PlanetORM)
            if host_star is not None:
                stmt = stmt.where(PlanetORM.host_star == host_star)
            if catalog is not None:
                stmt = stmt.where(PlanetORM.catalog == catalog.value)
            return int(self._session.execute(stmt).scalar_one())
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to count planets: {exc}") from exc

    def delete(self, identifier: str) -> None:
        """Delete a planet by identifier. No-op if it does not exist."""
        try:
            stmt = select(PlanetORM).where(PlanetORM.identifier == identifier)
            row = self._session.execute(stmt).scalar_one_or_none()
            if row is not None:
                self._session.delete(row)
                self._session.flush()
                logger.info("storage.planet.delete", identifier=identifier)
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to delete planet {identifier}: {exc}") from exc

    def count_recent(self, days: int = 7) -> int:
        """Count planets whose `created_at` is within the last `days` days."""
        if days < 0:
            raise StorageError("count_recent: days must be non-negative")
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            stmt = select(func.count()).select_from(PlanetORM).where(
                PlanetORM.created_at >= cutoff
            )
            return int(self._session.execute(stmt).scalar_one())
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to count recent planets: {exc}") from exc


class StarRepository:
    """CRUD operations for `Star` aggregates."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, star: Star) -> StarORM:
        """Insert or update a star keyed by `identifier`. Returns the ORM row."""
        try:
            stmt = select(StarORM).where(StarORM.identifier == star.identifier)
            existing = self._session.execute(stmt).scalar_one_or_none()
            kwargs = star_domain_to_orm_kwargs(star)
            if existing is None:
                existing = StarORM(**kwargs)
                self._session.add(existing)
            else:
                for key, value in kwargs.items():
                    setattr(existing, key, value)
            self._session.flush()
            logger.info("storage.star.upsert", identifier=star.identifier)
            return existing
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to upsert star {star.identifier}: {exc}") from exc

    def get(self, identifier: str) -> Star:
        """Fetch one star by identifier or raise `DataSourceNotFoundError`."""
        try:
            stmt = select(StarORM).where(StarORM.identifier == identifier)
            row = self._session.execute(stmt).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to fetch star {identifier}: {exc}") from exc
        if row is None:
            raise DataSourceNotFoundError(f"Star not found: {identifier}")
        return star_orm_to_domain(row)

    def list(
        self,
        catalog: Catalog | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Star]:
        """List stars, optionally filtered by source catalog."""
        try:
            stmt = select(StarORM)
            if catalog is not None:
                stmt = stmt.where(StarORM.catalog == catalog.value)
            stmt = stmt.order_by(StarORM.identifier).limit(limit).offset(offset)
            rows = self._session.execute(stmt).scalars().all()
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to list stars: {exc}") from exc
        return [star_orm_to_domain(r) for r in rows]

    def delete(self, identifier: str) -> None:
        """Delete a star by identifier. No-op if it does not exist."""
        try:
            stmt = select(StarORM).where(StarORM.identifier == identifier)
            row = self._session.execute(stmt).scalar_one_or_none()
            if row is not None:
                self._session.delete(row)
                self._session.flush()
                logger.info("storage.star.delete", identifier=identifier)
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to delete star {identifier}: {exc}") from exc

    def count(self, catalog: Catalog | None = None) -> int:
        """Count stars, optionally filtered by source catalog."""
        try:
            stmt = select(func.count()).select_from(StarORM)
            if catalog is not None:
                stmt = stmt.where(StarORM.catalog == catalog.value)
            return int(self._session.execute(stmt).scalar_one())
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to count stars: {exc}") from exc


class LightCurveRepository:
    """Persistence for `LightCurve` aggregates with JSON-serialized arrays."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def save(self, lc: LightCurve) -> int:
        """Persist a light curve and return the new row id."""
        try:
            row = LightCurveORM(
                target=lc.target,
                mission=lc.mission.value,
                quarter=lc.quarter,
                sector=lc.sector,
                cadence_minutes=lc.cadence_minutes,
                time_bjd_json=json.dumps(list(lc.time_bjd)),
                flux_json=json.dumps(list(lc.flux)),
                flux_err_json=json.dumps(list(lc.flux_err)) if lc.flux_err is not None else None,
            )
            self._session.add(row)
            self._session.flush()
            logger.info("storage.light_curve.save", target=lc.target, id=row.id)
            return int(row.id)
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to save light curve for {lc.target}: {exc}") from exc

    def load(self, id: int) -> LightCurve:
        """Load a light curve by row id or raise `DataSourceNotFoundError`."""
        try:
            row = self._session.get(LightCurveORM, id)
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to load light curve {id}: {exc}") from exc
        if row is None:
            raise DataSourceNotFoundError(f"Light curve not found: id={id}")
        return light_curve_orm_to_domain(row)

    def count(self, target: str | None = None) -> int:
        """Count stored light curves, optionally filtered by target."""
        try:
            stmt = select(func.count()).select_from(LightCurveORM)
            if target is not None:
                stmt = stmt.where(LightCurveORM.target == target)
            return int(self._session.execute(stmt).scalar_one())
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to count light curves: {exc}") from exc

    def list_for_target(self, target: str) -> list[dict[str, Any]]:
        """Return metadata-only dicts for all light curves belonging to `target`."""
        try:
            stmt = (
                select(LightCurveORM)
                .where(LightCurveORM.target == target)
                .order_by(LightCurveORM.id)
            )
            rows = self._session.execute(stmt).scalars().all()
        except SQLAlchemyError as exc:
            raise StorageError(f"Failed to list light curves for {target}: {exc}") from exc
        return [
            {
                "id": r.id,
                "target": r.target,
                "mission": r.mission,
                "quarter": r.quarter,
                "sector": r.sector,
                "cadence_minutes": r.cadence_minutes,
                "created_at": r.created_at,
            }
            for r in rows
        ]


class TransitSignalRepository:
    """Persistence for `TransitSignal` aggregates tied to a planet identifier."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def save(self, planet_id: str, sig: TransitSignal) -> int:
        """Persist a transit signal row and return its new id."""
        try:
            row = TransitSignalORM(
                planet_identifier=planet_id,
                period_days=sig.period_days,
                epoch_bjd=sig.epoch_bjd,
                duration_hours=sig.duration_hours,
                depth_ppm=sig.depth_ppm,
                snr=sig.snr,
                power=sig.power,
                method=sig.method,
            )
            self._session.add(row)
            self._session.flush()
            logger.info("storage.transit_signal.save", planet=planet_id, id=row.id)
            return int(row.id)
        except SQLAlchemyError as exc:
            raise StorageError(
                f"Failed to save transit signal for {planet_id}: {exc}"
            ) from exc

    def list_for_planet(self, planet_id: str) -> list[TransitSignal]:
        """Return all transit signals belonging to `planet_id`."""
        try:
            stmt = (
                select(TransitSignalORM)
                .where(TransitSignalORM.planet_identifier == planet_id)
                .order_by(TransitSignalORM.power.desc())
            )
            rows = self._session.execute(stmt).scalars().all()
        except SQLAlchemyError as exc:
            raise StorageError(
                f"Failed to list transit signals for {planet_id}: {exc}"
            ) from exc
        return [transit_signal_orm_to_domain(r) for r in rows]
