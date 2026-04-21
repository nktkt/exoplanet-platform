"""Tests for storage repositories against the in-memory SQLite fixture DB."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
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
from exoplanet_platform.storage.repository import (
    LightCurveRepository,
    PlanetRepository,
    StarRepository,
    TransitSignalRepository,
)

# ---------------------------------------------------------------------------
# PlanetRepository
# ---------------------------------------------------------------------------


class TestPlanetRepository:
    def test_upsert_and_get(self, db_session: Session, sample_planet: Planet) -> None:
        repo = PlanetRepository(db_session)
        repo.upsert(sample_planet)
        got = repo.get(sample_planet.identifier)
        assert got.identifier == sample_planet.identifier
        assert got.host_star == sample_planet.host_star
        assert got.orbital_period_days == pytest.approx(sample_planet.orbital_period_days)

    def test_upsert_updates_existing(
        self, db_session: Session, sample_planet: Planet
    ) -> None:
        repo = PlanetRepository(db_session)
        repo.upsert(sample_planet)
        updated = sample_planet.model_copy(update={"radius_earth": 2.5})
        repo.upsert(updated)
        got = repo.get(sample_planet.identifier)
        assert got.radius_earth == pytest.approx(2.5)
        assert repo.count() == 1

    def test_get_missing_raises(self, db_session: Session) -> None:
        repo = PlanetRepository(db_session)
        with pytest.raises(DataSourceNotFoundError):
            repo.get("nope")

    def test_list_with_filters(self, db_session: Session) -> None:
        repo = PlanetRepository(db_session)
        for i in range(3):
            repo.upsert(
                Planet(
                    identifier=f"P{i}",
                    host_star="HS1" if i < 2 else "HS2",
                    discovery_method=DetectionMethod.TRANSIT,
                    catalog=Catalog.KEPLER if i == 0 else Catalog.TESS,
                )
            )
        all_rows = repo.list()
        assert len(all_rows) == 3
        hs1 = repo.list(host_star="HS1")
        assert {p.identifier for p in hs1} == {"P0", "P1"}
        kepler = repo.list(catalog=Catalog.KEPLER)
        assert {p.identifier for p in kepler} == {"P0"}
        assert repo.count(host_star="HS1") == 2
        assert repo.count(catalog=Catalog.TESS) == 2

    def test_delete(self, db_session: Session, sample_planet: Planet) -> None:
        repo = PlanetRepository(db_session)
        repo.upsert(sample_planet)
        repo.delete(sample_planet.identifier)
        with pytest.raises(DataSourceNotFoundError):
            repo.get(sample_planet.identifier)
        # Idempotent.
        repo.delete(sample_planet.identifier)

    def test_count_recent(self, db_session: Session, sample_planet: Planet) -> None:
        repo = PlanetRepository(db_session)
        repo.upsert(sample_planet)
        assert repo.count_recent(days=1) >= 1
        assert repo.count_recent(days=0) >= 0

    def test_count_recent_negative_raises(self, db_session: Session) -> None:
        repo = PlanetRepository(db_session)
        with pytest.raises(StorageError):
            repo.count_recent(days=-1)


# ---------------------------------------------------------------------------
# StarRepository
# ---------------------------------------------------------------------------


class TestStarRepository:
    def test_upsert_and_get(self, db_session: Session, sample_star: Star) -> None:
        repo = StarRepository(db_session)
        repo.upsert(sample_star)
        got = repo.get(sample_star.identifier)
        assert got.effective_temperature_k == pytest.approx(5778.0)

    def test_list_filter_by_catalog(self, db_session: Session) -> None:
        repo = StarRepository(db_session)
        repo.upsert(Star(identifier="A", catalog=Catalog.GAIA))
        repo.upsert(Star(identifier="B", catalog=Catalog.TESS))
        gaia = repo.list(catalog=Catalog.GAIA)
        assert [s.identifier for s in gaia] == ["A"]
        assert repo.count() == 2

    def test_missing_raises(self, db_session: Session) -> None:
        repo = StarRepository(db_session)
        with pytest.raises(DataSourceNotFoundError):
            repo.get("nope")


# ---------------------------------------------------------------------------
# LightCurveRepository
# ---------------------------------------------------------------------------


class TestLightCurveRepository:
    def test_save_load_preserves_arrays(self, db_session: Session) -> None:
        repo = LightCurveRepository(db_session)
        times = [0.0, 0.5, 1.0, 1.5]
        fluxes = [1.0, 1.01, 0.99, 1.0]
        errs = [0.001, 0.001, 0.001, 0.001]
        lc = LightCurve(
            target="t1",
            mission=Catalog.TESS,
            time_bjd=times,
            flux=fluxes,
            flux_err=errs,
            sector=1,
            cadence_minutes=30.0,
        )
        new_id = repo.save(lc)
        loaded = repo.load(new_id)
        assert loaded.time_bjd == times
        assert loaded.flux == fluxes
        assert loaded.flux_err == errs
        assert loaded.mission is Catalog.TESS

    def test_load_missing_raises(self, db_session: Session) -> None:
        repo = LightCurveRepository(db_session)
        with pytest.raises(DataSourceNotFoundError):
            repo.load(987654)

    def test_list_for_target(self, db_session: Session) -> None:
        repo = LightCurveRepository(db_session)
        for sector in (1, 2):
            repo.save(
                LightCurve(
                    target="t",
                    mission=Catalog.TESS,
                    time_bjd=[0.0],
                    flux=[1.0],
                    sector=sector,
                )
            )
        meta = repo.list_for_target("t")
        assert len(meta) == 2
        assert {m["sector"] for m in meta} == {1, 2}
        assert repo.count(target="t") == 2


# ---------------------------------------------------------------------------
# TransitSignalRepository
# ---------------------------------------------------------------------------


class TestTransitSignalRepository:
    def test_save_and_list(self, db_session: Session) -> None:
        repo = TransitSignalRepository(db_session)
        sig = TransitSignal(
            period_days=3.14,
            epoch_bjd=2.0,
            duration_hours=2.1,
            depth_ppm=500.0,
            snr=7.0,
            power=0.123,
        )
        repo.save("planet1", sig)
        rows = repo.list_for_planet("planet1")
        assert len(rows) == 1
        assert rows[0].period_days == pytest.approx(3.14)


# ---------------------------------------------------------------------------
# Error wrapping.
# ---------------------------------------------------------------------------


def test_sql_failure_wrapped_as_storage_error() -> None:
    """Simulated SQLAlchemy failure should surface as StorageError."""
    bad_session = MagicMock(spec=Session)
    bad_session.execute.side_effect = SQLAlchemyError("boom")
    repo = PlanetRepository(bad_session)
    with pytest.raises(StorageError):
        repo.count()
