"""Shared pytest fixtures for the exoplanet-platform test suite.

The test database is an in-memory SQLite DB scoped to the session; individual
tests get a transactional `db_session` fixture that rolls back at teardown.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import numpy as np
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from exoplanet_platform.api.main import create_app
from exoplanet_platform.config import reset_settings_cache
from exoplanet_platform.domain import (
    Catalog,
    DetectionMethod,
    LightCurve,
    Planet,
    Star,
)
from exoplanet_platform.storage.database import (
    get_engine,
    get_session_factory,
    init_db,
    reset_engine,
)
from exoplanet_platform.storage.models import Base

# ---------------------------------------------------------------------------
# Session-level setup: isolate every test run from the user's real DB.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _test_settings() -> Iterator[None]:
    """Point every test at an in-memory SQLite DB and create its schema."""
    os.environ["EXOPLANET_STORAGE__DATABASE_URL"] = "sqlite:///:memory:"
    os.environ.setdefault("EXOPLANET_ENVIRONMENT", "test")
    # Rich truncates cells to the detected terminal width; under pytest this
    # defaults to ~80 cols and makes string assertions against rendered tables
    # unreliable. Force a wide output so cells stay intact.
    os.environ["COLUMNS"] = "200"
    reset_settings_cache()
    reset_engine()
    init_db()
    yield
    reset_engine()
    reset_settings_cache()


# ---------------------------------------------------------------------------
# Per-test DB cleanup. The shared in-memory SQLite uses a StaticPool with a
# single connection so TestClient commits are visible across tests; we wipe
# every table before each test to keep them isolated.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_db() -> Iterator[None]:
    """Delete all rows from every table before each test."""
    engine = get_engine()
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())
    yield


# ---------------------------------------------------------------------------
# Per-test DB session inside a rollback-only transaction.
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_session() -> Iterator[Session]:
    """Yield a transactional session that rolls back on teardown.

    Uses SQLAlchemy's "nested-transaction via SAVEPOINT" pattern so each test
    can freely commit without leaking state across tests.
    """
    engine = get_engine()
    connection = engine.connect()
    trans = connection.begin()
    factory = get_session_factory()
    session = factory(bind=connection)
    try:
        yield session
    finally:
        session.close()
        trans.rollback()
        connection.close()


# ---------------------------------------------------------------------------
# Domain-model fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_star() -> Star:
    """Sun-like star used across habitability / stellar tests."""
    return Star(
        identifier="TestSun",
        ra_deg=10.0,
        dec_deg=-5.0,
        distance_pc=10.0,
        effective_temperature_k=5778.0,
        radius_solar=1.0,
        mass_solar=1.0,
        luminosity_solar=1.0,
        metallicity_dex=0.0,
        spectral_type="G",
        catalog=Catalog.GAIA,
    )


@pytest.fixture()
def sample_planet() -> Planet:
    """Earth-like planet orbiting `sample_star` at 1 AU."""
    return Planet(
        identifier="TestSun b",
        host_star="TestSun",
        discovery_method=DetectionMethod.TRANSIT,
        discovery_year=2024,
        orbital_period_days=365.25,
        semi_major_axis_au=1.0,
        eccentricity=0.0167,
        inclination_deg=89.5,
        radius_earth=1.0,
        mass_earth=1.0,
        density_g_cm3=5.514,
        equilibrium_temperature_k=288.0,
        insolation_flux_earth=1.0,
        transit_epoch_bjd=2_450_000.0,
        transit_duration_hours=13.0,
        transit_depth_ppm=84.0,
        catalog=Catalog.NASA_EXOPLANET_ARCHIVE,
    )


# ---------------------------------------------------------------------------
# Synthetic light-curve fixture with a known transit signal.
# ---------------------------------------------------------------------------


def _synthetic_transit_lightcurve(
    period_days: float = 3.0,
    epoch_bjd: float = 2.0,
    depth: float = 1.0e-3,
    duration_hours: float = 3.0,
    baseline_days: float = 30.0,
    cadence_minutes: float = 30.0,
    noise_sigma: float = 1.0e-4,
    seed: int = 42,
) -> LightCurve:
    """Build a deterministic box-transit light curve suitable for BLS recovery."""
    rng = np.random.default_rng(seed)
    cadence_days = cadence_minutes / (60.0 * 24.0)
    n_points = int(baseline_days / cadence_days)
    times = np.linspace(0.0, baseline_days, n_points, endpoint=False)

    # Gentle sinusoidal baseline so detrending has something to do.
    baseline = 1.0 + 5.0e-4 * np.sin(2.0 * np.pi * times / 7.0)

    # Box-shaped transit dips.
    half_dur_days = (duration_hours / 24.0) / 2.0
    phase = ((times - epoch_bjd) / period_days + 0.5) % 1.0 - 0.5
    in_transit = np.abs(phase * period_days) < half_dur_days

    flux = baseline.copy()
    flux[in_transit] -= depth
    flux += rng.normal(0.0, noise_sigma, size=flux.shape)
    flux_err = np.full_like(flux, noise_sigma)

    return LightCurve(
        target="synthetic-transit",
        mission=Catalog.TESS,
        time_bjd=times.tolist(),
        flux=flux.tolist(),
        flux_err=flux_err.tolist(),
        sector=1,
        cadence_minutes=cadence_minutes,
    )


@pytest.fixture()
def sample_light_curve() -> LightCurve:
    """Synthetic transit light curve with known period = 3.0 d, depth = 1e-3."""
    return _synthetic_transit_lightcurve()


@pytest.fixture()
def make_light_curve():
    """Factory fixture so tests can tweak the synthetic light curve parameters."""
    return _synthetic_transit_lightcurve


# ---------------------------------------------------------------------------
# FastAPI TestClient.
# ---------------------------------------------------------------------------


@pytest.fixture()
def api_client() -> Iterator[TestClient]:
    """Build a fresh FastAPI TestClient backed by the shared in-memory DB."""
    app = create_app()
    with TestClient(app) as client:
        yield client
