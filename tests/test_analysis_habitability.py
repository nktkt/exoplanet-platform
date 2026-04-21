"""Tests for Kopparapu habitable-zone bounds and Earth Similarity Index."""

from __future__ import annotations

import pytest

from exoplanet_platform.analysis.habitability import HabitabilityAnalyzer
from exoplanet_platform.domain import Catalog, DetectionMethod, Planet, Star
from exoplanet_platform.exceptions import InsufficientDataError


@pytest.fixture()
def sun() -> Star:
    return Star(
        identifier="Sun",
        effective_temperature_k=5778.0,
        radius_solar=1.0,
        luminosity_solar=1.0,
        catalog=Catalog.GAIA,
    )


class TestHabitableZone:
    def test_sun_bounds_include_earth(self, sun: Star) -> None:
        cons_in, cons_out, opt_in, opt_out = HabitabilityAnalyzer().habitable_zone(sun)
        # Kopparapu conservative HZ for Sun roughly 0.95 - 1.67 AU.
        assert 0.85 < cons_in < 1.0
        assert 1.5 < cons_out < 1.85
        assert cons_in <= 1.0 <= cons_out
        # Optimistic bounds are strictly wider than conservative.
        assert opt_in <= cons_in
        assert opt_out >= cons_out

    def test_missing_teff_raises(self) -> None:
        star = Star(identifier="opaque", radius_solar=1.0, luminosity_solar=1.0)
        with pytest.raises(InsufficientDataError):
            HabitabilityAnalyzer().habitable_zone(star)

    def test_missing_luminosity_derivable(self) -> None:
        star = Star(
            identifier="X",
            effective_temperature_k=5778.0,
            radius_solar=1.0,
        )
        cons_in, cons_out, _, _ = HabitabilityAnalyzer().habitable_zone(star)
        assert cons_in < cons_out


class TestAssessment:
    def test_earth_in_conservative_hz(self, sun: Star) -> None:
        earth = Planet(
            identifier="Earth",
            host_star="Sun",
            discovery_method=DetectionMethod.TRANSIT,
            semi_major_axis_au=1.0,
            radius_earth=1.0,
            mass_earth=1.0,
            density_g_cm3=5.514,
            equilibrium_temperature_k=288.0,
        )
        assessment = HabitabilityAnalyzer().assess(earth, sun)
        assert assessment.in_conservative_hz is True
        assert assessment.in_optimistic_hz is True
        assert assessment.earth_similarity_index is not None
        assert assessment.earth_similarity_index == pytest.approx(1.0, abs=1e-3)

    def test_mercury_outside_hz(self, sun: Star) -> None:
        mercury = Planet(
            identifier="Mercury",
            host_star="Sun",
            semi_major_axis_au=0.387,
            radius_earth=0.383,
            mass_earth=0.055,
            equilibrium_temperature_k=440.0,
        )
        assessment = HabitabilityAnalyzer().assess(mercury, sun)
        assert assessment.in_conservative_hz is False
        assert assessment.in_optimistic_hz is False

    def test_hot_jupiter_low_esi(self, sun: Star) -> None:
        hot_j = Planet(
            identifier="HJ",
            host_star="Sun",
            semi_major_axis_au=0.05,
            radius_earth=11.0,
            mass_earth=300.0,
            density_g_cm3=1.0,
            equilibrium_temperature_k=1500.0,
        )
        assessment = HabitabilityAnalyzer().assess(hot_j, sun)
        assert assessment.in_conservative_hz is False
        assert assessment.earth_similarity_index is not None
        assert assessment.earth_similarity_index < 0.5

    def test_missing_sma_raises(self, sun: Star) -> None:
        p = Planet(identifier="p", host_star="Sun")
        with pytest.raises(InsufficientDataError):
            HabitabilityAnalyzer().assess(p, sun)
