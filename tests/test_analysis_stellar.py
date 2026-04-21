"""Tests for stellar property derivation."""

from __future__ import annotations

import pytest

from exoplanet_platform.analysis.stellar import StellarAnalyzer
from exoplanet_platform.domain import Star
from exoplanet_platform.exceptions import ValidationError


class TestLuminosity:
    def test_sun_is_one_lsun(self) -> None:
        sun = Star(
            identifier="Sun",
            radius_solar=1.0,
            effective_temperature_k=5772.0,
        )
        lum = StellarAnalyzer().luminosity(sun)
        assert lum is not None
        # Stefan-Boltzmann gives ~1 Lsun to within 0.5% at Teff=5772 K.
        assert lum == pytest.approx(1.0, rel=5e-3)

    def test_missing_radius_returns_none(self) -> None:
        s = Star(identifier="x", effective_temperature_k=5000.0)
        assert StellarAnalyzer().luminosity(s) is None

    def test_missing_teff_returns_none(self) -> None:
        s = Star(identifier="x", radius_solar=1.0)
        assert StellarAnalyzer().luminosity(s) is None


class TestSpectralType:
    @pytest.mark.parametrize(
        "teff, expected",
        [
            (40000.0, "O"),
            (20000.0, "B"),
            (8500.0, "A"),
            (6500.0, "F"),
            (5778.0, "G"),
            (4500.0, "K"),
            (3000.0, "M"),
        ],
    )
    def test_classification_boundaries(self, teff: float, expected: str) -> None:
        assert StellarAnalyzer.classify_spectral_type(teff) == expected

    def test_rejects_non_positive_teff(self) -> None:
        with pytest.raises(ValidationError):
            StellarAnalyzer.classify_spectral_type(0.0)


class TestLifetime:
    def test_solar_is_roughly_10_gyr(self) -> None:
        assert StellarAnalyzer.main_sequence_lifetime_gyr(1.0) == pytest.approx(10.0)

    def test_massive_star_shorter(self) -> None:
        t_solar = StellarAnalyzer.main_sequence_lifetime_gyr(1.0)
        t_heavy = StellarAnalyzer.main_sequence_lifetime_gyr(10.0)
        assert t_heavy < t_solar

    def test_rejects_non_positive_mass(self) -> None:
        with pytest.raises(ValidationError):
            StellarAnalyzer.main_sequence_lifetime_gyr(-1.0)


class TestEnrich:
    def test_fills_missing_luminosity_and_spectral_type(self) -> None:
        s = Star(identifier="X", radius_solar=1.0, effective_temperature_k=5778.0)
        enriched = StellarAnalyzer().enrich(s)
        assert enriched.luminosity_solar is not None
        assert enriched.spectral_type == "G"
        # Frozen means enrich must return a new instance.
        assert enriched is not s

    def test_preserves_explicit_values(self) -> None:
        s = Star(
            identifier="X",
            radius_solar=1.0,
            effective_temperature_k=5778.0,
            luminosity_solar=2.5,
            spectral_type="F",
        )
        enriched = StellarAnalyzer().enrich(s)
        assert enriched.luminosity_solar == 2.5
        assert enriched.spectral_type == "F"
