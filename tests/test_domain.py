"""Tests for the pydantic domain models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError as PydanticValidationError

from exoplanet_platform.domain import (
    Catalog,
    DetectionMethod,
    HabitabilityAssessment,
    LightCurve,
    Planet,
    Star,
    TransitSignal,
)


class TestEnums:
    def test_catalog_values(self) -> None:
        assert Catalog.NASA_EXOPLANET_ARCHIVE.value == "nasa_exoplanet_archive"
        assert Catalog.TESS.value == "tess"

    def test_detection_method_values(self) -> None:
        assert DetectionMethod.TRANSIT.value == "transit"
        assert DetectionMethod.UNKNOWN.value == "unknown"


class TestStar:
    def test_minimum_valid(self) -> None:
        star = Star(identifier="x")
        assert star.identifier == "x"
        assert star.ra_deg is None

    def test_frozen(self) -> None:
        star = Star(identifier="x")
        with pytest.raises(PydanticValidationError):
            star.ra_deg = 10.0  # type: ignore[misc]

    def test_requires_identifier(self) -> None:
        with pytest.raises(PydanticValidationError):
            Star()  # type: ignore[call-arg]

    def test_type_coercion_is_strict_on_str_field(self) -> None:
        # identifier is str; passing an int should not silently become a str.
        with pytest.raises(PydanticValidationError):
            Star(identifier=123)  # type: ignore[arg-type]


class TestPlanet:
    def test_minimum_valid(self) -> None:
        p = Planet(identifier="P", host_star="HS")
        assert p.host_star == "HS"
        assert p.discovery_method is DetectionMethod.UNKNOWN

    def test_frozen(self) -> None:
        p = Planet(identifier="P", host_star="HS")
        with pytest.raises(PydanticValidationError):
            p.radius_earth = 1.0  # type: ignore[misc]

    def test_requires_identifier_and_host(self) -> None:
        with pytest.raises(PydanticValidationError):
            Planet(identifier="P")  # type: ignore[call-arg]


class TestLightCurve:
    def test_minimum_valid(self) -> None:
        lc = LightCurve(
            target="t",
            mission=Catalog.TESS,
            time_bjd=[0.0, 1.0],
            flux=[1.0, 1.0],
        )
        assert lc.mission is Catalog.TESS
        assert lc.flux_err is None

    def test_mission_is_enum(self) -> None:
        with pytest.raises(PydanticValidationError):
            LightCurve(
                target="t",
                mission="not-a-catalog",  # type: ignore[arg-type]
                time_bjd=[0.0],
                flux=[1.0],
            )

    def test_not_frozen(self) -> None:
        """LightCurve uses arbitrary_types_allowed but is intentionally mutable."""
        lc = LightCurve(target="t", mission=Catalog.TESS, time_bjd=[0.0], flux=[1.0])
        lc.quarter = 3
        assert lc.quarter == 3


class TestTransitSignal:
    def test_defaults_and_frozen(self) -> None:
        s = TransitSignal(
            period_days=1.2,
            epoch_bjd=2.3,
            duration_hours=3.4,
            depth_ppm=45.0,
            snr=6.0,
            power=7.0,
        )
        assert s.method == "bls"
        with pytest.raises(PydanticValidationError):
            s.period_days = 9.9  # type: ignore[misc]


class TestHabitabilityAssessment:
    def test_round_trip(self) -> None:
        a = HabitabilityAssessment(
            planet="p",
            in_conservative_hz=True,
            in_optimistic_hz=True,
            hz_inner_au=0.8,
            hz_outer_au=1.7,
        )
        assert a.earth_similarity_index is None
        with pytest.raises(PydanticValidationError):
            a.planet = "other"  # type: ignore[misc]
