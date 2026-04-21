"""Tests for classical orbital mechanics helpers."""

from __future__ import annotations

import math

import pytest

from exoplanet_platform.analysis.orbital import OrbitalMechanics
from exoplanet_platform.exceptions import ValidationError


class TestKeplerThirdLaw:
    def test_earth_orbit_roundtrip(self) -> None:
        # Earth: a = 1 AU, M = 1 Msun -> P ~ 365.25 days.
        p = OrbitalMechanics.kepler_third_law_period(1.0, 1.0)
        assert p == pytest.approx(365.25, rel=1e-3)
        a = OrbitalMechanics.kepler_third_law_semi_major_axis(p, 1.0)
        assert a == pytest.approx(1.0, abs=1e-9)

    @pytest.mark.parametrize(
        "a_au, m_solar",
        [(0.05, 0.5), (1.0, 1.0), (5.2, 1.0), (30.0, 1.0), (0.1, 2.0)],
    )
    def test_roundtrip_various(self, a_au: float, m_solar: float) -> None:
        p = OrbitalMechanics.kepler_third_law_period(a_au, m_solar)
        a_back = OrbitalMechanics.kepler_third_law_semi_major_axis(p, m_solar)
        assert a_back == pytest.approx(a_au, rel=1e-9)

    def test_rejects_bad_inputs(self) -> None:
        with pytest.raises(ValidationError):
            OrbitalMechanics.kepler_third_law_period(0.0, 1.0)
        with pytest.raises(ValidationError):
            OrbitalMechanics.kepler_third_law_period(1.0, 0.0)
        with pytest.raises(ValidationError):
            OrbitalMechanics.kepler_third_law_semi_major_axis(-5.0, 1.0)


class TestSolveKepler:
    @pytest.mark.parametrize("M", [0.0, math.pi / 4, math.pi / 2, math.pi, 3.0])
    @pytest.mark.parametrize("e", [0.0, 0.1, 0.5, 0.9, 0.95])
    def test_round_trip(self, M: float, e: float) -> None:
        E = OrbitalMechanics.solve_kepler_equation(M, e)
        # Residual of Kepler equation: M_back = E - e*sinE.
        M_back = (E - e * math.sin(E)) % (2.0 * math.pi)
        assert M_back == pytest.approx(M % (2.0 * math.pi), abs=1e-8)

    def test_rejects_e_out_of_range(self) -> None:
        with pytest.raises(ValidationError):
            OrbitalMechanics.solve_kepler_equation(0.0, 1.0)
        with pytest.raises(ValidationError):
            OrbitalMechanics.solve_kepler_equation(0.0, -0.1)


class TestTrueAnomaly:
    def test_m_zero_is_zero(self) -> None:
        assert OrbitalMechanics.true_anomaly(0.0, 0.3) == pytest.approx(0.0, abs=1e-12)

    def test_m_pi_is_pi_for_circular(self) -> None:
        nu = OrbitalMechanics.true_anomaly(math.pi, 0.0)
        assert nu == pytest.approx(math.pi, abs=1e-9)

    def test_rejects_bad_eccentricity(self) -> None:
        with pytest.raises(ValidationError):
            OrbitalMechanics.true_anomaly(0.0, 1.0)


class TestPositionAtTime:
    def test_initial_position(self) -> None:
        # At t=0 with everything zero the body should sit at (a, 0, 0).
        x, y, z = OrbitalMechanics.position_at_time(
            a_au=1.0,
            e=0.0,
            i_deg=0.0,
            raan_deg=0.0,
            argp_deg=0.0,
            M0_deg=0.0,
            period_days=365.25,
            t_since_epoch_days=0.0,
        )
        assert x == pytest.approx(1.0, abs=1e-10)
        assert y == pytest.approx(0.0, abs=1e-10)
        assert z == pytest.approx(0.0, abs=1e-10)


class TestEquilibriumTemperature:
    def test_earth_like(self) -> None:
        t_eq = OrbitalMechanics.equilibrium_temperature(
            stellar_teff_k=5778.0,
            stellar_radius_solar=1.0,
            semi_major_axis_au=1.0,
            albedo=0.3,
        )
        # Textbook Earth T_eq ~ 254 K.
        assert t_eq == pytest.approx(254.0, abs=5.0)


class TestTransitProbability:
    def test_earth_sun(self) -> None:
        prob = OrbitalMechanics.transit_probability(
            planet_radius_earth=1.0,
            star_radius_solar=1.0,
            semi_major_axis_au=1.0,
        )
        # Rough geometric probability ~ 0.005.
        assert 0.003 < prob < 0.01

    def test_rejects_bad_inputs(self) -> None:
        with pytest.raises(ValidationError):
            OrbitalMechanics.transit_probability(-1.0, 1.0, 1.0)
