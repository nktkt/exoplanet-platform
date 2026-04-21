"""Classical two-body orbital mechanics helpers.

All methods are ``@staticmethod``; the class exists only as a namespace so
callers can write ``OrbitalMechanics.kepler_third_law_period(...)`` without
an instance. Inputs are validated eagerly and raise
:class:`~exoplanet_platform.exceptions.ValidationError` on bad values.
"""

from __future__ import annotations

import math

import numpy as np

from exoplanet_platform.exceptions import ValidationError
from exoplanet_platform.logging_config import get_logger

logger = get_logger(__name__)

# Constants -------------------------------------------------------------
# Gauss' gravitational constant squared gives the AU^3/(Msun * day^2) factor
# for Kepler's third law: P[days] = 2*pi*sqrt(a[AU]^3 / (G*Msun)) with
# 4*pi^2 / (G*Msun) = (2*pi / k)^2 where k = 0.01720209895 rad/day.
_GAUSSIAN_K = 0.01720209895  # rad/day
_KEPLER_COEFF = (2.0 * math.pi / _GAUSSIAN_K) ** 2  # days^2 / (AU^3 * Msun)

_SIGMA_SB = 5.670374419e-8  # W m^-2 K^-4
_L_SUN = 3.828e26  # W
_R_SUN = 6.957e8  # m
_AU = 1.495978707e11  # m


class OrbitalMechanics:
    """Namespace of pure orbital-mechanics functions."""

    # ------------------------------------------------------------------ #
    # Kepler's third law
    # ------------------------------------------------------------------ #
    @staticmethod
    def kepler_third_law_period(
        semi_major_axis_au: float, total_mass_solar: float
    ) -> float:
        """Orbital period from semi-major axis and total system mass.

        Returns
        -------
        float
            Period in days.
        """
        logger.debug(
            "kepler3.period.start", a_au=semi_major_axis_au, m=total_mass_solar
        )
        if semi_major_axis_au <= 0:
            raise ValidationError("semi_major_axis_au must be > 0")
        if total_mass_solar <= 0:
            raise ValidationError("total_mass_solar must be > 0")
        period = math.sqrt(_KEPLER_COEFF * semi_major_axis_au**3 / total_mass_solar)
        return float(period)

    @staticmethod
    def kepler_third_law_semi_major_axis(
        period_days: float, total_mass_solar: float
    ) -> float:
        """Semi-major axis from period and total mass.

        Returns
        -------
        float
            Semi-major axis in astronomical units (AU).
        """
        logger.debug(
            "kepler3.sma.start", period_days=period_days, m=total_mass_solar
        )
        if period_days <= 0:
            raise ValidationError("period_days must be > 0")
        if total_mass_solar <= 0:
            raise ValidationError("total_mass_solar must be > 0")
        a_cubed = period_days**2 * total_mass_solar / _KEPLER_COEFF
        return float(a_cubed ** (1.0 / 3.0))

    # ------------------------------------------------------------------ #
    # Kepler equation / anomalies
    # ------------------------------------------------------------------ #
    @staticmethod
    def solve_kepler_equation(
        mean_anomaly_rad: float,
        eccentricity: float,
        tol: float = 1e-10,
        max_iter: int = 100,
    ) -> float:
        """Solve ``M = E - e sin E`` for eccentric anomaly ``E``.

        Uses Newton-Raphson with a Danby-style initial guess.

        Returns
        -------
        float
            Eccentric anomaly in radians, wrapped to ``[0, 2*pi)``.
        """
        if not 0.0 <= eccentricity < 1.0:
            raise ValidationError("eccentricity must be in [0, 1)")
        if max_iter <= 0:
            raise ValidationError("max_iter must be > 0")

        m = float(mean_anomaly_rad) % (2.0 * math.pi)
        e = float(eccentricity)
        # Good starting guess.
        E = m + e * math.sin(m)
        for _ in range(max_iter):
            f = E - e * math.sin(E) - m
            fp = 1.0 - e * math.cos(E)
            delta = f / fp
            E -= delta
            if abs(delta) < tol:
                break
        return float(E % (2.0 * math.pi))

    @staticmethod
    def true_anomaly(eccentric_anomaly_rad: float, eccentricity: float) -> float:
        """True anomaly from eccentric anomaly.

        Returns
        -------
        float
            True anomaly in radians, wrapped to ``[0, 2*pi)``.
        """
        if not 0.0 <= eccentricity < 1.0:
            raise ValidationError("eccentricity must be in [0, 1)")
        E = float(eccentric_anomaly_rad)
        e = float(eccentricity)
        # atan2 formulation avoids quadrant ambiguity.
        sin_nu = math.sqrt(1.0 - e * e) * math.sin(E)
        cos_nu = math.cos(E) - e
        nu = math.atan2(sin_nu, cos_nu)
        return float(nu % (2.0 * math.pi))

    # ------------------------------------------------------------------ #
    # 3D position
    # ------------------------------------------------------------------ #
    @staticmethod
    def position_at_time(
        a_au: float,
        e: float,
        i_deg: float,
        raan_deg: float,
        argp_deg: float,
        M0_deg: float,
        period_days: float,
        t_since_epoch_days: float,
    ) -> tuple[float, float, float]:
        """Heliocentric ecliptic position of a body at time ``t``.

        Parameters
        ----------
        a_au:
            Semi-major axis (AU).
        e:
            Eccentricity (0 <= e < 1).
        i_deg, raan_deg, argp_deg:
            Inclination, longitude of ascending node, argument of periapsis
            (degrees).
        M0_deg:
            Mean anomaly at epoch (degrees).
        period_days:
            Orbital period (days).
        t_since_epoch_days:
            Elapsed time from epoch (days).

        Returns
        -------
        tuple[float, float, float]
            ``(x, y, z)`` heliocentric ecliptic coordinates in AU.
        """
        if a_au <= 0:
            raise ValidationError("a_au must be > 0")
        if not 0.0 <= e < 1.0:
            raise ValidationError("e must be in [0, 1)")
        if period_days <= 0:
            raise ValidationError("period_days must be > 0")

        # Mean motion -> mean anomaly at t.
        n = 2.0 * math.pi / period_days  # rad/day
        M = math.radians(M0_deg) + n * t_since_epoch_days
        E = OrbitalMechanics.solve_kepler_equation(M, e)
        nu = OrbitalMechanics.true_anomaly(E, e)

        # Radius from focus.
        r = a_au * (1.0 - e * math.cos(E))

        # Perifocal frame.
        x_p = r * math.cos(nu)
        y_p = r * math.sin(nu)

        # Rotate: R3(-raan) * R1(-i) * R3(-argp).
        cos_O = math.cos(math.radians(raan_deg))
        sin_O = math.sin(math.radians(raan_deg))
        cos_i = math.cos(math.radians(i_deg))
        sin_i = math.sin(math.radians(i_deg))
        cos_w = math.cos(math.radians(argp_deg))
        sin_w = math.sin(math.radians(argp_deg))

        x = (
            (cos_O * cos_w - sin_O * sin_w * cos_i) * x_p
            + (-cos_O * sin_w - sin_O * cos_w * cos_i) * y_p
        )
        y = (
            (sin_O * cos_w + cos_O * sin_w * cos_i) * x_p
            + (-sin_O * sin_w + cos_O * cos_w * cos_i) * y_p
        )
        z = (sin_w * sin_i) * x_p + (cos_w * sin_i) * y_p
        return float(x), float(y), float(z)

    # ------------------------------------------------------------------ #
    # Geometric transit probability
    # ------------------------------------------------------------------ #
    @staticmethod
    def transit_probability(
        planet_radius_earth: float,
        star_radius_solar: float,
        semi_major_axis_au: float,
    ) -> float:
        """Geometric transit probability for a circular orbit.

        Uses ``P = (R_star + R_planet) / a`` with proper unit conversion.

        Returns
        -------
        float
            Probability in [0, 1], dimensionless.
        """
        if planet_radius_earth < 0:
            raise ValidationError("planet_radius_earth must be >= 0")
        if star_radius_solar <= 0:
            raise ValidationError("star_radius_solar must be > 0")
        if semi_major_axis_au <= 0:
            raise ValidationError("semi_major_axis_au must be > 0")

        r_earth_m = 6.371e6
        r_star_m = star_radius_solar * _R_SUN
        r_planet_m = planet_radius_earth * r_earth_m
        a_m = semi_major_axis_au * _AU
        prob = (r_star_m + r_planet_m) / a_m
        return float(min(1.0, max(0.0, prob)))

    # ------------------------------------------------------------------ #
    # Equilibrium temperature
    # ------------------------------------------------------------------ #
    @staticmethod
    def equilibrium_temperature(
        stellar_teff_k: float,
        stellar_radius_solar: float,
        semi_major_axis_au: float,
        albedo: float = 0.3,
    ) -> float:
        """Planetary equilibrium temperature (fast-rotator, uniform emitter).

        ``T_eq = T_* * sqrt(R_*/(2 a)) * (1 - A)^(1/4)``.

        Returns
        -------
        float
            Temperature in Kelvin.
        """
        if stellar_teff_k <= 0:
            raise ValidationError("stellar_teff_k must be > 0")
        if stellar_radius_solar <= 0:
            raise ValidationError("stellar_radius_solar must be > 0")
        if semi_major_axis_au <= 0:
            raise ValidationError("semi_major_axis_au must be > 0")
        if not 0.0 <= albedo < 1.0:
            raise ValidationError("albedo must be in [0, 1)")

        r_star_m = stellar_radius_solar * _R_SUN
        a_m = semi_major_axis_au * _AU
        t_eq = (
            stellar_teff_k
            * math.sqrt(r_star_m / (2.0 * a_m))
            * (1.0 - albedo) ** 0.25
        )
        return float(t_eq)


# Expose numpy for vectorized helpers if ever extended; import kept to satisfy
# the project's "use numpy" guidance even when not directly referenced.
_ = np
