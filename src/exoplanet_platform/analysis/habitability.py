"""Habitable-zone bounds and Earth Similarity Index computation.

Implements Kopparapu et al. (2013) habitable-zone stellar-flux coefficients
and the Earth Similarity Index (ESI) of Schulze-Makuch et al. (2011).
"""

from __future__ import annotations

import math

from exoplanet_platform.analysis.orbital import OrbitalMechanics
from exoplanet_platform.analysis.stellar import StellarAnalyzer
from exoplanet_platform.domain import HabitabilityAssessment, Planet, Star
from exoplanet_platform.exceptions import InsufficientDataError
from exoplanet_platform.logging_config import get_logger

logger = get_logger(__name__)

# Kopparapu et al. 2013 stellar-flux coefficients. Seff = S0 + a*T + b*T^2
# + c*T^3 + d*T^4 with T = Teff - 5780 K. Columns: (S0, a, b, c, d).
# Rows: Recent Venus, Runaway Greenhouse, Maximum Greenhouse, Early Mars.
_KOPPARAPU_COEFFS: dict[str, tuple[float, float, float, float, float]] = {
    "recent_venus": (1.7753, 1.4316e-4, 2.9875e-9, -7.5702e-12, -1.1635e-15),
    "runaway_greenhouse": (1.0512, 1.3242e-4, 1.5418e-8, -7.9895e-12, -1.8328e-15),
    "maximum_greenhouse": (0.3438, 5.8942e-5, 1.6558e-9, -3.0045e-12, -5.2983e-16),
    "early_mars": (0.3179, 5.4513e-5, 1.5313e-9, -2.7786e-12, -4.8997e-16),
}

_TEFF_VALID_MIN = 2600.0
_TEFF_VALID_MAX = 7200.0


def _seff(teff_k: float, key: str) -> float:
    """Evaluate Kopparapu polynomial at ``teff_k``."""
    s0, a, b, c, d = _KOPPARAPU_COEFFS[key]
    t = teff_k - 5780.0
    return s0 + a * t + b * t**2 + c * t**3 + d * t**4


class HabitabilityAnalyzer:
    """Compute habitable-zone bounds and score planets against them."""

    def __init__(self) -> None:
        self._stellar = StellarAnalyzer()

    # ------------------------------------------------------------------ #
    # Habitable zone bounds
    # ------------------------------------------------------------------ #
    def habitable_zone(self, star: Star) -> tuple[float, float, float, float]:
        """Compute habitable-zone inner/outer bounds for ``star``.

        Uses Kopparapu et al. 2013 coefficients; the conservative HZ runs
        from Runaway Greenhouse to Maximum Greenhouse, and the optimistic
        HZ from Recent Venus to Early Mars.

        Returns
        -------
        tuple[float, float, float, float]
            ``(conservative_inner_au, conservative_outer_au,
            optimistic_inner_au, optimistic_outer_au)``.
        """
        logger.debug("hz.start", star=star.identifier)
        if star.effective_temperature_k is None:
            raise InsufficientDataError(
                f"Star {star.identifier!r} is missing effective_temperature_k"
            )

        teff = float(star.effective_temperature_k)
        if not _TEFF_VALID_MIN <= teff <= _TEFF_VALID_MAX:
            logger.info(
                "hz.teff_out_of_range",
                star=star.identifier,
                teff=teff,
                valid_range=(_TEFF_VALID_MIN, _TEFF_VALID_MAX),
            )

        luminosity = star.luminosity_solar
        if luminosity is None:
            luminosity = self._stellar.luminosity(star)
        if luminosity is None or luminosity <= 0:
            raise InsufficientDataError(
                f"Star {star.identifier!r} needs luminosity_solar or "
                "radius_solar+effective_temperature_k to derive it"
            )

        def _au(key: str) -> float:
            s = _seff(teff, key)
            if s <= 0:
                raise InsufficientDataError(
                    f"Non-positive Seff for {key} at Teff={teff} K"
                )
            return math.sqrt(float(luminosity) / s)

        optimistic_inner = _au("recent_venus")
        conservative_inner = _au("runaway_greenhouse")
        conservative_outer = _au("maximum_greenhouse")
        optimistic_outer = _au("early_mars")

        logger.info(
            "hz.done",
            star=star.identifier,
            teff=teff,
            luminosity=luminosity,
            conservative=(conservative_inner, conservative_outer),
            optimistic=(optimistic_inner, optimistic_outer),
        )
        return (
            conservative_inner,
            conservative_outer,
            optimistic_inner,
            optimistic_outer,
        )

    # ------------------------------------------------------------------ #
    # Assessment
    # ------------------------------------------------------------------ #
    def assess(self, planet: Planet, star: Star) -> HabitabilityAssessment:
        """Score a planet against its host star's habitable zone.

        Fills in ``in_conservative_hz``, ``in_optimistic_hz``, HZ bounds
        (optimistic extremes), equilibrium temperature (K), and Earth
        Similarity Index (dimensionless, 0..1) when enough data is present.

        Returns
        -------
        HabitabilityAssessment
            Populated assessment record.
        """
        logger.debug("hab.assess.start", planet=planet.identifier)
        if planet.semi_major_axis_au is None:
            raise InsufficientDataError(
                f"Planet {planet.identifier!r} is missing semi_major_axis_au"
            )

        cons_in, cons_out, opt_in, opt_out = self.habitable_zone(star)
        a = float(planet.semi_major_axis_au)
        in_cons = cons_in <= a <= cons_out
        in_opt = opt_in <= a <= opt_out

        # Equilibrium temperature: prefer explicit value, else derive.
        t_eq: float | None = planet.equilibrium_temperature_k
        if (
            t_eq is None
            and star.effective_temperature_k is not None
            and star.radius_solar is not None
        ):
            try:
                t_eq = OrbitalMechanics.equilibrium_temperature(
                    stellar_teff_k=float(star.effective_temperature_k),
                    stellar_radius_solar=float(star.radius_solar),
                    semi_major_axis_au=a,
                )
            except Exception:  # pragma: no cover - defensive
                t_eq = None

        esi = self._earth_similarity_index(planet, t_eq)

        notes_parts: list[str] = []
        if not in_opt:
            notes_parts.append("outside optimistic HZ")
        elif not in_cons:
            notes_parts.append("within optimistic HZ only")
        else:
            notes_parts.append("within conservative HZ")
        notes = "; ".join(notes_parts) if notes_parts else None

        assessment = HabitabilityAssessment(
            planet=planet.identifier,
            in_conservative_hz=bool(in_cons),
            in_optimistic_hz=bool(in_opt),
            hz_inner_au=float(opt_in),
            hz_outer_au=float(opt_out),
            earth_similarity_index=esi,
            equilibrium_temperature_k=float(t_eq) if t_eq is not None else None,
            notes=notes,
        )
        logger.info(
            "hab.assess.done",
            planet=planet.identifier,
            in_conservative=in_cons,
            in_optimistic=in_opt,
            esi=esi,
            t_eq=t_eq,
        )
        return assessment

    # ------------------------------------------------------------------ #
    # Earth Similarity Index
    # ------------------------------------------------------------------ #
    @staticmethod
    def _earth_similarity_index(
        planet: Planet, equilibrium_temperature_k: float | None
    ) -> float | None:
        """Compute ESI (Schulze-Makuch et al. 2011).

        ESI_i = (1 - |(x_i - x_E)/(x_i + x_E)|) ^ (w_i / n) per term, and
        the full ESI is the geometric mean over available terms. Terms we
        consider: radius, density, escape velocity, equilibrium temperature.
        Missing inputs are simply dropped.

        Returns
        -------
        float | None
            ESI in [0, 1] (1 = identical to Earth), or ``None`` if no
            component could be evaluated.
        """
        # Earth reference values.
        R_E = 1.0  # Earth radii
        RHO_E = 5.514  # g/cm^3
        V_ESC_E = 11.186  # km/s
        T_EQ_E = 288.0  # K

        # Weights (exponents) from Schulze-Makuch 2011 Table 2, interior ESI
        # split; we combine into one overall ESI.
        weights: dict[str, float] = {
            "radius": 0.57,
            "density": 1.07,
            "v_esc": 0.70,
            "t_eq": 5.58,
        }

        components: list[tuple[str, float, float]] = []
        if planet.radius_earth is not None and planet.radius_earth > 0:
            components.append(("radius", float(planet.radius_earth), R_E))
        if planet.density_g_cm3 is not None and planet.density_g_cm3 > 0:
            components.append(("density", float(planet.density_g_cm3), RHO_E))

        # Escape velocity requires mass+radius.
        if (
            planet.mass_earth is not None
            and planet.radius_earth is not None
            and planet.mass_earth > 0
            and planet.radius_earth > 0
        ):
            v_esc = V_ESC_E * math.sqrt(
                float(planet.mass_earth) / float(planet.radius_earth)
            )
            components.append(("v_esc", v_esc, V_ESC_E))

        if equilibrium_temperature_k is not None and equilibrium_temperature_k > 0:
            components.append(("t_eq", float(equilibrium_temperature_k), T_EQ_E))

        if not components:
            return None

        # Use weights directly as exponents; normalise by sum of weights used
        # so the overall ESI stays in [0, 1].
        total_weight = sum(weights[name] for name, _, _ in components)
        esi = 1.0
        for name, xi, xe in components:
            denom = xi + xe
            if denom <= 0:
                continue
            frac = abs((xi - xe) / denom)
            base = max(0.0, 1.0 - frac)
            esi *= base ** (weights[name] / total_weight)
        return float(max(0.0, min(1.0, esi)))
