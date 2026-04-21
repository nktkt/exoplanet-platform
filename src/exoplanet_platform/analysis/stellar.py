"""Stellar property derivation: luminosity, spectral type, lifetime, enrichment."""

from __future__ import annotations

import math

from exoplanet_platform.domain import Star
from exoplanet_platform.exceptions import ValidationError
from exoplanet_platform.logging_config import get_logger

logger = get_logger(__name__)

# Solar reference constants (SI).
_SIGMA_SB = 5.670374419e-8  # W m^-2 K^-4
_L_SUN = 3.828e26  # W
_R_SUN = 6.957e8  # m
_T_SUN = 5772.0  # K

# Harvard spectral classification cutoffs (K). Lower bound inclusive.
# Source: standard textbook (Carroll & Ostlie 2007, Table 8.1) rounded.
_SPECTRAL_TABLE: tuple[tuple[float, str], ...] = (
    (30000.0, "O"),
    (10000.0, "B"),
    (7500.0, "A"),
    (6000.0, "F"),
    (5200.0, "G"),
    (3700.0, "K"),
    (0.0, "M"),
)


class StellarAnalyzer:
    """Derivations of stellar quantities from sparse catalog data."""

    # ------------------------------------------------------------------ #
    # Luminosity
    # ------------------------------------------------------------------ #
    def luminosity(self, star: Star) -> float | None:
        """Bolometric luminosity from Stefan-Boltzmann, if derivable.

        Returns
        -------
        float | None
            Luminosity in solar units (dimensionless, ``L / L_sun``), or
            ``None`` if radius or effective temperature are missing.
        """
        logger.debug("luminosity.start", star=star.identifier)
        if star.radius_solar is None or star.effective_temperature_k is None:
            return None
        if star.radius_solar <= 0 or star.effective_temperature_k <= 0:
            return None
        r_m = float(star.radius_solar) * _R_SUN
        teff = float(star.effective_temperature_k)
        l_watt = 4.0 * math.pi * r_m**2 * _SIGMA_SB * teff**4
        l_solar = l_watt / _L_SUN
        logger.info(
            "luminosity.done", star=star.identifier, luminosity_solar=l_solar
        )
        return float(l_solar)

    # ------------------------------------------------------------------ #
    # Spectral type
    # ------------------------------------------------------------------ #
    @staticmethod
    def classify_spectral_type(teff_k: float) -> str:
        """Rough Harvard spectral type from effective temperature.

        Returns
        -------
        str
            One of ``"O" | "B" | "A" | "F" | "G" | "K" | "M"``.
        """
        if teff_k <= 0:
            raise ValidationError("teff_k must be > 0")
        for threshold, label in _SPECTRAL_TABLE:
            if teff_k >= threshold:
                return label
        return "M"  # pragma: no cover - unreachable given table terminator

    # ------------------------------------------------------------------ #
    # Main-sequence lifetime
    # ------------------------------------------------------------------ #
    @staticmethod
    def main_sequence_lifetime_gyr(mass_solar: float) -> float:
        """Approximate main-sequence lifetime from stellar mass.

        Uses the textbook scaling ``t_MS ~ 10 * M^-2.5 Gyr`` normalised to
        the Sun at 10 Gyr.

        Returns
        -------
        float
            Lifetime in gigayears (Gyr).
        """
        if mass_solar <= 0:
            raise ValidationError("mass_solar must be > 0")
        return float(10.0 * mass_solar ** (-2.5))

    # ------------------------------------------------------------------ #
    # Enrichment
    # ------------------------------------------------------------------ #
    def enrich(self, star: Star) -> Star:
        """Return a copy of ``star`` with derivable fields filled in.

        Currently fills ``luminosity_solar`` (Stefan-Boltzmann) and
        ``spectral_type`` (Teff classification) when missing. Existing
        fields are never overwritten.

        Returns
        -------
        Star
            New ``Star`` instance; input is unchanged (models are frozen).
        """
        logger.debug("stellar.enrich.start", star=star.identifier)
        updates: dict[str, object] = {}

        if star.luminosity_solar is None:
            lum = self.luminosity(star)
            if lum is not None:
                updates["luminosity_solar"] = lum

        if star.spectral_type is None and star.effective_temperature_k is not None:
            try:
                updates["spectral_type"] = self.classify_spectral_type(
                    float(star.effective_temperature_k)
                )
            except ValidationError:
                pass

        if not updates:
            return star

        logger.info(
            "stellar.enrich.done",
            star=star.identifier,
            filled=list(updates.keys()),
        )
        return star.model_copy(update=updates)
