"""Analysis sub-package: transit detection, orbital mechanics, habitability, stellar enrichment.

All public classes are re-exported here so callers can write
``from exoplanet_platform.analysis import TransitDetector`` without caring
which module the implementation lives in.
"""

from __future__ import annotations

from exoplanet_platform.analysis.habitability import HabitabilityAnalyzer
from exoplanet_platform.analysis.orbital import OrbitalMechanics
from exoplanet_platform.analysis.stellar import StellarAnalyzer
from exoplanet_platform.analysis.transit import TransitDetector

__all__ = [
    "HabitabilityAnalyzer",
    "OrbitalMechanics",
    "StellarAnalyzer",
    "TransitDetector",
]
