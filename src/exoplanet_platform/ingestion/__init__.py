"""Ingestion sub-package.

Provides clients for external astronomical data sources. Each client
inherits from :class:`DataSourceClient` (see :mod:`.base`) so callers
can use them uniformly.
"""

from __future__ import annotations

from .gaia import GaiaClient
from .jpl_horizons import JPLHorizonsClient
from .mast import MASTClient
from .nasa_exoplanet_archive import NASAExoplanetArchiveClient
from .simbad import ResolvedObject, SimbadClient

__all__ = [
    "GaiaClient",
    "JPLHorizonsClient",
    "MASTClient",
    "NASAExoplanetArchiveClient",
    "ResolvedObject",
    "SimbadClient",
]
