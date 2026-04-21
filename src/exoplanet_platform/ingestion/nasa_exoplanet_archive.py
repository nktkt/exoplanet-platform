"""Client for the NASA Exoplanet Archive TAP service.

Uses the synchronous TAP endpoint and the ``pscomppars`` planetary systems
composite parameters table to retrieve confirmed/candidate planet records.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import get_settings
from ..domain import Catalog, DetectionMethod, Planet
from ..exceptions import (
    DataSourceNotFoundError,
    DataSourceUnavailableError,
    ValidationError,
)
from ..http import get_http_client
from ..logging_config import get_logger
from .base import DataSourceClient

log = get_logger(__name__)


# pscomppars columns we care about. Order matters because the order we select
# controls the order of keys in the returned JSON.
_COLUMNS: tuple[str, ...] = (
    "pl_name",
    "hostname",
    "discoverymethod",
    "disc_year",
    "pl_orbper",
    "pl_orbsmax",
    "pl_orbeccen",
    "pl_orbincl",
    "pl_rade",
    "pl_bmasse",
    "pl_dens",
    "pl_eqt",
    "pl_insol",
    "pl_tranmid",
    "pl_trandur",
    "pl_trandep",
)


# Mapping between NASA Exoplanet Archive discovery method strings and our enum.
_METHOD_MAP: dict[str, DetectionMethod] = {
    "transit": DetectionMethod.TRANSIT,
    "radial velocity": DetectionMethod.RADIAL_VELOCITY,
    "imaging": DetectionMethod.IMAGING,
    "microlensing": DetectionMethod.MICROLENSING,
    "astrometry": DetectionMethod.ASTROMETRY,
    "transit timing variations": DetectionMethod.TIMING,
    "eclipse timing variations": DetectionMethod.TIMING,
    "pulsar timing": DetectionMethod.TIMING,
    "pulsation timing variations": DetectionMethod.TIMING,
}


def _escape_adql(value: str) -> str:
    """Escape single quotes in a user-provided ADQL string literal.

    ADQL follows SQL-92 and requires doubling a single quote to embed it
    inside a string literal. This is our single line of defence against
    string-injection when building queries.
    """
    return value.replace("'", "''")


def _map_method(raw: str | None) -> DetectionMethod:
    """Map a NASA discovery method string to our ``DetectionMethod`` enum."""
    if not raw:
        return DetectionMethod.UNKNOWN
    return _METHOD_MAP.get(raw.strip().lower(), DetectionMethod.UNKNOWN)


def _to_float(value: Any) -> float | None:
    """Coerce a TAP JSON scalar to ``float``, returning None for missing values."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    """Coerce a TAP JSON scalar to ``int``, returning None for missing values."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class NASAExoplanetArchiveClient(DataSourceClient):
    """Fetch planet records from the NASA Exoplanet Archive TAP service.

    Queries the ``pscomppars`` (planetary systems composite parameters)
    table via the synchronous TAP endpoint and returns domain ``Planet``
    objects. Exposes a planet-name search and a single-planet fetch.
    """

    def __init__(self) -> None:
        settings = get_settings().data_sources
        self._base_url = settings.nasa_exoplanet_archive_url
        self._http = get_http_client()

    @property
    def name(self) -> Catalog:
        """Return the catalog identifier for this client."""
        return Catalog.NASA_EXOPLANET_ARCHIVE

    def health_check(self) -> bool:
        """Return True if the TAP endpoint responds to a trivial probe query."""
        log.debug("nasa_ea.health_check.start")
        try:
            rows = self._run_query("SELECT TOP 1 pl_name FROM pscomppars")
            return isinstance(rows, list)
        except Exception as e:  # noqa: BLE001 - health check must never raise
            log.warning("nasa_ea.health_check.failed", error=str(e))
            return False

    def search_planets(
        self,
        name: str | None = None,
        host_star: str | None = None,
        limit: int = 100,
    ) -> list[Planet]:
        """Search planets by name and/or host-star name.

        Args:
            name: Case-insensitive substring matched against ``pl_name``.
            host_star: Case-insensitive substring matched against ``hostname``.
            limit: Maximum number of rows to return. Must be between 1 and 10000.

        Returns:
            A list of ``Planet`` objects (possibly empty).

        Raises:
            ValidationError: If ``limit`` is not a positive integer <= 10000.
            DataSourceUnavailableError: If the TAP service is unreachable.
        """
        log.debug(
            "nasa_ea.search_planets.start",
            name=name,
            host_star=host_star,
            limit=limit,
        )
        if limit <= 0 or limit > 10_000:
            raise ValidationError(f"limit must be in (0, 10000], got {limit}")

        clauses: list[str] = []
        if name:
            clauses.append(f"UPPER(pl_name) LIKE UPPER('%{_escape_adql(name)}%')")
        if host_star:
            clauses.append(f"UPPER(hostname) LIKE UPPER('%{_escape_adql(host_star)}%')")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        cols = ",".join(_COLUMNS)
        adql = f"SELECT TOP {int(limit)} {cols} FROM pscomppars{where}"

        rows = self._run_query(adql)
        planets = [self._row_to_planet(r) for r in rows]
        log.info(
            "nasa_ea.search_planets.done",
            count=len(planets),
            name=name,
            host_star=host_star,
        )
        return planets

    def fetch_planet(self, identifier: str) -> Planet:
        """Alias of :meth:`get_planet` — accepted by API ingest routes."""
        return self.get_planet(identifier)

    def get_planet(self, name: str) -> Planet:
        """Fetch the composite record for a single planet by exact name.

        Args:
            name: The planet's ``pl_name`` (e.g. ``"Kepler-10 b"``).

        Returns:
            The matching ``Planet`` object.

        Raises:
            ValidationError: If ``name`` is empty.
            DataSourceNotFoundError: If no row matches ``name``.
            DataSourceUnavailableError: If the TAP service is unreachable.
        """
        log.debug("nasa_ea.get_planet.start", name=name)
        if not name or not name.strip():
            raise ValidationError("name must not be empty")
        safe = _escape_adql(name.strip())
        cols = ",".join(_COLUMNS)
        adql = f"SELECT TOP 1 {cols} FROM pscomppars WHERE pl_name = '{safe}'"
        rows = self._run_query(adql)
        if not rows:
            log.warning("nasa_ea.get_planet.not_found", name=name)
            raise DataSourceNotFoundError(f"Planet '{name}' not found in NASA EA")
        planet = self._row_to_planet(rows[0])
        log.info("nasa_ea.get_planet.done", name=planet.identifier)
        return planet

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_query(self, adql: str) -> list[dict[str, Any]]:
        """Execute an ADQL query against the TAP sync endpoint and return JSON rows."""
        params = {"query": adql, "format": "json"}
        log.debug("nasa_ea.tap.request", url=self._base_url, adql=adql)
        try:
            response = self._http.get(self._base_url, params=params)
        except (DataSourceUnavailableError, httpx.HTTPError) as e:
            log.warning("nasa_ea.tap.network_error", error=str(e))
            raise DataSourceUnavailableError(
                f"NASA Exoplanet Archive request failed: {e}"
            ) from e
        self._handle_response(response)
        try:
            payload = response.json()
        except ValueError as e:
            log.warning("nasa_ea.tap.bad_json", error=str(e))
            raise DataSourceUnavailableError(
                f"NASA EA returned non-JSON response: {e}"
            ) from e
        if not isinstance(payload, list):
            raise DataSourceUnavailableError(
                f"NASA EA returned unexpected payload type: {type(payload).__name__}"
            )
        return payload

    @staticmethod
    def _row_to_planet(row: dict[str, Any]) -> Planet:
        """Convert a single TAP JSON row into a ``Planet`` domain object."""
        trandep_pct = _to_float(row.get("pl_trandep"))
        trandep_ppm = trandep_pct * 10_000 if trandep_pct is not None else None

        return Planet(
            identifier=str(row.get("pl_name", "")).strip() or "unknown",
            host_star=str(row.get("hostname", "")).strip() or "unknown",
            discovery_method=_map_method(row.get("discoverymethod")),
            discovery_year=_to_int(row.get("disc_year")),
            orbital_period_days=_to_float(row.get("pl_orbper")),
            semi_major_axis_au=_to_float(row.get("pl_orbsmax")),
            eccentricity=_to_float(row.get("pl_orbeccen")),
            inclination_deg=_to_float(row.get("pl_orbincl")),
            radius_earth=_to_float(row.get("pl_rade")),
            mass_earth=_to_float(row.get("pl_bmasse")),
            density_g_cm3=_to_float(row.get("pl_dens")),
            equilibrium_temperature_k=_to_float(row.get("pl_eqt")),
            insolation_flux_earth=_to_float(row.get("pl_insol")),
            transit_epoch_bjd=_to_float(row.get("pl_tranmid")),
            transit_duration_hours=_to_float(row.get("pl_trandur")),
            transit_depth_ppm=trandep_ppm,
            catalog=Catalog.NASA_EXOPLANET_ARCHIVE,
            last_updated=datetime.now(timezone.utc),
        )
