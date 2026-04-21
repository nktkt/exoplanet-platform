"""Client for Gaia DR3 queries via ``astroquery.gaia``.

Provides stellar lookups by Gaia source_id or resolved name, plus cone
searches. Results are translated to domain ``Star`` objects.
"""

from __future__ import annotations

from typing import Any

from ..domain import Catalog, Star
from ..exceptions import (
    DataSourceNotFoundError,
    DataSourceUnavailableError,
    ValidationError,
)
from ..logging_config import get_logger
from .base import DataSourceClient

log = get_logger(__name__)


# ADQL fragments for the columns we care about. Kept as a tuple so the
# select-list order matches the ordering used by the row parser.
_COLUMNS: tuple[str, ...] = (
    "source_id",
    "ra",
    "dec",
    "parallax",
    "teff_gspphot",
    "radius_gspphot",
    "mh_gspphot",
)


def _escape_adql(value: str) -> str:
    """Escape single quotes in a user-supplied ADQL string literal."""
    return value.replace("'", "''")


def _to_float(value: Any) -> float | None:
    """Coerce astropy/numpy scalars (or None/masked) to Python float or None."""
    if value is None:
        return None
    if getattr(value, "mask", False):
        return None
    try:
        if hasattr(value, "item"):
            value = value.item()
    except Exception:
        pass
    try:
        fv = float(value)
    except (TypeError, ValueError):
        return None
    # astropy may surface NaN for masked values that weren't flagged as masked
    import math

    if math.isnan(fv):
        return None
    return fv


class GaiaClient(DataSourceClient):
    """Query the Gaia DR3 catalog for stellar astrometry and astrophysics."""

    @property
    def name(self) -> Catalog:
        """Return the catalog identifier for this client."""
        return Catalog.GAIA

    def health_check(self) -> bool:
        """Return True if a trivial ADQL probe against Gaia succeeds."""
        log.debug("gaia.health_check.start")
        try:
            from astroquery.gaia import Gaia

            job = Gaia.launch_job("SELECT TOP 1 source_id FROM gaiadr3.gaia_source")
            _ = job.get_results()
            return True
        except Exception as e:
            log.warning("gaia.health_check.failed", error=str(e))
            return False

    def query_star(
        self,
        source_id: int | None = None,
        name: str | None = None,
    ) -> Star:
        """Fetch a single star by Gaia ``source_id`` or a resolved name.

        Args:
            source_id: Gaia DR3 source_id (preferred when known).
            name: A name resolvable via Gaia's ADQL resolver (exact match
                against ``gaia_source.designation``).

        Returns:
            A ``Star`` domain object.

        Raises:
            ValidationError: If neither ``source_id`` nor ``name`` is given.
            DataSourceNotFoundError: If no matching record is found.
            DataSourceUnavailableError: If Gaia is unreachable.
        """
        log.debug("gaia.query_star.start", source_id=source_id, name=name)
        if source_id is None and not name:
            raise ValidationError("Provide either source_id or name")

        cols = ",".join(_COLUMNS)
        if source_id is not None:
            adql = (
                f"SELECT TOP 1 {cols} FROM gaiadr3.gaia_source "
                f"WHERE source_id = {int(source_id)}"
            )
            identifier = f"Gaia DR3 {source_id}"
        else:
            assert name is not None
            safe = _escape_adql(name.strip())
            adql = (
                f"SELECT TOP 1 {cols} FROM gaiadr3.gaia_source "
                f"WHERE designation = '{safe}'"
            )
            identifier = name.strip()

        rows = self._run_query(adql)
        if not rows:
            log.warning("gaia.query_star.not_found", source_id=source_id, name=name)
            raise DataSourceNotFoundError(
                f"No Gaia DR3 star found for identifier {identifier!r}"
            )
        star = self._row_to_star(rows[0], fallback_identifier=identifier)
        log.info("gaia.query_star.done", identifier=star.identifier)
        return star

    def fetch_star(self, identifier: str) -> Star:
        """Alias of :meth:`get_star` — accepted by API ingest routes."""
        return self.get_star(identifier)

    def get_star(self, identifier: str) -> Star:
        """Fetch a star by a human-readable identifier.

        If ``identifier`` is numeric, it is treated as a Gaia DR3 source_id.
        Otherwise it is passed to ``query_star(name=...)`` as a designation.
        """
        ident = identifier.strip()
        if ident.isdigit():
            return self.query_star(source_id=int(ident))
        return self.query_star(name=ident)

    def cone_search(
        self,
        ra_deg: float,
        dec_deg: float,
        radius_arcsec: float,
        limit: int = 50,
    ) -> list[Star]:
        """Return Gaia DR3 stars within ``radius_arcsec`` of (RA, Dec).

        Args:
            ra_deg: Right ascension of the cone center, in degrees.
            dec_deg: Declination of the cone center, in degrees.
            radius_arcsec: Cone radius, in arcseconds.
            limit: Maximum rows to return. Must be between 1 and 10000.

        Returns:
            A list of ``Star`` objects (possibly empty), nearest first.

        Raises:
            ValidationError: If inputs are out of valid ranges.
            DataSourceUnavailableError: If the Gaia TAP service is unreachable.
        """
        log.debug(
            "gaia.cone_search.start",
            ra_deg=ra_deg,
            dec_deg=dec_deg,
            radius_arcsec=radius_arcsec,
            limit=limit,
        )
        if not (0.0 <= ra_deg <= 360.0):
            raise ValidationError(f"ra_deg must be in [0, 360], got {ra_deg}")
        if not (-90.0 <= dec_deg <= 90.0):
            raise ValidationError(f"dec_deg must be in [-90, 90], got {dec_deg}")
        if radius_arcsec <= 0 or radius_arcsec > 3600.0:
            raise ValidationError(
                f"radius_arcsec must be in (0, 3600], got {radius_arcsec}"
            )
        if limit <= 0 or limit > 10_000:
            raise ValidationError(f"limit must be in (0, 10000], got {limit}")

        radius_deg = radius_arcsec / 3600.0
        cols = ",".join(_COLUMNS)
        adql = (
            f"SELECT TOP {int(limit)} {cols}, "
            f"DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', {ra_deg}, {dec_deg})) AS d "
            f"FROM gaiadr3.gaia_source "
            f"WHERE 1=CONTAINS(POINT('ICRS', ra, dec), "
            f"CIRCLE('ICRS', {ra_deg}, {dec_deg}, {radius_deg})) "
            f"ORDER BY d ASC"
        )

        rows = self._run_query(adql)
        stars = [self._row_to_star(r) for r in rows]
        log.info(
            "gaia.cone_search.done",
            ra_deg=ra_deg,
            dec_deg=dec_deg,
            radius_arcsec=radius_arcsec,
            count=len(stars),
        )
        return stars

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_query(self, adql: str) -> list[dict[str, Any]]:
        """Execute an ADQL query via astroquery.gaia and return row dicts."""
        try:
            from astroquery.gaia import Gaia
        except ImportError as e:
            raise DataSourceUnavailableError(
                f"astroquery is required for Gaia access: {e}"
            ) from e

        log.debug("gaia.tap.request", adql=adql)
        try:
            job = Gaia.launch_job(adql)
            table = job.get_results()
        except Exception as e:
            log.warning("gaia.tap.failed", error=str(e))
            raise DataSourceUnavailableError(
                f"Gaia ADQL request failed: {e}"
            ) from e

        rows: list[dict[str, Any]] = []
        if table is None:
            return rows
        for r in table:
            row: dict[str, Any] = {}
            for col in table.colnames:
                row[col] = r[col]
            rows.append(row)
        return rows

    @staticmethod
    def _row_to_star(
        row: dict[str, Any],
        fallback_identifier: str | None = None,
    ) -> Star:
        """Convert a Gaia ADQL row dict into a ``Star`` domain object."""
        source_id = row.get("source_id")
        if source_id is not None and hasattr(source_id, "item"):
            source_id = source_id.item()
        if source_id is not None:
            identifier = f"Gaia DR3 {int(source_id)}"
        else:
            identifier = fallback_identifier or "Gaia DR3 unknown"

        parallax = _to_float(row.get("parallax"))
        distance_pc: float | None = None
        if parallax is not None and parallax > 0:
            distance_pc = 1000.0 / parallax

        return Star(
            identifier=identifier,
            ra_deg=_to_float(row.get("ra")),
            dec_deg=_to_float(row.get("dec")),
            distance_pc=distance_pc,
            effective_temperature_k=_to_float(row.get("teff_gspphot")),
            radius_solar=_to_float(row.get("radius_gspphot")),
            mass_solar=None,
            luminosity_solar=None,
            metallicity_dex=_to_float(row.get("mh_gspphot")),
            spectral_type=None,
            catalog=Catalog.GAIA,
        )
