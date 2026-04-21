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


# Gaia DR3 splits astrophysics across two tables:
#   * ``gaiadr3.gaia_source`` — astrometry + (in DR3) a subset of GSP-Phot
#     parameters including ``teff_gspphot`` and ``mh_gspphot``.
#   * ``gaiadr3.astrophysical_parameters`` — full GSP-Phot output, including
#     ``radius_gspphot`` which is absent from ``gaia_source``.
# We LEFT JOIN so we still get a row when the AP table has no entry.
_SOURCE_COLUMNS: tuple[str, ...] = (
    "s.source_id",
    "s.ra",
    "s.dec",
    "s.parallax",
    "s.teff_gspphot",
    "s.mh_gspphot",
)
_AP_COLUMNS: tuple[str, ...] = ("ap.radius_gspphot",)
_COLUMNS: tuple[str, ...] = _SOURCE_COLUMNS + _AP_COLUMNS
# Stable short names used by the row parser (strip the ``s.`` / ``ap.`` alias).
_COLUMN_NAMES: tuple[str, ...] = tuple(c.split(".", 1)[1] for c in _COLUMNS)
_FROM_JOIN: str = (
    "gaiadr3.gaia_source AS s "
    "LEFT JOIN gaiadr3.astrophysical_parameters AS ap "
    "ON s.source_id = ap.source_id"
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
                f"SELECT TOP 1 {cols} FROM {_FROM_JOIN} "
                f"WHERE s.source_id = {int(source_id)}"
            )
            identifier = f"Gaia DR3 {source_id}"
        else:
            assert name is not None
            safe = _escape_adql(name.strip())
            adql = (
                f"SELECT TOP 1 {cols} FROM {_FROM_JOIN} "
                f"WHERE s.designation = '{safe}'"
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
        Otherwise SIMBAD is consulted to resolve the name to either a Gaia
        DR3 source_id or sky coordinates, and that result drives the Gaia
        query. Falls back to the old designation-match behaviour if SIMBAD
        is unreachable.
        """
        ident = identifier.strip()
        if ident.isdigit():
            return self.query_star(source_id=int(ident))
        return self.resolve_by_name(ident)

    def resolve_by_name(self, name: str, cone_radius_arcsec: float = 5.0) -> Star:
        """Look up a star by a common name using SIMBAD → Gaia DR3.

        Strategy:
          1. Query SIMBAD. If it returns a ``Gaia DR3 <id>`` cross-identifier,
             fetch that row directly from Gaia.
          2. Otherwise, if SIMBAD returned coordinates, run a small Gaia
             cone search and return the nearest star.
          3. If SIMBAD itself is unreachable, fall back to the existing
             designation-based query as a last resort.

        Args:
            name: Object name resolvable by SIMBAD (e.g. ``"Kepler-10"``).
            cone_radius_arcsec: Cone radius when coordinate fallback kicks in.

        Returns:
            The matching :class:`Star`, with ``identifier`` set to ``name``
            so the planet's ``host_star`` string lines up.

        Raises:
            ValidationError: If the name is empty.
            DataSourceNotFoundError: If neither SIMBAD nor Gaia have a match.
            DataSourceUnavailableError: If both SIMBAD and Gaia are unreachable.
        """
        # Local import to avoid a cycle (simbad module also imports from here
        # transitively via `base`).
        from .simbad import SimbadClient

        log.debug("gaia.resolve_by_name.start", name=name)

        star: Star | None = None
        simbad_unavailable = False
        try:
            resolved = SimbadClient().resolve(name)
        except DataSourceNotFoundError:
            log.warning("gaia.resolve_by_name.simbad_miss", name=name)
            resolved = None
        except DataSourceUnavailableError as e:
            log.warning("gaia.resolve_by_name.simbad_down", name=name, error=str(e))
            resolved = None
            simbad_unavailable = True

        if resolved is not None:
            gaia_id = resolved.gaia_dr3_source_id or resolved.gaia_dr2_source_id
            if gaia_id is not None:
                try:
                    star = self.query_star(source_id=int(gaia_id))
                except DataSourceNotFoundError:
                    log.info(
                        "gaia.resolve_by_name.dr3_miss_fallback_cone",
                        name=name,
                        gaia_id=gaia_id,
                    )

            if star is None and resolved.ra_deg is not None and resolved.dec_deg is not None:
                matches = self.cone_search(
                    ra_deg=resolved.ra_deg,
                    dec_deg=resolved.dec_deg,
                    radius_arcsec=cone_radius_arcsec,
                    limit=1,
                )
                if matches:
                    star = matches[0]

        if star is None and simbad_unavailable:
            # Last-ditch fallback: try matching the name directly on Gaia.
            try:
                star = self.query_star(name=name)
            except DataSourceNotFoundError:
                pass

        if star is None:
            raise DataSourceNotFoundError(
                f"Could not resolve {name!r} via SIMBAD or Gaia"
            )

        # Re-stamp the identifier so DB lookups by planet.host_star succeed.
        renamed = star.model_copy(update={"identifier": name})
        log.info(
            "gaia.resolve_by_name.done",
            name=name,
            gaia_source=star.identifier,
            teff=renamed.effective_temperature_k,
        )
        return renamed

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
            f"DISTANCE(POINT('ICRS', s.ra, s.dec), "
            f"POINT('ICRS', {ra_deg}, {dec_deg})) AS d "
            f"FROM {_FROM_JOIN} "
            f"WHERE 1=CONTAINS(POINT('ICRS', s.ra, s.dec), "
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
