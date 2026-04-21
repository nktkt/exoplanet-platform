"""Client for the CDS SIMBAD name resolver via ``astroquery.simbad``.

SIMBAD is the canonical object-name → coordinates + cross-identifier service
for stars. We use it purely for resolution: given a name like ``"Kepler-10"``,
it returns sky coordinates and a dictionary of alternate identifiers (Gaia
DR3 source_id, HD number, TYC, 2MASS, etc.).

The actual stellar astrophysics (Teff, radius, ...) is then fetched from
Gaia using the resolved source_id by :class:`GaiaClient`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ..domain import Catalog
from ..exceptions import (
    DataSourceNotFoundError,
    DataSourceUnavailableError,
    ValidationError,
)
from ..logging_config import get_logger
from .base import DataSourceClient

log = get_logger(__name__)


# Match either "Gaia DR3 <digits>" or "Gaia DR2 <digits>" (DR3 preferred).
_GAIA_DR3_RE = re.compile(r"Gaia\s+DR3\s+(\d+)", re.IGNORECASE)
_GAIA_DR2_RE = re.compile(r"Gaia\s+DR2\s+(\d+)", re.IGNORECASE)


@dataclass(frozen=True)
class ResolvedObject:
    """What SIMBAD tells us about a given name.

    ``identifiers`` is a mapping from a cross-catalog prefix (e.g. ``"Gaia DR3"``,
    ``"HD"``, ``"2MASS"``, ``"TIC"``) to the catalog-specific identifier string.
    """

    name: str
    ra_deg: float | None
    dec_deg: float | None
    identifiers: dict[str, str]

    @property
    def gaia_dr3_source_id(self) -> int | None:
        """Return the Gaia DR3 source_id if SIMBAD cross-listed one."""
        for ident in self.identifiers.values():
            m = _GAIA_DR3_RE.search(ident)
            if m:
                return int(m.group(1))
        return None

    @property
    def gaia_dr2_source_id(self) -> int | None:
        """Return the Gaia DR2 source_id (fallback when DR3 is missing)."""
        for ident in self.identifiers.values():
            m = _GAIA_DR2_RE.search(ident)
            if m:
                return int(m.group(1))
        return None


def _prefix_of(identifier: str) -> str:
    """Extract a catalog prefix from an identifier, e.g. 'HD 209458' → 'HD'."""
    ident = identifier.strip()
    parts = ident.split(None, 1)
    if len(parts) == 1:
        return ident
    # 'Gaia DR3 123' has two-word prefix.
    if parts[0].lower() == "gaia":
        sub = parts[1].split(None, 1)
        if sub and sub[0].upper() in {"DR1", "DR2", "DR3"}:
            return f"Gaia {sub[0].upper()}"
    return parts[0]


class SimbadClient(DataSourceClient):
    """Thin wrapper around ``astroquery.simbad.Simbad`` for name resolution."""

    @property
    def name(self) -> Catalog:
        """SIMBAD is not a first-class catalog in our domain; reuse Gaia's tag."""
        return Catalog.GAIA

    def health_check(self) -> bool:
        """Return True if a trivial SIMBAD query completes."""
        log.debug("simbad.health_check.start")
        try:
            from astroquery.simbad import Simbad

            s = Simbad()
            s.TIMEOUT = 30
            result = s.query_object("Sun")
            return result is not None
        except Exception as e:
            log.warning("simbad.health_check.failed", error=str(e))
            return False

    def resolve(self, name: str) -> ResolvedObject:
        """Resolve a catalog name via SIMBAD.

        Args:
            name: Any name SIMBAD understands (e.g. ``"Kepler-10"``,
                ``"HD 209458"``, ``"Proxima Cen"``).

        Returns:
            A :class:`ResolvedObject` with coordinates (decimal degrees) and a
            dictionary of cross-catalog identifiers.

        Raises:
            ValidationError: If ``name`` is empty or whitespace.
            DataSourceNotFoundError: If SIMBAD has no record of ``name``.
            DataSourceUnavailableError: If the SIMBAD service is unreachable.
        """
        if not name or not name.strip():
            raise ValidationError("name must not be empty")

        log.debug("simbad.resolve.start", name=name)
        try:
            from astroquery.simbad import Simbad
        except ImportError as e:
            raise DataSourceUnavailableError(
                f"astroquery is required for SIMBAD lookups: {e}"
            ) from e

        simbad = Simbad()
        simbad.TIMEOUT = 60
        # add_votable_fields signals we also want cross-IDs, typed coordinates.
        try:
            simbad.add_votable_fields("ids")
        except Exception:
            # Newer astroquery accepts this directly; older versions may raise
            # if already added. Safe to ignore.
            pass

        try:
            result = simbad.query_object(name)
        except Exception as e:
            log.warning("simbad.resolve.failed", name=name, error=str(e))
            raise DataSourceUnavailableError(
                f"SIMBAD query failed for {name!r}: {e}"
            ) from e

        if result is None or len(result) == 0:
            log.warning("simbad.resolve.not_found", name=name)
            raise DataSourceNotFoundError(f"SIMBAD has no object named {name!r}")

        row = result[0]
        ra_deg = _extract_ra_deg(row)
        dec_deg = _extract_dec_deg(row)
        identifiers = _parse_ids_field(row)

        resolved = ResolvedObject(
            name=str(row.get("main_id", name) or name),
            ra_deg=ra_deg,
            dec_deg=dec_deg,
            identifiers=identifiers,
        )
        log.info(
            "simbad.resolve.done",
            name=name,
            ra_deg=ra_deg,
            dec_deg=dec_deg,
            gaia_dr3=resolved.gaia_dr3_source_id,
            n_ids=len(identifiers),
        )
        return resolved


def _extract_ra_deg(row: object) -> float | None:
    """Pull an RA in decimal degrees from a SIMBAD row (column names vary)."""
    for key in ("ra", "RA_d", "RA"):
        try:
            value = row.get(key) if hasattr(row, "get") else row[key]  # type: ignore[index]
        except (KeyError, ValueError, TypeError):
            continue
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _extract_dec_deg(row: object) -> float | None:
    """Pull a Dec in decimal degrees from a SIMBAD row."""
    for key in ("dec", "DEC_d", "DEC"):
        try:
            value = row.get(key) if hasattr(row, "get") else row[key]  # type: ignore[index]
        except (KeyError, ValueError, TypeError):
            continue
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _parse_ids_field(row: object) -> dict[str, str]:
    """Parse SIMBAD's ``ids`` column (pipe-delimited) into a prefix→id map."""
    # astroquery's column may be named 'ids' or 'IDS' depending on version.
    raw: object = None
    for key in ("ids", "IDS"):
        try:
            raw = row.get(key) if hasattr(row, "get") else row[key]  # type: ignore[index]
        except (KeyError, ValueError, TypeError):
            continue
        if raw is not None:
            break
    if raw is None:
        return {}
    try:
        text = str(raw)
    except Exception:
        return {}
    out: dict[str, str] = {}
    for ident in text.split("|"):
        ident = ident.strip()
        if not ident:
            continue
        prefix = _prefix_of(ident)
        # When SIMBAD has multiple ids with the same prefix (e.g. several
        # 2MASS entries), the last one wins — fine for our purposes since we
        # mainly care about the Gaia-prefixed IDs.
        out[prefix] = ident
    return out
