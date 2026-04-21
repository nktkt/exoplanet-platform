"""Client for MAST light-curve retrieval via ``lightkurve``.

``lightkurve`` proxies the Mikulski Archive for Space Telescopes (MAST)
and provides a uniform API over Kepler, K2, and TESS time-series products.
We wrap it so callers deal only with domain types and domain exceptions.
"""

from __future__ import annotations

from typing import Any

from ..domain import Catalog, LightCurve
from ..exceptions import (
    DataSourceNotFoundError,
    DataSourceUnavailableError,
    ValidationError,
)
from ..logging_config import get_logger
from .base import DataSourceClient

log = get_logger(__name__)


_MISSION_MAP: dict[str, Catalog] = {
    "tess": Catalog.TESS,
    "kepler": Catalog.KEPLER,
    "k2": Catalog.K2,
}


def _normalize_mission(mission: str | Catalog) -> tuple[str, Catalog]:
    """Return (lightkurve_mission_name, Catalog) for a user-supplied mission.

    Accepts either a plain string (``"TESS"``, ``"Kepler"``, ``"K2"``) or a
    ``Catalog`` enum. String comparison is case-insensitive.

    Raises:
        ValidationError: If ``mission`` is not one of TESS/Kepler/K2.
    """
    key = mission.value if isinstance(mission, Catalog) else str(mission).strip().lower()
    key = key.lower()
    if key not in _MISSION_MAP:
        raise ValidationError(
            f"Unsupported mission '{mission}'. Must be one of TESS, Kepler, K2."
        )
    display = {"tess": "TESS", "kepler": "Kepler", "k2": "K2"}[key]
    return display, _MISSION_MAP[key]


class MASTClient(DataSourceClient):
    """Retrieve light curves from MAST via ``lightkurve``.

    Use ``search_light_curves`` for metadata-only listings and
    ``download_light_curve`` to actually fetch the time-series data.
    """

    @property
    def name(self) -> Catalog:
        """Return the catalog identifier for this client (TESS by default).

        MAST hosts multiple missions; this returns ``Catalog.TESS`` as the
        nominal default. Per-query catalog is captured inside each
        ``LightCurve`` via its ``mission`` field.
        """
        return Catalog.TESS

    def health_check(self) -> bool:
        """Return True if ``lightkurve`` can be imported and queried briefly."""
        log.debug("mast.health_check.start")
        try:
            import lightkurve as lk  # noqa: F401 - import check is the test
        except Exception as e:
            log.warning("mast.health_check.import_failed", error=str(e))
            return False
        return True

    def search_light_curves(
        self,
        target: str,
        mission: str | Catalog = "TESS",
    ) -> list[dict[str, Any]]:
        """Search for available light-curve products for a target.

        Args:
            target: Target name understood by MAST (e.g. ``"Kepler-10"``,
                ``"TIC 261136679"``).
            mission: One of ``"TESS"``, ``"Kepler"``, ``"K2"``.

        Returns:
            A list of metadata dicts (one per matched product). Each dict
            contains at minimum the keys ``mission``, ``target_name``,
            ``exptime``, ``author``, ``quarter``, ``sector``, ``year``.

        Raises:
            ValidationError: If ``mission`` is not supported.
            DataSourceUnavailableError: If MAST cannot be reached.
            DataSourceNotFoundError: If no products match ``target``.
        """
        log.debug("mast.search.start", target=target, mission=mission)
        if not target or not target.strip():
            raise ValidationError("target must not be empty")
        display_mission, _ = _normalize_mission(mission)

        try:
            import lightkurve as lk
        except ImportError as e:
            raise DataSourceUnavailableError(
                f"lightkurve is required for MAST access: {e}"
            ) from e

        try:
            log.debug("mast.search.request", target=target, mission=display_mission)
            result = lk.search_lightcurve(target, mission=display_mission)
        except Exception as e:
            log.warning("mast.search.failed", target=target, error=str(e))
            raise DataSourceUnavailableError(
                f"MAST search failed for {target!r}: {e}"
            ) from e

        if result is None or len(result) == 0:
            log.warning("mast.search.empty", target=target, mission=display_mission)
            raise DataSourceNotFoundError(
                f"No {display_mission} light curves found for target {target!r}"
            )

        rows: list[dict[str, Any]] = []
        table = getattr(result, "table", None)
        if table is not None:
            for r in table:
                rows.append({col: _scalar(r[col]) for col in table.colnames})
        log.info(
            "mast.search.done",
            target=target,
            mission=display_mission,
            count=len(rows),
        )
        return rows

    def download(
        self,
        target: str,
        mission: str | Catalog,
        quarter: int | None = None,
        sector: int | None = None,
    ) -> LightCurve:
        """Alias of :meth:`download_light_curve` accepting ``str`` or ``Catalog``."""
        return self.download_light_curve(
            target=target, mission=mission, quarter=quarter, sector=sector
        )

    def download_light_curve(
        self,
        target: str,
        mission: str | Catalog,
        quarter: int | None = None,
        sector: int | None = None,
    ) -> LightCurve:
        """Download and assemble a light curve for a target.

        When multiple products are available, all are stitched together
        via ``lightkurve.LightCurveCollection.stitch()``. If ``quarter``
        (Kepler) or ``sector`` (TESS) is supplied, the search is narrowed.

        Args:
            target: Target name (e.g. ``"Kepler-10"``).
            mission: One of ``"TESS"``, ``"Kepler"``, ``"K2"``.
            quarter: Kepler quarter number to restrict to.
            sector: TESS sector number to restrict to.

        Returns:
            A domain ``LightCurve`` object.

        Raises:
            ValidationError: If ``mission`` is not supported.
            DataSourceNotFoundError: If no matching products are found.
            DataSourceUnavailableError: If the download fails or MAST is down.
        """
        log.debug(
            "mast.download.start",
            target=target,
            mission=mission,
            quarter=quarter,
            sector=sector,
        )
        if not target or not target.strip():
            raise ValidationError("target must not be empty")
        display_mission, catalog = _normalize_mission(mission)

        try:
            import lightkurve as lk
        except ImportError as e:
            raise DataSourceUnavailableError(
                f"lightkurve is required for MAST access: {e}"
            ) from e

        search_kwargs: dict[str, Any] = {"mission": display_mission}
        if quarter is not None:
            search_kwargs["quarter"] = quarter
        if sector is not None:
            search_kwargs["sector"] = sector

        try:
            log.debug("mast.download.search", target=target, kwargs=search_kwargs)
            result = lk.search_lightcurve(target, **search_kwargs)
        except Exception as e:
            log.warning("mast.download.search_failed", target=target, error=str(e))
            raise DataSourceUnavailableError(
                f"MAST search failed for {target!r}: {e}"
            ) from e

        if result is None or len(result) == 0:
            log.warning("mast.download.empty", target=target, mission=display_mission)
            raise DataSourceNotFoundError(
                f"No {display_mission} light curves found for {target!r} "
                f"(quarter={quarter}, sector={sector})"
            )

        try:
            if len(result) == 1:
                log.debug("mast.download.single", target=target)
                lc = result.download()
            else:
                log.debug("mast.download.collection", target=target, n=len(result))
                collection = result.download_all()
                lc = collection.stitch()
        except Exception as e:
            log.warning("mast.download.failed", target=target, error=str(e))
            raise DataSourceUnavailableError(
                f"Failed to download light curve for {target!r}: {e}"
            ) from e

        if lc is None:
            raise DataSourceNotFoundError(
                f"Light curve download returned nothing for {target!r}"
            )

        time_bjd = _array_to_list(getattr(lc.time, "value", lc.time))
        flux = _array_to_list(getattr(lc.flux, "value", lc.flux))
        flux_err_attr = getattr(lc, "flux_err", None)
        flux_err: list[float] | None = None
        if flux_err_attr is not None:
            flux_err = _array_to_list(getattr(flux_err_attr, "value", flux_err_attr))

        cadence_minutes = _infer_cadence_minutes(time_bjd)
        result_lc = LightCurve(
            target=target,
            mission=catalog,
            time_bjd=time_bjd,
            flux=flux,
            flux_err=flux_err,
            quarter=quarter,
            sector=sector,
            cadence_minutes=cadence_minutes,
        )
        log.info(
            "mast.download.done",
            target=target,
            mission=display_mission,
            n_points=len(time_bjd),
            cadence_minutes=cadence_minutes,
        )
        return result_lc


def _scalar(value: Any) -> Any:
    """Convert astropy Column scalars into plain Python types for metadata dicts."""
    try:
        if hasattr(value, "item"):
            return value.item()
    except Exception:
        pass
    return value


def _array_to_list(arr: Any) -> list[float]:
    """Convert a numpy/astropy array of floats (possibly masked) to a Python list."""
    try:
        import numpy as np
    except ImportError:
        return [float(x) for x in arr]

    a = np.asarray(arr, dtype=float)
    if np.ma.isMaskedArray(arr):
        a = np.where(arr.mask, np.nan, a)
    return [float(x) for x in a.tolist()]


def _infer_cadence_minutes(times: list[float]) -> float | None:
    """Infer cadence in minutes from a monotonically-sampled BJD time array."""
    if len(times) < 2:
        return None
    try:
        import numpy as np

        diffs = np.diff(np.asarray(times, dtype=float))
        diffs = diffs[np.isfinite(diffs) & (diffs > 0)]
        if diffs.size == 0:
            return None
        return float(np.median(diffs) * 24.0 * 60.0)
    except Exception:
        return None
