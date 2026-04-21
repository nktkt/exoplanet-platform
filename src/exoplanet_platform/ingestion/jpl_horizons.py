"""Client for the JPL Horizons ephemeris service via ``astroquery``.

Provides time-series ephemerides and osculating orbital elements for
Solar-System bodies. The underlying package ``astroquery.jplhorizons``
handles the on-the-wire protocol; we wrap it in a domain-friendly API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..domain import Catalog
from ..exceptions import (
    DataSourceNotFoundError,
    DataSourceUnavailableError,
    ValidationError,
)
from ..logging_config import get_logger
from .base import DataSourceClient

if TYPE_CHECKING:
    import pandas as pd

log = get_logger(__name__)


def _is_unknown_body_error(exc: BaseException) -> bool:
    """Return True if a JPL Horizons error string indicates an unknown body."""
    msg = str(exc).lower()
    return any(
        token in msg
        for token in (
            "no matches found",
            "unknown target",
            "no such object",
            "ambiguous target",
            "no ephemeris",
            "not found",
        )
    )


class JPLHorizonsClient(DataSourceClient):
    """Fetch ephemerides and orbital elements from the JPL Horizons system."""

    @property
    def name(self) -> Catalog:
        """Return the catalog identifier for this client."""
        return Catalog.JPL_HORIZONS

    def health_check(self) -> bool:
        """Return True if ``astroquery.jplhorizons`` responds for Mars."""
        log.debug("jpl.health_check.start")
        try:
            from astroquery.jplhorizons import Horizons

            probe = Horizons(id="499", location="500", epochs=2451545.0)
            probe.elements()
            return True
        except Exception as e:  # noqa: BLE001 - health must not raise
            log.warning("jpl.health_check.failed", error=str(e))
            return False

    def get_ephemeris(
        self,
        body: str,
        start: str,
        stop: str,
        step: str = "1d",
    ) -> "pd.DataFrame":
        """Fetch an ephemeris table for a body over a date range.

        Args:
            body: JPL Horizons ID or name (e.g. ``"499"`` or ``"Mars"``).
            start: ISO-format start date (e.g. ``"2024-01-01"``).
            stop: ISO-format stop date (exclusive).
            step: Horizons-formatted step size (e.g. ``"1d"``, ``"6h"``).

        Returns:
            A pandas DataFrame with the ephemeris columns reported by
            Horizons (typically includes ``datetime_jd``, ``RA``, ``DEC``,
            ``delta``, ``r``, and more).

        Raises:
            ValidationError: If any argument is empty.
            DataSourceNotFoundError: If Horizons reports no such body.
            DataSourceUnavailableError: If the service is unreachable.
        """
        log.debug(
            "jpl.get_ephemeris.start",
            body=body,
            start=start,
            stop=stop,
            step=step,
        )
        self._validate_nonempty(body=body, start=start, stop=stop, step=step)

        try:
            from astroquery.jplhorizons import Horizons
        except ImportError as e:
            raise DataSourceUnavailableError(
                f"astroquery is required for JPL Horizons: {e}"
            ) from e

        try:
            log.debug("jpl.ephemerides.request", body=body)
            obj = Horizons(
                id=body,
                location="500",  # geocentric observer
                epochs={"start": start, "stop": stop, "step": step},
            )
            table = obj.ephemerides()
        except Exception as e:  # noqa: BLE001 - astroquery raises bare exceptions
            if _is_unknown_body_error(e):
                log.warning("jpl.ephemerides.not_found", body=body, error=str(e))
                raise DataSourceNotFoundError(
                    f"JPL Horizons has no ephemeris for body {body!r}"
                ) from e
            log.warning("jpl.ephemerides.failed", body=body, error=str(e))
            raise DataSourceUnavailableError(
                f"JPL Horizons ephemeris failed for {body!r}: {e}"
            ) from e

        df = table.to_pandas()
        log.info("jpl.get_ephemeris.done", body=body, rows=len(df))
        return df

    def get_orbital_elements(
        self,
        body: str,
        epoch: str | float | None = None,
    ) -> dict[str, float | None]:
        """Fetch osculating orbital elements for a body at a given epoch.

        Args:
            body: JPL Horizons ID or name.
            epoch: Either a JD float, an ISO-format date string, or None
                (defaults to J2000 = 2451545.0).

        Returns:
            A dict with keys ``a_au``, ``e``, ``i_deg``, ``raan_deg``,
            ``argp_deg``, ``M_deg``, ``period_days``. Values are floats
            or None if a field is absent.

        Raises:
            ValidationError: If ``body`` is empty.
            DataSourceNotFoundError: If Horizons has no record for ``body``.
            DataSourceUnavailableError: If the service is unreachable.
        """
        log.debug("jpl.get_orbital_elements.start", body=body, epoch=epoch)
        self._validate_nonempty(body=body)

        try:
            from astroquery.jplhorizons import Horizons
        except ImportError as e:
            raise DataSourceUnavailableError(
                f"astroquery is required for JPL Horizons: {e}"
            ) from e

        # Horizons' elements() requires a scalar epoch (JD) or dict range.
        if epoch is None:
            epoch_value: str | float = 2451545.0  # J2000.0 default
        else:
            epoch_value = epoch

        try:
            log.debug("jpl.elements.request", body=body, epoch=epoch_value)
            obj = Horizons(id=body, location="@sun", epochs=epoch_value)
            table = obj.elements()
        except Exception as e:  # noqa: BLE001
            if _is_unknown_body_error(e):
                log.warning("jpl.elements.not_found", body=body, error=str(e))
                raise DataSourceNotFoundError(
                    f"JPL Horizons has no orbital elements for body {body!r}"
                ) from e
            log.warning("jpl.elements.failed", body=body, error=str(e))
            raise DataSourceUnavailableError(
                f"JPL Horizons elements failed for {body!r}: {e}"
            ) from e

        if table is None or len(table) == 0:
            log.warning("jpl.elements.empty", body=body)
            raise DataSourceNotFoundError(
                f"JPL Horizons returned no elements for body {body!r}"
            )

        row = table[0]
        result = {
            "a_au": _col_float(row, "a"),
            "e": _col_float(row, "e"),
            "i_deg": _col_float(row, "incl"),
            "raan_deg": _col_float(row, "Omega"),
            "argp_deg": _col_float(row, "w"),
            "M_deg": _col_float(row, "M"),
            "period_days": _col_float(row, "P"),
        }
        log.info("jpl.get_orbital_elements.done", body=body, a_au=result["a_au"])
        return result

    @staticmethod
    def _validate_nonempty(**kwargs: str) -> None:
        """Raise ``ValidationError`` if any keyword argument is an empty string."""
        for k, v in kwargs.items():
            if not isinstance(v, str) or not v.strip():
                raise ValidationError(f"{k} must be a non-empty string")


def _col_float(row: Any, column: str) -> float | None:
    """Return ``row[column]`` as a float, or None if missing/masked."""
    try:
        value = row[column]
    except (KeyError, IndexError, ValueError):
        return None
    if value is None:
        return None
    # Astropy masked scalar
    if getattr(value, "mask", False):
        return None
    try:
        if hasattr(value, "item"):
            value = value.item()
        return float(value)
    except (TypeError, ValueError):
        return None
