"""Transit detection pipeline: detrending, BLS search, and phase folding.

The :class:`TransitDetector` is stateless; all configuration comes from
``get_settings().analysis``. Public methods are pure with respect to their
inputs (they never mutate the passed :class:`LightCurve`).
"""

from __future__ import annotations

import numpy as np
from astropy import units as u
from astropy.timeseries import BoxLeastSquares
from scipy.ndimage import median_filter

from exoplanet_platform.config import AnalysisSettings, get_settings
from exoplanet_platform.domain import LightCurve, TransitSignal
from exoplanet_platform.exceptions import InsufficientDataError
from exoplanet_platform.logging_config import get_logger

logger = get_logger(__name__)


class TransitDetector:
    """Transit search / detrending utilities built on top of astropy BLS.

    Parameters
    ----------
    settings:
        Optional analysis-settings override (chiefly for tests). When omitted
        the cached :func:`get_settings` value is used.
    """

    def __init__(self, settings: AnalysisSettings | None = None) -> None:
        self._settings: AnalysisSettings = settings or get_settings().analysis

    # ------------------------------------------------------------------ #
    # Detrending
    # ------------------------------------------------------------------ #
    def detrend(self, lc: LightCurve) -> LightCurve:
        """Median-filter detrend and sigma-clip a light curve.

        Applies a rolling median filter with window ``detrend_window_hours``
        (converted to samples from the median cadence), divides the flux by
        the trend, then masks points further than ``sigma_clip`` MAD-sigmas
        from unity.

        Returns
        -------
        LightCurve
            New ``LightCurve`` with detrended flux. Time, flux, and flux_err
            arrays are filtered to surviving indices. Units of flux become
            dimensionless (normalized to 1.0).
        """
        logger.debug("detrend.start", target=lc.target, n_points=len(lc.time_bjd))
        self._guard_minimum_points(lc)

        time = np.asarray(lc.time_bjd, dtype=float)
        flux = np.asarray(lc.flux, dtype=float)
        flux_err = (
            np.asarray(lc.flux_err, dtype=float) if lc.flux_err is not None else None
        )

        # Drop non-finite values before any numeric work.
        finite = np.isfinite(time) & np.isfinite(flux)
        if flux_err is not None:
            finite &= np.isfinite(flux_err)
        time = time[finite]
        flux = flux[finite]
        flux_err = flux_err[finite] if flux_err is not None else None

        if time.size < 100:
            raise InsufficientDataError(
                f"Detrending requires >=100 finite points, got {time.size}"
            )

        # Median cadence (days) -> window size in samples.
        diffs = np.diff(time)
        median_cadence_days = float(np.median(diffs[diffs > 0])) if diffs.size else 0.0
        if median_cadence_days <= 0:
            raise InsufficientDataError("Cannot determine cadence from time array")

        window_days = float(self._settings.detrend_window_hours) / 24.0
        window_samples = max(3, int(round(window_days / median_cadence_days)))
        # median_filter expects odd sizes for symmetric behavior.
        if window_samples % 2 == 0:
            window_samples += 1

        trend = median_filter(flux, size=window_samples, mode="nearest")
        # Avoid division by zero: fall back to 1.0 where trend collapses.
        trend = np.where(trend == 0.0, 1.0, trend)
        detrended = flux / trend

        # Sigma-clip around unity using robust MAD sigma.
        median = float(np.median(detrended))
        mad = float(np.median(np.abs(detrended - median)))
        sigma = 1.4826 * mad if mad > 0 else float(np.std(detrended))
        if sigma <= 0:
            sigma = 1.0
        threshold = float(self._settings.sigma_clip) * sigma
        keep = np.abs(detrended - median) <= threshold

        new_time = time[keep]
        new_flux = detrended[keep]
        new_err = flux_err[keep] / trend[keep] if flux_err is not None else None

        if new_time.size < 100:
            raise InsufficientDataError(
                f"After sigma-clip only {new_time.size} points remain (<100)"
            )

        logger.info(
            "detrend.done",
            target=lc.target,
            n_in=int(time.size),
            n_out=int(new_time.size),
            window_samples=window_samples,
        )
        return LightCurve(
            target=lc.target,
            mission=lc.mission,
            time_bjd=new_time.tolist(),
            flux=new_flux.tolist(),
            flux_err=new_err.tolist() if new_err is not None else None,
            quarter=lc.quarter,
            sector=lc.sector,
            cadence_minutes=lc.cadence_minutes,
        )

    # ------------------------------------------------------------------ #
    # Transit search
    # ------------------------------------------------------------------ #
    def search(
        self,
        lc: LightCurve,
        min_period_days: float | None = None,
        max_period_days: float | None = None,
    ) -> list[TransitSignal]:
        """Run Box Least Squares on ``lc`` and return the top-5 signals.

        Parameters
        ----------
        lc:
            Light curve to search. Should already be detrended for best
            results.
        min_period_days, max_period_days:
            Optional overrides for the period-grid bounds; defaults come
            from settings.

        Returns
        -------
        list[TransitSignal]
            Up to five signals sorted by BLS power (descending). Period is
            in days, duration in hours, depth in ppm, SNR dimensionless.
        """
        logger.debug("bls.search.start", target=lc.target)
        self._guard_minimum_points(lc)

        time = np.asarray(lc.time_bjd, dtype=float)
        flux = np.asarray(lc.flux, dtype=float)
        flux_err = (
            np.asarray(lc.flux_err, dtype=float) if lc.flux_err is not None else None
        )

        baseline = float(time.max() - time.min())
        p_min = float(min_period_days or self._settings.bls_min_period_days)
        p_max = float(max_period_days or self._settings.bls_max_period_days)
        # Cap to baseline/2 to guarantee we cover >=2 cycles.
        p_max = min(p_max, baseline / 2.0)
        if p_max <= p_min:
            raise InsufficientDataError(
                f"Baseline {baseline:.2f} d too short for period range "
                f"[{p_min}, {p_max}] d (need baseline >= 2*period)"
            )

        durations = np.asarray(self._settings.bls_duration_grid, dtype=float) * u.day
        bls = BoxLeastSquares(
            time * u.day,
            flux,
            dy=flux_err if flux_err is not None else None,
        )
        # Build a frequency-factor spaced period grid via astropy helper.
        periods = bls.autoperiod(
            durations,
            minimum_period=p_min * u.day,
            maximum_period=p_max * u.day,
            frequency_factor=float(self._settings.bls_frequency_factor),
        )
        result = bls.power(periods, durations)

        power = np.asarray(result.power, dtype=float)
        period_arr = np.asarray(result.period.to(u.day).value, dtype=float)
        duration_arr = np.asarray(result.duration.to(u.day).value, dtype=float)
        t0_arr = np.asarray(result.transit_time.to(u.day).value, dtype=float)
        depth_arr = np.asarray(result.depth, dtype=float)

        # Pick top-5 by power. argpartition is cheap but we still need them
        # sorted among themselves.
        k = min(5, power.size)
        top_idx = np.argpartition(power, -k)[-k:]
        top_idx = top_idx[np.argsort(-power[top_idx])]

        # Noise per transit: per-point scatter / sqrt(points in transit).
        point_sigma = float(np.std(flux))
        median_cadence_days = float(np.median(np.diff(time)))
        median_cadence_days = median_cadence_days if median_cadence_days > 0 else 1.0

        signals: list[TransitSignal] = []
        for idx in top_idx:
            duration_d = float(duration_arr[idx])
            depth = float(depth_arr[idx])
            n_in_transit = max(1.0, duration_d / median_cadence_days)
            noise_per_transit = point_sigma / float(np.sqrt(n_in_transit))
            snr = float(abs(depth) / noise_per_transit) if noise_per_transit > 0 else 0.0
            signals.append(
                TransitSignal(
                    period_days=float(period_arr[idx]),
                    epoch_bjd=float(t0_arr[idx]),
                    duration_hours=duration_d * 24.0,
                    depth_ppm=depth * 1.0e6,
                    snr=snr,
                    power=float(power[idx]),
                    method="bls",
                )
            )

        logger.info(
            "bls.search.done",
            target=lc.target,
            n_signals=len(signals),
            top_period_days=signals[0].period_days if signals else None,
        )
        return signals

    # ------------------------------------------------------------------ #
    # Phase folding
    # ------------------------------------------------------------------ #
    def phase_fold(
        self, lc: LightCurve, period_days: float, epoch_bjd: float
    ) -> tuple[np.ndarray, np.ndarray]:
        """Fold a light curve on a candidate period/epoch.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            ``(phase, flux)`` arrays with ``phase`` in the half-open interval
            ``[-0.5, 0.5)`` (dimensionless), sorted ascending by phase. Flux
            retains its input units.
        """
        logger.debug(
            "phase_fold.start",
            target=lc.target,
            period_days=period_days,
            epoch=epoch_bjd,
        )
        if period_days <= 0:
            raise InsufficientDataError("period_days must be positive")

        time = np.asarray(lc.time_bjd, dtype=float)
        flux = np.asarray(lc.flux, dtype=float)
        if time.size < 100:
            raise InsufficientDataError(
                f"Phase folding needs >=100 points, got {time.size}"
            )

        phase = ((time - epoch_bjd) / period_days + 0.5) % 1.0 - 0.5
        order = np.argsort(phase)
        return phase[order], flux[order]

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _guard_minimum_points(lc: LightCurve) -> None:
        n = len(lc.time_bjd)
        if n < 100:
            raise InsufficientDataError(
                f"Light curve has {n} points; need at least 100"
            )
        if len(lc.flux) != n:
            raise InsufficientDataError("time_bjd and flux have mismatched lengths")
