"""Tests for the TransitDetector (detrend, BLS search, phase fold)."""

from __future__ import annotations

import numpy as np
import pytest

from exoplanet_platform.analysis.transit import TransitDetector
from exoplanet_platform.domain import Catalog, LightCurve
from exoplanet_platform.exceptions import InsufficientDataError

KNOWN_PERIOD_DAYS = 3.0
KNOWN_EPOCH_BJD = 2.0


def test_search_recovers_known_period(sample_light_curve: LightCurve) -> None:
    detector = TransitDetector()
    signals = detector.search(
        sample_light_curve,
        min_period_days=1.0,
        max_period_days=10.0,
    )
    assert signals, "BLS should return at least one signal"
    top = signals[0]
    recovered = top.period_days
    # Recover the period within 1% of truth.
    assert abs(recovered - KNOWN_PERIOD_DAYS) / KNOWN_PERIOD_DAYS < 0.01
    assert top.method == "bls"
    assert top.power > 0


def test_detrend_reduces_variance(make_light_curve) -> None:
    # Use a louder sinusoidal baseline so there's variance to reduce.
    lc = make_light_curve(noise_sigma=5.0e-4)
    raw_variance = float(np.var(lc.flux))

    detector = TransitDetector()
    detrended = detector.detrend(lc)
    new_variance = float(np.var(detrended.flux))
    assert new_variance < raw_variance
    # Flux stays dimensionless and centered near 1.
    assert abs(float(np.median(detrended.flux)) - 1.0) < 1e-2


def test_phase_fold_shape_and_range(sample_light_curve: LightCurve) -> None:
    detector = TransitDetector()
    phase, flux = detector.phase_fold(
        sample_light_curve,
        period_days=KNOWN_PERIOD_DAYS,
        epoch_bjd=KNOWN_EPOCH_BJD,
    )
    assert phase.shape == flux.shape
    assert len(phase) == len(sample_light_curve.time_bjd)
    assert phase.min() >= -0.5
    assert phase.max() < 0.5
    # Output is sorted ascending by phase.
    assert np.all(np.diff(phase) >= 0)


def test_insufficient_points_raises() -> None:
    lc = LightCurve(
        target="tiny",
        mission=Catalog.TESS,
        time_bjd=[0.0, 1.0, 2.0],
        flux=[1.0, 1.0, 1.0],
    )
    detector = TransitDetector()
    with pytest.raises(InsufficientDataError):
        detector.search(lc)
    with pytest.raises(InsufficientDataError):
        detector.detrend(lc)


def test_phase_fold_rejects_non_positive_period(sample_light_curve: LightCurve) -> None:
    detector = TransitDetector()
    with pytest.raises(InsufficientDataError):
        detector.phase_fold(sample_light_curve, period_days=0.0, epoch_bjd=0.0)
