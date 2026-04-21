"""Tests for the Gaia client.

The real `astroquery.gaia.Gaia` does network I/O on import; we stub it out with
a lightweight fake so these tests run offline. Integration tests (excluded by
default via the `integration` marker) would exercise the real service.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from exoplanet_platform.domain import Catalog
from exoplanet_platform.exceptions import (
    DataSourceNotFoundError,
    DataSourceUnavailableError,
    ValidationError,
)


class _FakeTable:
    """Minimal stand-in for an astropy Table with only what `_run_query` uses."""

    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows
        self.colnames = list(rows[0].keys()) if rows else []

    def __iter__(self):
        return iter(self._rows)

    def __len__(self) -> int:
        return len(self._rows)


class _FakeJob:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def get_results(self) -> _FakeTable:
        return _FakeTable(self._rows)


class _FakeGaia:
    last_adql: str | None = None
    next_rows: list[dict[str, Any]] = []
    raise_on_launch: Exception | None = None

    @classmethod
    def launch_job(cls, adql: str) -> _FakeJob:
        cls.last_adql = adql
        if cls.raise_on_launch is not None:
            raise cls.raise_on_launch
        return _FakeJob(cls.next_rows)


@pytest.fixture()
def fake_gaia(monkeypatch: pytest.MonkeyPatch):
    """Insert a fake `astroquery.gaia` module into sys.modules."""
    fake_module = types.ModuleType("astroquery.gaia")
    fake_module.Gaia = _FakeGaia  # type: ignore[attr-defined]
    # Parent package must exist so `from astroquery.gaia import Gaia` works.
    parent = types.ModuleType("astroquery")
    monkeypatch.setitem(sys.modules, "astroquery", parent)
    monkeypatch.setitem(sys.modules, "astroquery.gaia", fake_module)
    _FakeGaia.last_adql = None
    _FakeGaia.next_rows = []
    _FakeGaia.raise_on_launch = None

    from exoplanet_platform.ingestion import gaia as gaia_module

    return gaia_module


def test_query_star_by_source_id(fake_gaia: Any) -> None:
    _FakeGaia.next_rows = [
        {
            "source_id": 12345,
            "ra": 10.0,
            "dec": -5.0,
            "parallax": 10.0,
            "teff_gspphot": 5778.0,
            "radius_gspphot": 1.0,
            "mh_gspphot": 0.0,
        }
    ]
    client = fake_gaia.GaiaClient()
    star = client.query_star(source_id=12345)
    assert star.identifier == "Gaia DR3 12345"
    assert star.catalog is Catalog.GAIA
    assert star.distance_pc == pytest.approx(100.0)  # 1000 / parallax(10)
    assert star.effective_temperature_k == pytest.approx(5778.0)
    assert "gaiadr3.gaia_source" in _FakeGaia.last_adql
    assert "source_id = 12345" in _FakeGaia.last_adql


def test_query_star_by_name_escapes(fake_gaia: Any) -> None:
    _FakeGaia.next_rows = []
    client = fake_gaia.GaiaClient()
    with pytest.raises(DataSourceNotFoundError):
        client.query_star(name="O'Brien")
    assert "''" in _FakeGaia.last_adql


def test_query_star_requires_source_id_or_name(fake_gaia: Any) -> None:
    client = fake_gaia.GaiaClient()
    with pytest.raises(ValidationError):
        client.query_star()


def test_cone_search_shapes_adql(fake_gaia: Any) -> None:
    _FakeGaia.next_rows = []
    client = fake_gaia.GaiaClient()
    out = client.cone_search(ra_deg=10.0, dec_deg=-5.0, radius_arcsec=3.0, limit=7)
    assert out == []
    adql = _FakeGaia.last_adql
    assert "TOP 7" in adql
    assert "CIRCLE('ICRS', 10.0, -5.0" in adql


def test_cone_search_validates_ranges(fake_gaia: Any) -> None:
    client = fake_gaia.GaiaClient()
    with pytest.raises(ValidationError):
        client.cone_search(ra_deg=400.0, dec_deg=0.0, radius_arcsec=1.0)
    with pytest.raises(ValidationError):
        client.cone_search(ra_deg=0.0, dec_deg=100.0, radius_arcsec=1.0)
    with pytest.raises(ValidationError):
        client.cone_search(ra_deg=0.0, dec_deg=0.0, radius_arcsec=0.0)
    with pytest.raises(ValidationError):
        client.cone_search(ra_deg=0.0, dec_deg=0.0, radius_arcsec=1.0, limit=0)


def test_network_error_mapped(fake_gaia: Any) -> None:
    _FakeGaia.raise_on_launch = RuntimeError("network down")
    client = fake_gaia.GaiaClient()
    with pytest.raises(DataSourceUnavailableError):
        client.query_star(source_id=1)


@pytest.mark.integration
def test_gaia_live() -> None:  # pragma: no cover - not run by default
    """Live test hitting the real Gaia TAP service (deselect by default)."""
    from exoplanet_platform.ingestion.gaia import GaiaClient

    client = GaiaClient()
    assert client.health_check()
