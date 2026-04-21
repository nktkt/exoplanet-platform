"""Tests for the NASA Exoplanet Archive client (offline; HTTPClient mocked)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from exoplanet_platform.domain import Catalog, DetectionMethod
from exoplanet_platform.exceptions import (
    DataSourceNotFoundError,
    ValidationError,
)
from exoplanet_platform.ingestion import nasa_exoplanet_archive as nea


def _fake_response(payload: Any, status: int = 200) -> httpx.Response:
    req = httpx.Request("GET", "https://example.test/")
    return httpx.Response(status_code=status, json=payload, request=req)


@pytest.fixture()
def patched_client(monkeypatch: pytest.MonkeyPatch):
    """Return (client_instance, mock_http) with the HTTP layer replaced."""
    mock_http = MagicMock()
    monkeypatch.setattr(nea, "get_http_client", lambda: mock_http)
    client = nea.NASAExoplanetArchiveClient()
    return client, mock_http


def _row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "pl_name": "Kepler-10 b",
        "hostname": "Kepler-10",
        "discoverymethod": "Transit",
        "disc_year": 2011,
        "pl_orbper": 0.8374907,
        "pl_orbsmax": 0.01684,
        "pl_orbeccen": 0.0,
        "pl_orbincl": 84.8,
        "pl_rade": 1.47,
        "pl_bmasse": 3.33,
        "pl_dens": 5.8,
        "pl_eqt": 2169.0,
        "pl_insol": 3732.0,
        "pl_tranmid": 2454964.57,
        "pl_trandur": 1.81,
        "pl_trandep": 0.0152,
    }
    base.update(overrides)
    return base


def test_get_planet_parses_fields(patched_client: Any) -> None:
    client, mock_http = patched_client
    mock_http.get.return_value = _fake_response([_row()])

    planet = client.get_planet("Kepler-10 b")
    assert planet.identifier == "Kepler-10 b"
    assert planet.host_star == "Kepler-10"
    assert planet.discovery_method is DetectionMethod.TRANSIT
    assert planet.catalog is Catalog.NASA_EXOPLANET_ARCHIVE
    assert planet.orbital_period_days == pytest.approx(0.8374907)
    # pl_trandep is reported as fraction; we multiply by 10_000 to get ppm.
    assert planet.transit_depth_ppm == pytest.approx(152.0)


def test_get_planet_not_found(patched_client: Any) -> None:
    client, mock_http = patched_client
    mock_http.get.return_value = _fake_response([])
    with pytest.raises(DataSourceNotFoundError):
        client.get_planet("Nonexistent")


def test_get_planet_escapes_single_quotes(patched_client: Any) -> None:
    client, mock_http = patched_client
    mock_http.get.return_value = _fake_response([_row(pl_name="O'Brien b")])
    client.get_planet("O'Brien b")
    params = mock_http.get.call_args.kwargs["params"]
    assert "''" in params["query"]
    assert "O''Brien" in params["query"]


def test_get_planet_empty_name_validation(patched_client: Any) -> None:
    client, _ = patched_client
    with pytest.raises(ValidationError):
        client.get_planet("   ")


def test_search_planets_filters_and_limits(patched_client: Any) -> None:
    client, mock_http = patched_client
    mock_http.get.return_value = _fake_response([_row(), _row(pl_name="Kepler-10 c")])

    planets = client.search_planets(name="Kepler-10", host_star="Kepler-10", limit=5)
    assert len(planets) == 2
    adql = mock_http.get.call_args.kwargs["params"]["query"]
    assert "TOP 5" in adql
    assert "UPPER(pl_name) LIKE" in adql
    assert "UPPER(hostname) LIKE" in adql


def test_search_planets_rejects_bad_limit(patched_client: Any) -> None:
    client, _ = patched_client
    with pytest.raises(ValidationError):
        client.search_planets(name="x", limit=0)
    with pytest.raises(ValidationError):
        client.search_planets(name="x", limit=100_000)


def test_unknown_method_maps_to_unknown(patched_client: Any) -> None:
    client, mock_http = patched_client
    mock_http.get.return_value = _fake_response(
        [_row(discoverymethod="UnheardOfMethod")]
    )
    planet = client.get_planet("Kepler-10 b")
    assert planet.discovery_method is DetectionMethod.UNKNOWN


def test_name_property() -> None:
    assert nea.NASAExoplanetArchiveClient.__mro__[0] is nea.NASAExoplanetArchiveClient
