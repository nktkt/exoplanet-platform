"""TestClient-based integration tests for the FastAPI application."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from exoplanet_platform.api.dependencies import (
    get_nasa_client,
    get_planet_repo,
    get_star_repo,
)
from exoplanet_platform.api.errors import register_exception_handlers
from exoplanet_platform.api.main import create_app
from exoplanet_platform.domain import (
    Catalog,
    DetectionMethod,
    Planet,
    Star,
)
from exoplanet_platform.exceptions import ValidationError


def test_health_endpoint(api_client: TestClient) -> None:
    resp = api_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] in {"ok", "degraded"}
    assert "database" in body["checks"]
    assert body["checks"]["database"] == "ok"
    assert body["version"]


def test_planets_empty_list(api_client: TestClient) -> None:
    resp = api_client.get("/planets")
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []
    assert body["total"] == 0


def test_planet_not_found_returns_404(api_client: TestClient) -> None:
    resp = api_client.get("/planets/xyz-nonexistent")
    assert resp.status_code == 404
    body = resp.json()
    # ErrorResponse envelope.
    assert "error" in body
    assert body.get("type") == "DataSourceNotFoundError"


def test_ingest_planet_via_mocked_client() -> None:
    """POST /planets/ingest uses a monkeypatched NASA client via Depends override."""
    app = create_app()
    earth = Planet(
        identifier="TestIngested b",
        host_star="TestIngested",
        discovery_method=DetectionMethod.TRANSIT,
        discovery_year=2024,
        orbital_period_days=1.5,
        semi_major_axis_au=0.03,
        radius_earth=1.1,
        catalog=Catalog.NASA_EXOPLANET_ARCHIVE,
        last_updated=datetime.now(UTC),
    )

    class _Client:
        def fetch_planet(self, identifier: str) -> Planet:
            return earth

        # Also expose get_planet in case callers reach for the ingestion API.
        def get_planet(self, identifier: str) -> Planet:
            return earth

    app.dependency_overrides[get_nasa_client] = lambda: _Client()

    with TestClient(app) as client:
        resp = client.post(
            "/planets/ingest",
            json={
                "identifier": "TestIngested b",
                "catalog": Catalog.NASA_EXOPLANET_ARCHIVE.value,
            },
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["planet"]["identifier"] == "TestIngested b"

        # The ingested record should now be listable.
        listed = client.get("/planets/TestIngested b")
        assert listed.status_code == 200


def test_habitability_endpoint_earth_like(
    api_client: TestClient, sample_planet: Planet, sample_star: Star
) -> None:
    """Seed an Earth-like planet+star via repo overrides, then hit /analysis/habitability."""

    class _FakePlanetRepo:
        def get(self, identifier: str) -> Planet:
            return sample_planet

    class _FakeStarRepo:
        def get(self, identifier: str) -> Star:
            return sample_star

    app = api_client.app  # type: ignore[attr-defined]
    app.dependency_overrides[get_planet_repo] = lambda: _FakePlanetRepo()
    app.dependency_overrides[get_star_repo] = lambda: _FakeStarRepo()

    try:
        resp = api_client.post(
            "/analysis/habitability",
            json={"planet_identifier": sample_planet.identifier},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assessment = body["assessment"]
        assert assessment["in_conservative_hz"] is True
        assert assessment["planet"] == sample_planet.identifier
    finally:
        app.dependency_overrides.pop(get_planet_repo, None)
        app.dependency_overrides.pop(get_star_repo, None)


def test_validation_error_maps_to_400() -> None:
    """A handler that raises our ValidationError should return 400 + ErrorResponse."""
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/boom")
    def _boom() -> dict[str, Any]:
        raise ValidationError("bad input")

    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/boom")
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"] == "bad input"
        assert body["type"] == "ValidationError"
