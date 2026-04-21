"""CLI tests using Typer's CliRunner."""

from __future__ import annotations

from typer.testing import CliRunner

from exoplanet_platform.cli.main import app as cli_app
from exoplanet_platform.domain import Catalog, DetectionMethod, Planet
from exoplanet_platform.ingestion import nasa_exoplanet_archive as nea


def test_help_lists_subcommands() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_app, ["--help"])
    assert result.exit_code == 0
    out = result.stdout
    for cmd in ("search", "get", "ingest", "list-planets", "orbit", "db", "serve", "analyze"):
        assert cmd in out


def test_db_init() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_app, ["db", "init"])
    assert result.exit_code == 0, result.stdout
    assert "Database initialised" in result.stdout or "initialised" in result.stdout.lower()


def test_list_planets_empty(monkeypatch) -> None:
    """On a fresh in-memory DB `list-planets` should print a friendly empty message."""
    runner = CliRunner()
    result = runner.invoke(cli_app, ["list-planets"])
    assert result.exit_code == 0, result.stdout
    assert "No planets stored" in result.stdout or "No planets" in result.stdout


def test_ingest_nasa_stores_planet(monkeypatch) -> None:
    """Monkeypatch NASAExoplanetArchiveClient.get_planet and verify ingestion."""
    runner = CliRunner()

    synthetic = Planet(
        identifier="Kepler-CLI b",
        host_star="Kepler-CLI",
        discovery_method=DetectionMethod.TRANSIT,
        orbital_period_days=2.2,
        radius_earth=1.3,
        catalog=Catalog.NASA_EXOPLANET_ARCHIVE,
    )

    def _fake_get_planet(self, name: str) -> Planet:
        return synthetic

    monkeypatch.setattr(
        nea.NASAExoplanetArchiveClient, "get_planet", _fake_get_planet, raising=True
    )

    result = runner.invoke(cli_app, ["ingest", "Kepler-CLI b", "--catalog", "nasa"])
    assert result.exit_code == 0, result.stdout
    assert "Kepler-CLI b" in result.stdout

    # The planet should now appear in `list-planets`.
    listed = runner.invoke(cli_app, ["list-planets"])
    assert listed.exit_code == 0
    assert "Kepler-CLI b" in listed.stdout


def test_ingest_rejects_unknown_catalog() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_app, ["ingest", "foo", "--catalog", "bogus"])
    assert result.exit_code != 0
    assert "Unsupported catalog" in result.stdout or "bogus" in result.stdout
