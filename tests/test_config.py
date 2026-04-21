"""Tests for config loading, env overrides, and YAML merging."""

from __future__ import annotations

from pathlib import Path

import pytest

from exoplanet_platform.config import (
    Settings,
    get_settings,
    reset_settings_cache,
)


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    reset_settings_cache()


def test_defaults_load(monkeypatch: pytest.MonkeyPatch) -> None:
    # Wipe any EXOPLANET_* env vars so defaults surface cleanly.
    for key in list(__import__("os").environ):
        if key.startswith("EXOPLANET_"):
            monkeypatch.delenv(key, raising=False)
    reset_settings_cache()
    s = get_settings()
    assert s.environment == "development"
    assert s.api.host == "0.0.0.0"
    assert s.api.port == 8000
    assert s.storage.database_url == "sqlite:///./exoplanet.db"
    assert s.analysis.bls_min_period_days == pytest.approx(0.5)


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXOPLANET_ENVIRONMENT", "production")
    monkeypatch.setenv("EXOPLANET_API__PORT", "9999")
    monkeypatch.setenv(
        "EXOPLANET_STORAGE__DATABASE_URL", "sqlite:///./custom.db"
    )
    reset_settings_cache()
    s = get_settings()
    assert s.environment == "production"
    assert s.api.port == 9999
    assert s.storage.database_url == "sqlite:///./custom.db"


def test_yaml_merge(tmp_path: Path) -> None:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """
environment: staging
api:
  port: 7777
analysis:
  bls_min_period_days: 1.25
""",
        encoding="utf-8",
    )
    reset_settings_cache()
    s = get_settings(config_file=str(cfg))
    assert s.environment == "staging"
    assert s.api.port == 7777
    assert s.analysis.bls_min_period_days == pytest.approx(1.25)


def test_empty_database_url_rejected() -> None:
    with pytest.raises(ValueError):
        Settings(storage={"database_url": ""})  # type: ignore[arg-type]


def test_get_settings_is_cached() -> None:
    reset_settings_cache()
    a = get_settings()
    b = get_settings()
    assert a is b
