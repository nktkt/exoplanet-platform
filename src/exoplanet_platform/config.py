"""Application configuration loaded from environment + optional YAML file.

Centralizes all tunable parameters so that other modules never read env vars
or magic constants directly. Import `get_settings()` (cached) everywhere.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataSourceSettings(BaseSettings):
    """Configuration for external astronomical data sources."""

    nasa_exoplanet_archive_url: str = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    mast_url: str = "https://mast.stsci.edu/api/v0.1"
    jpl_horizons_url: str = "https://ssd.jpl.nasa.gov/api/horizons.api"
    gaia_tap_url: str = "https://gea.esac.esa.int/tap-server/tap"

    http_timeout_seconds: float = 60.0
    http_max_retries: int = 4
    http_retry_backoff_seconds: float = 2.0

    cache_ttl_seconds: int = 60 * 60 * 24  # 24h
    cache_max_entries: int = 10_000


class StorageSettings(BaseSettings):
    """Database / persistence configuration."""

    database_url: str = "sqlite:///./exoplanet.db"
    echo_sql: bool = False
    pool_size: int = 10
    max_overflow: int = 20

    @field_validator("database_url")
    @classmethod
    def _no_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("database_url must not be empty")
        return v


class APISettings(BaseSettings):
    """FastAPI server configuration."""

    host: str = "0.0.0.0"
    port: int = 8000
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])
    request_timeout_seconds: float = 120.0
    rate_limit_per_minute: int = 120


class AnalysisSettings(BaseSettings):
    """Analysis engine tuning parameters."""

    bls_min_period_days: float = 0.5
    bls_max_period_days: float = 100.0
    bls_frequency_factor: float = 5.0
    bls_duration_grid: list[float] = Field(
        default_factory=lambda: [0.05, 0.08, 0.12, 0.16, 0.2]
    )
    detrend_window_hours: float = 12.0
    sigma_clip: float = 5.0


class Settings(BaseSettings):
    """Root settings object composing all sub-sections."""

    model_config = SettingsConfigDict(
        env_prefix="EXOPLANET_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = "development"
    log_level: str = "INFO"
    log_json: bool = False
    data_dir: Path = Path("./data")

    data_sources: DataSourceSettings = Field(default_factory=DataSourceSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    api: APISettings = Field(default_factory=APISettings)
    analysis: AnalysisSettings = Field(default_factory=AnalysisSettings)

    @field_validator("data_dir")
    @classmethod
    def _ensure_data_dir(cls, v: Path) -> Path:
        v.mkdir(parents=True, exist_ok=True)
        return v


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a mapping at top level")
    return data


@lru_cache(maxsize=1)
def get_settings(config_file: str | None = None) -> Settings:
    """Return cached application settings. Merges YAML (if any) with env/defaults."""
    overrides: dict[str, Any] = {}
    if config_file:
        overrides = _load_yaml(Path(config_file))
    return Settings(**overrides)


def reset_settings_cache() -> None:
    """Reset settings cache - useful in tests."""
    get_settings.cache_clear()
