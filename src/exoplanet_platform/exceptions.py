"""Domain-specific exception hierarchy.

All modules should raise these (or subclasses) rather than bare `Exception`,
so callers (API, CLI) can map them to user-facing errors reliably.
"""

from __future__ import annotations


class ExoplanetPlatformError(Exception):
    """Base class for all platform errors."""


class ConfigurationError(ExoplanetPlatformError):
    """Raised when configuration is invalid or missing."""


class DataSourceError(ExoplanetPlatformError):
    """Base class for errors fetching from external data sources."""


class DataSourceUnavailableError(DataSourceError):
    """External service is unreachable or returned a transient error."""


class DataSourceNotFoundError(DataSourceError):
    """Requested object does not exist in the upstream catalog."""


class DataSourceQuotaError(DataSourceError):
    """External service rate-limited or quota-exceeded."""


class ValidationError(ExoplanetPlatformError):
    """Input validation failed (bad planet id, out-of-range params, etc.)."""


class AnalysisError(ExoplanetPlatformError):
    """Analysis pipeline failure (not enough data, convergence failure, etc.)."""


class InsufficientDataError(AnalysisError):
    """Not enough data points / signal-to-noise for a reliable analysis."""


class StorageError(ExoplanetPlatformError):
    """Database or cache operation failed."""
