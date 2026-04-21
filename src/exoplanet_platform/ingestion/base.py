"""Abstract base class for external data-source clients.

All ingestion clients inherit from `DataSourceClient` so that higher-level
code (API, CLI, pipelines) can treat them uniformly and perform health checks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from ..domain import Catalog
from ..exceptions import (
    DataSourceNotFoundError,
    DataSourceQuotaError,
    DataSourceUnavailableError,
)
from ..logging_config import get_logger

log = get_logger(__name__)


class DataSourceClient(ABC):
    """Abstract contract every ingestion client must satisfy.

    Concrete subclasses expose a catalog-specific ``name`` and a
    lightweight ``health_check`` used by monitoring code to verify the
    upstream service is reachable before dispatching real queries.
    """

    @property
    @abstractmethod
    def name(self) -> Catalog:
        """Return the Catalog enum value identifying this data source."""

    @abstractmethod
    def health_check(self) -> bool:
        """Return True if the upstream data source is reachable.

        Implementations should issue the cheapest possible request
        (``SELECT 1``-style TAP query or a bare metadata call) and return
        False on any error rather than propagating it.
        """

    @staticmethod
    def _handle_response(response: httpx.Response) -> httpx.Response:
        """Translate an HTTP status code into the appropriate domain exception.

        Args:
            response: An ``httpx.Response`` returned by the shared HTTP client.

        Returns:
            The same response, if the status code indicates success.

        Raises:
            DataSourceNotFoundError: If the upstream returned 404.
            DataSourceQuotaError: If the upstream returned 429.
            DataSourceUnavailableError: For 5xx and other non-success codes.
        """
        status = response.status_code
        if 200 <= status < 300:
            return response
        url = str(response.request.url) if response.request else "<unknown>"
        if status == 404:
            raise DataSourceNotFoundError(f"Resource not found at {url}")
        if status == 429:
            raise DataSourceQuotaError(f"Rate-limited by {url}")
        if 500 <= status < 600:
            raise DataSourceUnavailableError(
                f"Upstream {url} returned {status}: {response.text[:200]}"
            )
        raise DataSourceUnavailableError(
            f"Unexpected status {status} from {url}: {response.text[:200]}"
        )
