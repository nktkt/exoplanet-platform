"""Shared HTTP client with retries, timeout, and simple TTL cache.

Use `get_http_client()` (cached) from ingestion modules. Do NOT instantiate
httpx.Client directly elsewhere — consistency in retry/backoff matters.
"""

from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from typing import Any

import httpx
from cachetools import TTLCache
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import get_settings
from .exceptions import (
    DataSourceQuotaError,
    DataSourceUnavailableError,
)
from .logging_config import get_logger

log = get_logger(__name__)


class HTTPClient:
    """Thin wrapper around httpx.Client with retry + response cache."""

    def __init__(
        self,
        timeout: float,
        max_retries: int,
        backoff: float,
        cache_ttl: int,
        cache_size: int,
    ) -> None:
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "exoplanet-platform/0.1"},
        )
        self._max_retries = max_retries
        self._backoff = backoff
        self._cache: TTLCache[str, httpx.Response] = TTLCache(
            maxsize=cache_size, ttl=cache_ttl
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HTTPClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @staticmethod
    def _cache_key(method: str, url: str, params: Any, data: Any) -> str:
        payload = json.dumps(
            {"method": method, "url": url, "params": params, "data": data},
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, str] | None = None,
        use_cache: bool = True,
    ) -> httpx.Response:
        key = self._cache_key(method, url, params, data)
        if use_cache and method.upper() == "GET" and key in self._cache:
            log.debug("http.cache_hit", url=url)
            return self._cache[key]

        response = self._request_with_retry(method, url, params, data, headers)

        if use_cache and method.upper() == "GET" and response.status_code == 200:
            self._cache[key] = response
        return response

    def _request_with_retry(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | str | None,
        headers: dict[str, str] | None,
    ) -> httpx.Response:
        @retry(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=self._backoff, min=1.0, max=30.0),
            retry=retry_if_exception_type(
                (httpx.TimeoutException, httpx.NetworkError, DataSourceUnavailableError)
            ),
            reraise=True,
        )
        def _do() -> httpx.Response:
            log.debug("http.request", method=method, url=url, params=params)
            try:
                resp = self._client.request(
                    method, url, params=params, data=data, headers=headers
                )
            except httpx.HTTPError as e:
                raise DataSourceUnavailableError(f"HTTP error for {url}: {e}") from e

            if resp.status_code == 429:
                raise DataSourceQuotaError(f"Rate-limited by {url}")
            if 500 <= resp.status_code < 600:
                raise DataSourceUnavailableError(
                    f"Upstream {url} returned {resp.status_code}"
                )
            return resp

        return _do()

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def clear_cache(self) -> None:
        self._cache.clear()


@lru_cache(maxsize=1)
def get_http_client() -> HTTPClient:
    """Return a process-wide HTTP client configured from settings."""
    s = get_settings().data_sources
    return HTTPClient(
        timeout=s.http_timeout_seconds,
        max_retries=s.http_max_retries,
        backoff=s.http_retry_backoff_seconds,
        cache_ttl=s.cache_ttl_seconds,
        cache_size=s.cache_max_entries,
    )
