"""Tests for the shared HTTP client (retry, cache, quota mapping).

Never hits the network — the underlying `httpx.Client.request` is monkey-patched
so we can count calls and script responses.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from exoplanet_platform.exceptions import DataSourceQuotaError
from exoplanet_platform.http import HTTPClient


def _make_client() -> HTTPClient:
    # Tight retry/backoff to keep the test fast.
    return HTTPClient(
        timeout=1.0,
        max_retries=3,
        backoff=0.0,  # wait_exponential's min/max still apply but we aren't asserting time.
        cache_ttl=60,
        cache_size=16,
    )


def _response(status: int, json_body: Any = None) -> httpx.Response:
    req = httpx.Request("GET", "https://example.test/x")
    return httpx.Response(
        status_code=status,
        json=json_body if json_body is not None else {},
        request=req,
    )


def test_retry_on_500_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client()
    calls: list[tuple[str, str]] = []

    def fake_request(method: str, url: str, **_: Any) -> httpx.Response:
        calls.append((method, url))
        if len(calls) < 2:
            return _response(500)
        return _response(200, [{"ok": True}])

    monkeypatch.setattr(client._client, "request", fake_request)

    resp = client.get("https://example.test/x")
    assert resp.status_code == 200
    assert len(calls) == 2


def test_429_raises_quota_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client()
    mock_req = MagicMock(return_value=_response(429))
    monkeypatch.setattr(client._client, "request", mock_req)

    with pytest.raises(DataSourceQuotaError):
        client.get("https://example.test/x")
    # Quota error is not retried.
    assert mock_req.call_count == 1


def test_cache_hit_returns_same_response(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client()
    mock_req = MagicMock(return_value=_response(200, [{"hi": 1}]))
    monkeypatch.setattr(client._client, "request", mock_req)

    r1 = client.get("https://example.test/x", params={"q": "a"})
    r2 = client.get("https://example.test/x", params={"q": "a"})
    assert r1 is r2
    assert mock_req.call_count == 1


def test_cache_miss_for_different_params(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client()
    mock_req = MagicMock(return_value=_response(200, [{"hi": 1}]))
    monkeypatch.setattr(client._client, "request", mock_req)

    client.get("https://example.test/x", params={"q": "a"})
    client.get("https://example.test/x", params={"q": "b"})
    assert mock_req.call_count == 2


def test_cache_disabled_via_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client()
    mock_req = MagicMock(return_value=_response(200, [{"ok": 1}]))
    monkeypatch.setattr(client._client, "request", mock_req)

    client.get("https://example.test/x", use_cache=False)
    client.get("https://example.test/x", use_cache=False)
    assert mock_req.call_count == 2


def test_post_is_not_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client()
    mock_req = MagicMock(return_value=_response(200))
    monkeypatch.setattr(client._client, "request", mock_req)

    client.post("https://example.test/x", data={"a": 1})
    client.post("https://example.test/x", data={"a": 1})
    assert mock_req.call_count == 2


def test_clear_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client()
    mock_req = MagicMock(return_value=_response(200))
    monkeypatch.setattr(client._client, "request", mock_req)

    client.get("https://example.test/x")
    client.clear_cache()
    client.get("https://example.test/x")
    assert mock_req.call_count == 2
