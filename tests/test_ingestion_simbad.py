"""Tests for the SIMBAD name-resolver client."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from exoplanet_platform.exceptions import (
    DataSourceNotFoundError,
    DataSourceUnavailableError,
    ValidationError,
)
from exoplanet_platform.ingestion.simbad import (
    ResolvedObject,
    SimbadClient,
    _parse_ids_field,
    _prefix_of,
)


class _FakeRow:
    """Dict-like shim that mimics astropy Table rows for test purposes."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def __getitem__(self, key: str) -> Any:
        if key in self._data:
            return self._data[key]
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class _FakeTable:
    """Minimal `len(...)` + indexing facade over a list of rows."""

    def __init__(self, rows: list[_FakeRow]):
        self._rows = rows

    def __len__(self) -> int:
        return len(self._rows)

    def __getitem__(self, index: int) -> _FakeRow:
        return self._rows[index]


class _FakeSimbad:
    TIMEOUT = 30
    next_result: _FakeTable | None = None
    raise_on_query: Exception | None = None

    def add_votable_fields(self, *_: str) -> None:
        pass

    def query_object(self, _name: str) -> _FakeTable | None:
        if _FakeSimbad.raise_on_query is not None:
            raise _FakeSimbad.raise_on_query
        return _FakeSimbad.next_result


def _install_fake_simbad(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub out astroquery.simbad.Simbad with our fake class."""
    # Reset state between cases.
    _FakeSimbad.next_result = None
    _FakeSimbad.raise_on_query = None
    # `Simbad` is imported lazily inside `resolve`/`health_check`, so we patch
    # the real astroquery module instead of trying to stub the symbol inside
    # exoplanet_platform.ingestion.simbad.
    import astroquery.simbad as aq_simbad

    monkeypatch.setattr(aq_simbad, "Simbad", _FakeSimbad)


def test_prefix_of_gaia_dr3() -> None:
    assert _prefix_of("Gaia DR3 1234567") == "Gaia DR3"


def test_prefix_of_single_word() -> None:
    assert _prefix_of("HD") == "HD"


def test_prefix_of_two_word() -> None:
    assert _prefix_of("HD 209458") == "HD"


def test_parse_ids_field_extracts_gaia() -> None:
    row = _FakeRow({"ids": "HD 209458|Gaia DR3 1234567|TYC 1-2-3"})
    ids = _parse_ids_field(row)
    assert ids["Gaia DR3"] == "Gaia DR3 1234567"
    assert ids["HD"] == "HD 209458"


def test_resolved_object_prefers_gaia_dr3() -> None:
    r = ResolvedObject(
        name="Kepler-10",
        ra_deg=285.0,
        dec_deg=50.0,
        identifiers={"Gaia DR3": "Gaia DR3 2131146452016919424"},
    )
    assert r.gaia_dr3_source_id == 2131146452016919424
    assert r.gaia_dr2_source_id is None


def test_resolved_object_falls_back_to_dr2() -> None:
    r = ResolvedObject(
        name="X",
        ra_deg=None,
        dec_deg=None,
        identifiers={"Gaia DR2": "Gaia DR2 999"},
    )
    assert r.gaia_dr3_source_id is None
    assert r.gaia_dr2_source_id == 999


def test_resolve_empty_name_raises_validation() -> None:
    with pytest.raises(ValidationError):
        SimbadClient().resolve("  ")


def test_resolve_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_simbad(monkeypatch)
    _FakeSimbad.next_result = _FakeTable(
        [
            _FakeRow(
                {
                    "main_id": "Kepler-10",
                    "ra": 285.6794,
                    "dec": 50.2414,
                    "ids": "Kepler-10|HD 100|Gaia DR3 2131146452016919424",
                }
            )
        ]
    )
    resolved = SimbadClient().resolve("Kepler-10")
    assert resolved.ra_deg == pytest.approx(285.6794)
    assert resolved.dec_deg == pytest.approx(50.2414)
    assert resolved.gaia_dr3_source_id == 2131146452016919424


def test_resolve_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_simbad(monkeypatch)
    _FakeSimbad.next_result = _FakeTable([])
    with pytest.raises(DataSourceNotFoundError):
        SimbadClient().resolve("definitely-not-a-real-object-xyzzy")


def test_resolve_service_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_simbad(monkeypatch)
    _FakeSimbad.raise_on_query = RuntimeError("connection reset")
    with pytest.raises(DataSourceUnavailableError):
        SimbadClient().resolve("Kepler-10")


def test_resolve_missing_ids_field_yields_empty_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_simbad(monkeypatch)
    _FakeSimbad.next_result = _FakeTable(
        [
            _FakeRow(
                {"main_id": "SomeStar", "ra": 10.0, "dec": -5.0}
            )
        ]
    )
    resolved = SimbadClient().resolve("SomeStar")
    assert resolved.identifiers == {}
    assert resolved.gaia_dr3_source_id is None


@pytest.mark.integration
def test_resolve_real_simbad() -> None:
    """Live SIMBAD round-trip — deselected by default, run with `pytest -m integration`."""
    resolved = SimbadClient().resolve("Kepler-10")
    assert resolved.gaia_dr3_source_id is not None
    assert resolved.ra_deg is not None and 285 < resolved.ra_deg < 286
    assert resolved.dec_deg is not None and 50 < resolved.dec_deg < 51


def test_health_check_uses_stubbed_simbad(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_simbad(monkeypatch)
    _FakeSimbad.next_result = _FakeTable(
        [_FakeRow({"main_id": "Sun", "ra": 0.0, "dec": 0.0})]
    )
    assert SimbadClient().health_check() is True


def test_health_check_returns_false_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_simbad(monkeypatch)
    _FakeSimbad.raise_on_query = RuntimeError("boom")
    assert SimbadClient().health_check() is False


# ---------------------------------------------------------------------------
# GaiaClient.resolve_by_name smoke test via stubs.
# ---------------------------------------------------------------------------


def test_gaia_resolve_by_name_uses_gaia_dr3_from_simbad(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from exoplanet_platform.domain import Catalog, Star
    from exoplanet_platform.ingestion.gaia import GaiaClient

    stubbed = Star(
        identifier="Gaia DR3 123",
        ra_deg=10.0,
        dec_deg=-5.0,
        effective_temperature_k=5700.0,
        radius_solar=1.0,
        mass_solar=1.0,
        catalog=Catalog.GAIA,
    )

    def fake_resolve(self, name: str) -> ResolvedObject:
        return ResolvedObject(
            name=name,
            ra_deg=10.0,
            dec_deg=-5.0,
            identifiers={"Gaia DR3": "Gaia DR3 123"},
        )

    with (
        patch.object(SimbadClient, "resolve", fake_resolve),
        patch.object(GaiaClient, "query_star", return_value=stubbed),
    ):
        star = GaiaClient().resolve_by_name("Foo Star")

    assert star.identifier == "Foo Star"  # re-stamped to the requested name
    assert star.effective_temperature_k == 5700.0
