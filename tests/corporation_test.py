from __future__ import annotations

import typing

import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_corporation_production_overview(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/corporation/production", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert response.json()["success"] is True

def test_corporation_production_overview_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/corporation/production/csv", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]

def test_corporation_fuel_consumption(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    from tests.db_fixtures import INTERNAL_AUTH_HEADERS
    response = client.get("/internal/corporation/fuel-consumption?days=7", headers=INTERNAL_AUTH_HEADERS)
    assert response.status_code == 200
    data = response.json()
    assert "total_flights" in data
    assert "total_stl" in data
    assert "total_ftl" in data
    assert "corp_pricing" in data
    assert "cx_pricing" in data
    assert "savings_vs_cx" in data

def test_corporation_trade_summary(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    from tests.db_fixtures import INTERNAL_AUTH_HEADERS
    response = client.get("/internal/corporation/trade-summary?days=7", headers=INTERNAL_AUTH_HEADERS)
    assert response.status_code == 200
    data = response.json()
    assert "days" in data
    assert "trades" in data
    assert isinstance(data["trades"], dict)

