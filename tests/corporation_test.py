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
