from __future__ import annotations

import typing

import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_search_user_ships(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/ships", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["Username"] == "testuser"

def test_search_user_ships_single(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/ships/user", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["ShipId"] == "s1"
