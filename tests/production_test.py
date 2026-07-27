from __future__ import annotations

import typing

import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_search_production(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/production", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["Username"] == "testuser"

def test_search_production_user(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/production/user", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200

def test_search_burn_production_user(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/production/user/burn", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200

def test_search_simple_production_user(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/production/user/production/simple", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200

def test_get_user_production_internal(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/internal/production/user_production", headers={"Authorization": "Bearer ptk_fake", "Origin": "http://localhost:5174"})
    assert response.status_code == 200
    assert response.json()["success"] is True

def test_search_production_user_with_username(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/production/user?username=testuser", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200

def test_search_burn_production_user_with_username(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/production/user/burn?username=testuser", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
