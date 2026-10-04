from __future__ import annotations

import typing

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_get_storages_json(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/storages", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    (user_storage,) = response.json()
    assert user_storage["Username"] == "testuser"
    (storage,) = user_storage["Storages"]
    assert storage["Location"] == "Hortus"
    (item,) = storage["StorageItems"]
    assert item["MaterialTicker"] == "RAT"
    assert item["MaterialAmount"] == 10

def test_get_storages_user(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/storages/user", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    (storage,) = response.json()
    assert storage["Location"] == "Hortus"
    (item,) = storage["StorageItems"]
    assert item["MaterialTicker"] == "RAT"
    assert item["MaterialAmount"] == 10

def test_get_storages_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/storages/csv", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "testuser" in response.text
