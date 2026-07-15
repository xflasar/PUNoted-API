from __future__ import annotations

import typing

import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_get_orders_json(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cxuser/orders", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["Username"] == "testuser"

def test_get_order_user(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cxuser/orders/user", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["order_id"] == "o1"

def test_get_orders_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cxuser/orders/csv", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "testuser" in response.text
