from __future__ import annotations

import typing

import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_get_contracts(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/contracts", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["Username"] == "testuser"

def test_get_contract_user(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/contracts/user", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200

def test_export_contracts_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/contracts/csv", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "ContractId,LocalId" in response.text

def test_export_user_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/contracts/user/csv", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "ContractId,LocalId" in response.text
