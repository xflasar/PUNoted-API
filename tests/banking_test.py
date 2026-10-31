from __future__ import annotations

import typing
import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient


def test_get_public_banks(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:
    """Test public banking directory endpoint /banking."""
    response = client.get("/banking")
    assert response.status_code == 200
    banks = response.json()
    assert isinstance(banks, list)
    assert len(banks) > 0
    assert any(b["name"] == "Test Reserve Bank" for b in banks)


def test_get_public_bank_detail(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:
    """Test public bank detail inspection /banking/{id}."""
    response = client.get("/banking/101")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == 101
    assert data["name"] == "Test Reserve Bank"


def test_internal_get_bank_ledger(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:
    """Test bank ledger inspection /internal/banking/{id}/ledger."""
    response = client.get("/internal/banking/101/ledger", headers={"X-Internal-Origin": "punoted-internal"})
    assert response.status_code == 200
    data = response.json()
    assert data["bank_id"] == 101
    assert data["bank_name"] == "Test Reserve Bank"
    assert "history" in data
    assert isinstance(data["history"], list)


def test_get_protected_user_banking(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:
    """Test protected external banking API /banking with api token."""
    response = client.get("/banking", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


def test_internal_get_banks(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:
    """Test internal banking list endpoint /internal/banking."""
    response = client.get("/internal/banking", headers={"X-Internal-Origin": "punoted-internal"})
    assert response.status_code == 200
    banks = response.json()
    assert isinstance(banks, list)
    assert len(banks) > 0


def test_internal_get_bank_by_id(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:
    """Test internal bank details /internal/banking/{id}."""
    response = client.get("/internal/banking/101", headers={"X-Internal-Origin": "punoted-internal"})
    assert response.status_code == 200
    bank = response.json()
    assert bank["id"] == 101
    assert bank["name"] == "Test Reserve Bank"
    assert bank["default_interest_rate"] == 4.5
