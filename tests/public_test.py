from __future__ import annotations

import typing

import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

# ----------------- Vendors -----------------
def test_get_vendors(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/vendors/")
    assert response.status_code == 200
    assert response.json() == [{"CompanyCode": "FAKE", "CompanyName": "fake co", "CorporationName": "fake corp", "PlayerName": "testuser"}]

# ----------------- CX -----------------
def test_get_cx_prices_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cx/prices/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]

def test_get_cx_prices_json(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cx/prices")
    assert response.status_code == 200
    assert response.json() == [{"Ticker": "H2O", "MMBuy": 100.0, "MMSell": 100.0, "AI1-Average": 100.0, "AI1-AskAmt": 100.0, "AI1-AskPrice": 100.0, "AI1-AskAvail": 100.0, "AI1-BidAmt": 100.0, "AI1-BidPrice": 100.0, "AI1-BidAvail": 100.0, "CI1-Average": 100.0, "CI1-AskAmt": 100.0, "CI1-AskPrice": 100.0, "CI1-AskAvail": 100.0, "CI1-BidAmt": 100.0, "CI1-BidPrice": 100.0, "CI1-BidAvail": 100.0, "CI2-Average": 100.0, "CI2-AskAmt": 100.0, "CI2-AskPrice": 100.0, "CI2-AskAvail": 100.0, "CI2-BidAmt": 100.0, "CI2-BidPrice": 100.0, "CI2-BidAvail": 100.0, "NC1-Average": 100.0, "NC1-AskAmt": 100.0, "NC1-AskPrice": 100.0, "NC1-AskAvail": 100.0, "NC1-BidAmt": 100.0, "NC1-BidPrice": 100.0, "NC1-BidAvail": 100.0, "NC2-Average": 100.0, "NC2-AskAmt": 100.0, "NC2-AskPrice": 100.0, "NC2-AskAvail": 100.0, "NC2-BidAmt": 100.0, "NC2-BidPrice": 100.0, "NC2-BidAvail": 100.0, "IC1-Average": 100.0, "IC1-AskAmt": 100.0, "IC1-AskPrice": 100.0, "IC1-AskAvail": 100.0, "IC1-BidAmt": 100.0, "IC1-BidPrice": 100.0, "IC1-BidAvail": 100.0}]

# ----------------- Materials -----------------
def test_get_materials_list(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/materials/list")
    assert response.status_code == 200
    assert response.json() == [{"ticker": "H2O", "name": "Water", "category": "Water", "weight": 1.0, "volume": 1.0}]

def test_get_materials_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/materials/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]

def test_get_material_recipes(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/materials/recipes")
    assert response.status_code == 200
    assert response.json() == [{"RecipeName": "Water extraction"}]

# ----------------- Planets -----------------
def test_get_planets(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/planets/")
    assert response.status_code == 200
    assert response.json() == [{"PlanetNaturalId": "p1", "PlanetName": "Planet 1"}]

# ----------------- Corp -----------------
def test_get_corporation_prices_json(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/corporation/prices")
    assert response.status_code == 200

def test_get_corporation_members_endpoint(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/corporation/members", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert response.json()["success"] is True

def test_get_ship_presets_endpoint(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/corporation/ship-presets?corporation_id=COSM")
    assert response.status_code == 200

def test_get_ship_orders_endpoint(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/corporation/ship-orders?corporation_id=COSM")
    assert response.status_code == 200

def test_get_ship_order_by_pin_endpoint(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/corporation/ship-orders/by-pin?corporation_id=COSM&pin=1234")
    assert response.status_code == 200

def test_get_user_role_endpoint(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/corporation/user-role?corporation_id=COSM")
    assert response.status_code == 200

# ----------------- Company -----------------
def test_get_public_company(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/company/FAKE")
    assert response.status_code == 200
    assert response.json() == {"CompanyName": "fake co"}

# ----------------- Buildings -----------------
def test_get_buildings(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/buildings/")
    assert response.status_code == 200
    assert response.json() == [{"BuildingTicker": "RIG"}]
