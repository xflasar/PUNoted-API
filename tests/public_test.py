from __future__ import annotations

import typing

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

# ----------------- Vendors -----------------
def test_get_vendors(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/vendors/")
    assert response.status_code == 200
    res = response.json()
    assert res.get("success") is True
    assert isinstance(res.get("vendors"), list)

def test_get_vendors_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/vendors/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]

# ----------------- CX -----------------
def test_get_cx_prices_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cx/prices/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]

def test_get_cx_prices_json(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cx/prices")
    assert response.status_code == 200
    res = response.json()
    assert isinstance(res, list)
    if len(res) > 0:
        assert "ticker" in res[0] or "Ticker" in res[0]

def test_get_cx_detail(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cx/detail/H2O")
    assert response.status_code == 200
    assert isinstance(response.json(), (dict, list))

def test_get_cx_history(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/cx/history/H2O")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

# ----------------- Materials -----------------
def test_get_materials_list(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/materials/list")
    assert response.status_code == 200
    tickers = [m["ticker"] for m in response.json()]
    assert "H2O" in tickers

def test_get_materials_csv(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/materials/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]

def test_get_material_recipes(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/materials/recipes")
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert "2xH = 1xH2O" in [r["RecipeName"] for r in response.json()]

# ----------------- Planets -----------------
def test_get_planets(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/planets/")
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["PlanetNaturalId"] == "HRT"
    assert response.json()[0]["PlanetName"] == "Hortus"

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
    assert response.json()["CompanyName"] == "fake co"
    assert response.json()["CompanyCode"] == "FAKE"

# ----------------- Buildings -----------------
def test_get_buildings(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/buildings/")
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["Ticker"] == "RIG"

# ----------------- Governance -----------------
def test_get_governance_terms(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/governance/terms")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_governance_motions(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/governance/motions?full=true")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

# ----------------- Public Shipments -----------------
def test_get_public_shipment_not_found(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/public/shipments/NONEXISTENT?company=PRO")
    assert response.status_code == 404
    data = response.json()
    assert data["success"] is False

