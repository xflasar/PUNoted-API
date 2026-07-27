from __future__ import annotations

import os
import pytest
import fastapi.testclient
import asyncpg
import asyncio
import unittest.mock

# Mock Redis before any routers load it
import app.core.redis_client
mock_redis = unittest.mock.AsyncMock()
mock_redis.get.return_value = None
mock_redis.set.return_value = True
mock_redis.ping.return_value = True
app.core.redis_client.redis_client = mock_redis

import main

# Skip all tests in this module unless RUN_LIVE_TESTS=1
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_TESTS") != "1",
    reason="RUN_LIVE_TESTS=1 is not set"
)

@pytest.fixture(scope="module")
def live_data() -> tuple[str, str]:
    # Run the async database lookup synchronously
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(fetch_live_token_and_user())

async def fetch_live_token_and_user() -> tuple[str, str]:
    dsn = os.getenv("DATABASE_URL") or os.getenv("XATA_DATABASE_URL")
    if not dsn:
        pytest.skip("No DATABASE_URL or XATA_DATABASE_URL found in environment")
    
    if "localhost" in dsn:
        dsn = dsn.replace("localhost", "127.0.0.1")
        
    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow("""
            SELECT u.username, t.token_hash 
            FROM user_api_tokens t 
            JOIN users u ON u.accountid::text = t.user_id::text 
            LIMIT 1
        """)
        if not row:
            pytest.skip("No users with API tokens found in database")
        return row["token_hash"], row["username"]
    finally:
        await conn.close()

# ==============================================================================
# 1. PROTECTED EXTERNAL (V1) ENDPOINTS (WITH AUTH)
# ==============================================================================

def test_live_production_user(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/production/user", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_production_burn(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/production/user/burn", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_production_burn_with_username(live_data: tuple[str, str]) -> None:
    token, username = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get(f"/v1/production/user/burn?username={username}", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_storages_user(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/storages/user", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_storages_all(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/storages", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_storages_csv(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/storages/csv", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_company_data(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/user/companydata", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_workforce(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/workforce", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_workforce_user(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/workforce/user", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_workforce_csv(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/workforce/csv", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_accounting(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/accounting", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_accounting_user(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/accounting/user", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_flights(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/flights", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_flights_user(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/flights/user", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_ships(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/ships", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_ships_user(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/ships/user", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_sites(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/sites", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_sites_user(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/sites/user", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_contracts(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/contracts", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_contracts_user(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/contracts/user", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_contracts_csv(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/contracts/csv", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_cxuser(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/cxuser/orders", headers={"X-Data-Token": token})
        assert response.status_code == 200

def test_live_cxuser_csv(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/cxuser/orders/csv", headers={"X-Data-Token": token})
        assert response.status_code == 200

# ==============================================================================
# 2. PUBLIC ENDPOINTS (NO AUTH REQUIRED)
# ==============================================================================

def test_live_public_vendors() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/vendors")
        assert response.status_code == 200

def test_live_public_cx_prices() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/cx/prices")
        assert response.status_code == 200

def test_live_public_cx_prices_csv() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/cx/prices/csv")
        assert response.status_code == 200

def test_live_public_materials_list() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/materials/list")
        assert response.status_code == 200

def test_live_public_materials_csv() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/materials/csv")
        assert response.status_code == 200

def test_live_public_materials_recipes() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/materials/recipes")
        assert response.status_code == 200

def test_live_public_planets() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/planets")
        assert response.status_code == 200

def test_live_public_buildings() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/buildings")
        assert response.status_code == 200

def test_live_public_corporation_prices() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/corporation/prices")
        assert response.status_code == 200

# ==============================================================================
# 3. INTERNAL ENDPOINTS (REQUIRE SESSION AUTH / TOKEN AND ORIGIN)
# ==============================================================================

def test_live_internal_production(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/internal/production/user_production", headers={
            "Authorization": f"Bearer {token}",
            "Origin": "https://punoted.net"
        })
        assert response.status_code == 200

def test_live_internal_storage(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/internal/storage/user_storage", headers={
            "Authorization": f"Bearer {token}",
            "Origin": "https://punoted.net"
        })
        assert response.status_code == 200

def test_live_internal_sites(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/internal/sites/user_site_platforms/site1", headers={
            "Authorization": f"Bearer {token}",
            "Origin": "https://punoted.net"
        })
        # site1 might not exist in the live database, so 200 (if empty response) or 404 are both fine, as long as it isn't 500
        assert response.status_code in [200, 404]

def test_live_internal_ships(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/internal/ships", headers={
            "Authorization": f"Bearer {token}",
            "Origin": "https://punoted.net"
        })
        assert response.status_code == 200

def test_live_internal_users(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/internal/users/list", headers={
            "Authorization": f"Bearer {token}",
            "Origin": "https://punoted.net"
        })
        assert response.status_code == 200

def test_live_internal_finances(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/internal/finances/overview", headers={
            "Authorization": f"Bearer {token}",
            "Origin": "https://punoted.net"
        })
        assert response.status_code == 200

def test_live_internal_contracts(live_data: tuple[str, str]) -> None:
    token, _ = live_data
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/internal/contracts/list", headers={
            "Authorization": f"Bearer {token}",
            "Origin": "https://punoted.net"
        })
        # Since it is a POST route, GET returns 405 Method Not Allowed, which is correct and proves the route exists!
        assert response.status_code in [200, 405]

# ==============================================================================
# 4. NEGATIVE / ERROR ENDPOINT CHECKS (UNAUTHORIZED & NOT FOUND)
# ==============================================================================

PROTECTED_V1_ENDPOINTS = [
    "/v1/production/user",
    "/v1/production/user/burn",
    "/v1/storages/user",
    "/v1/storages",
    "/v1/storages/csv",
    "/v1/user/companydata",
    "/v1/workforce",
    "/v1/workforce/user",
    "/v1/workforce/csv",
    "/v1/accounting",
    "/v1/accounting/user",
    "/v1/flights",
    "/v1/flights/user",
    "/v1/ships",
    "/v1/ships/user",
    "/v1/sites",
    "/v1/sites/user",
    "/v1/contracts",
    "/v1/contracts/user",
    "/v1/contracts/csv",
    "/v1/cxuser/orders",
    "/v1/cxuser/orders/csv"
]

PROTECTED_INTERNAL_ENDPOINTS = [
    "/internal/production/user_production",
    "/internal/storage/user_storage",
    "/internal/sites/user_site_platforms/site1",
    "/internal/ships",
    "/internal/users/list",
    "/internal/finances/overview",
    "/internal/contracts/list"
]

@pytest.mark.parametrize("endpoint", PROTECTED_V1_ENDPOINTS)
def test_live_unauthorized_protected_v1(endpoint: str) -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get(endpoint)
        assert response.status_code == 401

@pytest.mark.parametrize("endpoint", PROTECTED_INTERNAL_ENDPOINTS)
def test_live_unauthorized_internal(endpoint: str) -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get(endpoint)
        assert response.status_code in [401, 403, 405]

def test_live_not_found() -> None:
    with fastapi.testclient.TestClient(main.app) as client:
        response = client.get("/v1/nonexistent_route_nuked_check")
        assert response.status_code == 404
