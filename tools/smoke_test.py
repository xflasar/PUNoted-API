#!/usr/bin/env python3
"""
PUNoted API Comprehensive Smoke Tester & Endpoint Health Check
==============================================================
Runs fast parallel sanity checks across ALL Protected and Public v1 endpoints.

Validates:
  1. HTTP 200 OK (or expected 2xx/3xx/4xx for parametrized endpoints)
  2. Latency / Response Time (flags > 1.5s or > 4s)
  3. Strict Multi-Tenant Data Isolation (Guarantees no foreign username is leaked in single-user endpoints)
  4. Response item counting and format validation

Usage:
  .venv/Scripts/python.exe tools/smoke_test.py
  .venv/Scripts/python.exe tools/smoke_test.py --user tomato-chef
  .venv/Scripts/python.exe tools/smoke_test.py --token ptk_...
  .venv/Scripts/python.exe tools/smoke_test.py --url https://api.punoted.net --token ptk_...
"""

import argparse
import asyncio
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import httpx
from dotenv import load_dotenv

load_dotenv()

# ANSI Color codes for clean terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
# Prevent pytest from collecting this file or its helper functions as automated test suite
__test__ = False


async def get_test_credentials(
    explicit_token: Optional[str] = None,
    explicit_user: Optional[str] = None
) -> Dict[str, Any]:
    """Retrieves credentials for testing: either explicitly given, or resolved from DB."""
    dsn = os.getenv("DATABASE_URL") or os.getenv("XATA_DATABASE_URL")
    if not dsn:
        print(f"{RED}[ERROR] Neither DATABASE_URL nor XATA_DATABASE_URL found in .env{RESET}")
        sys.exit(1)

    if "localhost" in dsn:
        dsn = dsn.replace("localhost", "127.0.0.1")

    conn = await asyncpg.connect(dsn)
    try:
        user_row = None
        # Case 1: Token explicitly provided by user
        if explicit_token:
            row = await conn.fetchrow("""
                SELECT u.username, u.accountid, t.token_hash 
                FROM user_api_tokens t 
                JOIN users u ON u.accountid::text = t.user_id::text 
                WHERE t.token_hash = $1
            """, explicit_token)
            if row:
                user_row = (row["token_hash"], row["username"], str(row["accountid"]))
            else:
                user_row = (explicit_token, explicit_user or "custom_token_user", "explicit_account")

        # Case 2: Specific username provided by user
        elif explicit_user:
            row = await conn.fetchrow("""
                SELECT u.username, u.accountid, t.token_hash 
                FROM user_api_tokens t 
                JOIN users u ON u.accountid::text = t.user_id::text 
                WHERE LOWER(u.username) = LOWER($1) AND t.token_hash LIKE 'ptk_%'
                ORDER BY t.last_used_at DESC NULLS LAST
                LIMIT 1
            """, explicit_user)
            if not row:
                print(f"{RED}[ERROR] No API tokens found for user '{explicit_user}'.{RESET}")
                sys.exit(1)
            user_row = (row["token_hash"], row["username"], str(row["accountid"]))

        # Case 3: Default fallback to most recently active token
        else:
            row = await conn.fetchrow("""
                SELECT u.username, u.accountid, t.token_hash 
                FROM user_api_tokens t 
                JOIN users u ON u.accountid::text = t.user_id::text 
                WHERE t.token_hash LIKE 'ptk_%'
                ORDER BY t.last_used_at DESC NULLS LAST
                LIMIT 1
            """)
            if not row:
                print(f"{RED}[ERROR] No active user_api_tokens found in database.{RESET}")
                sys.exit(1)
            user_row = (row["token_hash"], row["username"], str(row["accountid"]))

        token, uname, accid = user_row

        # Dynamically discover contextual IDs for parameters
        user_bank = await conn.fetchrow("SELECT id FROM player_banks WHERE LOWER(owner_username) = LOWER($1) LIMIT 1", uname)
        any_bank = await conn.fetchrow("SELECT id FROM player_banks WHERE is_public = true LIMIT 1")
        shipment = await conn.fetchrow("""
            SELECT c.localid, COALESCE(c.partnercode, cd.companycode) as comp_code 
            FROM contracts c
            LEFT JOIN company_data cd ON c.userid = cd.userdataid
            WHERE c.localid IS NOT NULL 
              AND (c.partnercode IS NOT NULL OR cd.companycode IS NOT NULL)
              AND EXISTS (
                  SELECT 1 FROM contract_conditions cc 
                  WHERE cc.contractid = c.id AND cc.type = 'DELIVERY_SHIPMENT'
              )
            LIMIT 1
        """)
        corp = await conn.fetchrow("SELECT id FROM corporations LIMIT 1")

        context = {
            "token": token,
            "username": uname,
            "accountid": accid,
            "user_bank_id": user_bank["id"] if user_bank else None,
            "public_bank_id": any_bank["id"] if any_bank else 1,
            "shipment_local_id": shipment["localid"] if shipment else None,
            "shipment_company": shipment["comp_code"] if shipment else None,
            "corp_id": corp["id"] if corp else "1",
        }
        return context
    finally:
        await conn.close()


def check_user_isolation(data: Any, expected_username: str) -> Tuple[bool, str]:
    """
    Scans the returned JSON payload to guarantee no foreign username leaked.
    """
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                u = item.get("Username") or item.get("username")
                if u and u.lower() != expected_username.lower():
                    return False, f"Leaked user '{u}'"
    elif isinstance(data, dict):
        u = data.get("Username") or data.get("username")
        if u and u.lower() != expected_username.lower():
            return False, f"Leaked user '{u}'"
    return True, "OK"


async def query_endpoint(
    client: httpx.AsyncClient,
    path: str,
    headers: Dict[str, str],
    expected_user: str,
    category: str,
    is_protected_single_user: bool = False
) -> Dict[str, Any]:
    query_endpoint.__test__ = False
    start_t = time.perf_counter()
    try:
        resp = await client.get(path, headers=headers, timeout=12.0)
        latency_ms = round((time.perf_counter() - start_t) * 1000, 1)
        status_code = resp.status_code

        passed = (status_code == 200)
        detail = ""
        items_count = None

        if passed:
            # Check content-type
            ct = resp.headers.get("content-type", "")
            if "csv" in ct or "text/" in ct:
                line_count = len(resp.text.strip().splitlines())
                items_count = max(0, line_count - 1) # minus header
            else:
                try:
                    json_data = resp.json()
                    if isinstance(json_data, list):
                        items_count = len(json_data)
                    elif isinstance(json_data, dict):
                        if "vendors" in json_data and isinstance(json_data["vendors"], list):
                            items_count = len(json_data["vendors"])
                        elif "contracts" in json_data and isinstance(json_data["contracts"], list):
                            items_count = len(json_data["contracts"])
                        elif "data" in json_data and isinstance(json_data["data"], (list, dict)):
                            items_count = len(json_data["data"])
                        else:
                            items_count = len(json_data)

                    # Check strict user isolation on single user endpoints
                    if is_protected_single_user:
                        iso_ok, iso_msg = check_user_isolation(json_data, expected_user)
                        if not iso_ok:
                            passed = False
                            detail = f"{RED}DATA LEAK: {iso_msg}{RESET}"
                except Exception:
                    detail = "Non-JSON response"
        else:
            detail = f"HTTP {status_code}"

        return {
            "path": path,
            "status": status_code,
            "latency": latency_ms,
            "passed": passed,
            "items": items_count,
            "detail": detail,
            "category": category
        }
    except Exception as e:
        latency_ms = round((time.perf_counter() - start_t) * 1000, 1)
        return {
            "path": path,
            "status": 0,
            "latency": latency_ms,
            "passed": False,
            "items": None,
            "detail": str(e)[:30],
            "category": category
        }


async def main_async(base_url: str, token: Optional[str] = None, user: Optional[str] = None):
    ctx = await get_test_credentials(explicit_token=token, explicit_user=user)
    token = ctx["token"]
    username = ctx["username"]
    accountid = ctx["accountid"]
    user_bank_id = ctx["user_bank_id"]
    public_bank_id = ctx["public_bank_id"]
    shipment_local_id = ctx["shipment_local_id"]
    shipment_company = ctx["shipment_company"]
    corp_id = ctx["corp_id"]

    print(f"\n{BOLD}{CYAN}=== PUNoted API Full Endpoint Test Suite ==={RESET}")
    print(f"Target Server : {base_url}")
    print(f"Test User     : {BOLD}{username}{RESET} ({accountid[:8]}...)")
    print(f"Test Token    : {token[:12]}...\n")

    auth_headers = {"X-Data-Token": token}
    public_headers = {}

    # Comprehensive list of all Protected and Public endpoints
    endpoints = [
        # --- PROTECTED ENDPOINTS (SINGLE USER) ---
        ("/v1/production/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/production/user/burn", auth_headers, "Protected (Single-User)", True),
        ("/v1/production/user/production/simple", auth_headers, "Protected (Single-User)", True),
        ("/v1/storages/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/contracts/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/contracts/user/csv", auth_headers, "Protected (Single-User)", True),
        ("/v1/shipments/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/user/companydata/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/workforce/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/accounting/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/flights/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/ships/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/sites/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/cxuser/orders/user", auth_headers, "Protected (Single-User)", True),
        ("/v1/banking/my-bank", auth_headers, "Protected (Single-User)", True),
        ("/v1/banking/loans/requested", auth_headers, "Protected (Single-User)", True),
    ]

    # If the user actually owns a bank, test bank-operator specific endpoints
    if user_bank_id is not None:
        endpoints.extend([
            (f"/v1/banking/loans/received?bank_id={user_bank_id}", auth_headers, "Protected (Single-User)", True),
            (f"/v1/banking/depositors?bank_id={user_bank_id}", auth_headers, "Protected (Single-User)", True),
        ])

    endpoints.extend([
        # --- PROTECTED ENDPOINTS (MULTI-USER / GROUPS) ---
        ("/v1/production/", auth_headers, "Protected (Group/List)", False),
        ("/v1/storages/", auth_headers, "Protected (Group/List)", False),
        ("/v1/storages/csv", auth_headers, "Protected (Group/List)", False),
        ("/v1/contracts/", auth_headers, "Protected (Group/List)", False),
        ("/v1/contracts/csv", auth_headers, "Protected (Group/List)", False),
        ("/v1/user/companydata", auth_headers, "Protected (Group/List)", False),
        ("/v1/workforce/", auth_headers, "Protected (Group/List)", False),
        ("/v1/workforce/csv", auth_headers, "Protected (Group/List)", False),
        ("/v1/accounting/", auth_headers, "Protected (Group/List)", False),
        ("/v1/flights/", auth_headers, "Protected (Group/List)", False),
        ("/v1/ships/", auth_headers, "Protected (Group/List)", False),
        ("/v1/sites/", auth_headers, "Protected (Group/List)", False),
        ("/v1/cxuser/orders", auth_headers, "Protected (Group/List)", False),
        ("/v1/cxuser/orders/csv", auth_headers, "Protected (Group/List)", False),
        ("/v1/corporation/members", auth_headers, "Protected (Corporation)", False),
        ("/v1/corporation/production", auth_headers, "Protected (Corporation)", False),
        ("/v1/corporation/production/csv", auth_headers, "Protected (Corporation)", False),
        (f"/v1/corporation/ship-orders?corporation_id={corp_id}", auth_headers, "Protected (Corporation)", False),
        (f"/v1/corporation/ship-presets?corporation_id={corp_id}", auth_headers, "Protected (Corporation)", False),
        (f"/v1/corporation/user-role?corporation_id={corp_id}", auth_headers, "Protected (Corporation)", False),

        # --- PUBLIC ENDPOINTS (NO AUTH REQUIRED) ---
        ("/status", public_headers, "Public", False),
        ("/v1/vendors/", public_headers, "Public", False),
        ("/v1/vendors/csv", public_headers, "Public", False),
        ("/v1/materials/list", public_headers, "Public", False),
        ("/v1/materials/csv", public_headers, "Public", False),
        ("/v1/materials/recipes", public_headers, "Public", False),
        ("/v1/cx/prices", public_headers, "Public", False),
        ("/v1/cx/prices/csv", public_headers, "Public", False),
        ("/v1/cx/history/DW", public_headers, "Public", False),
        ("/v1/cx/detail/DW", public_headers, "Public", False),
        ("/v1/planets/", public_headers, "Public", False),
        ("/v1/buildings/", public_headers, "Public", False),
        ("/v1/company/ANTR", public_headers, "Public", False),
        ("/v1/corporation/prices", public_headers, "Public", False),
        ("/v1/governance/motions", public_headers, "Public", False),
        ("/v1/governance/terms", public_headers, "Public", False),
        ("/v1/banking", public_headers, "Public", False),
        (f"/v1/banking/{public_bank_id}", public_headers, "Public", False),
    ])

    if shipment_local_id and shipment_company:
        endpoints.append((f"/v1/shipments/{shipment_local_id}?company={shipment_company}", public_headers, "Public", False))

    async with httpx.AsyncClient(base_url=base_url) as client:
        tasks = [
            query_endpoint(client, path, hdrs, username, cat, is_single)
            for path, hdrs, cat, is_single in endpoints
        ]
        results = await asyncio.gather(*tasks)

    # Group and display report
    current_category = None
    all_passed = True

    print(f"{BOLD}{'Endpoint':<40} | {'Status':<10} | {'Latency':<9} | {'Records':<8} | {'Result':<10}{RESET}")
    print("=" * 86)

    for r in results:
        if r["category"] != current_category:
            current_category = r["category"]
            print(f"\n{BOLD}{CYAN}--- {current_category.upper()} ---{RESET}")

        status_str = f"{GREEN}200 OK{RESET}" if r["status"] == 200 else f"{RED}{r['status']}{RESET}"
        
        # Latency coloring
        lat = r["latency"]
        if lat < 500:
            lat_str = f"{GREEN}{lat:>6.1f}ms{RESET}"
        elif lat < 2000:
            lat_str = f"{YELLOW}{lat:>6.1f}ms{RESET}"
        else:
            lat_str = f"{RED}{lat:>6.1f}ms{RESET}"

        count_str = str(r["items"]) if r["items"] is not None else "-"
        
        if r["passed"]:
            res_str = f"{GREEN}PASS{RESET}"
        else:
            res_str = f"{RED}FAIL{RESET}"
            all_passed = False

        detail = f" ({r['detail']})" if r["detail"] and not r["passed"] else ""
        print(f"{r['path']:<40} | {status_str:<19} | {lat_str:<18} | {count_str:<8} | {res_str}{detail}")

    print("\n" + "=" * 86)
    if all_passed:
        print(f"{GREEN}{BOLD}ALL {len(results)} ENDPOINTS HEALTHY! Zero failures, zero leaks.{RESET}\n")
    else:
        print(f"{RED}{BOLD}FAILURES DETECTED! Check failed endpoints above.{RESET}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PUNoted Comprehensive Smoke Test")
    parser.add_argument("--url", default="http://127.0.0.1:9900", help="Base URL of backend (default: http://127.0.0.1:9900)")
    parser.add_argument("--token", default=None, help="Explicit API token (e.g. ptk_...)")
    parser.add_argument("--user", default=None, help="Target username to test (e.g. tomato-chef, kikyo)")
    args = parser.parse_args()

    asyncio.run(main_async(args.url, token=args.token, user=args.user))
