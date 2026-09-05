import datetime
import json
import logging
from typing import Any, Dict, List, Optional

from managers.global_ws_manager import global_ws_manager as manager
from services.contract_notification_evaluator import ContractNotificationEvaluator

logger = logging.getLogger(__name__)

async def get_user_rules(conn, account_id: str) -> Dict[str, Any]:
    """
    Fetches notification rules for a user or returns default configuration.
    """
    row = await conn.fetchrow("SELECT * FROM user_notification_rules WHERE accountid = $1;", account_id)
    if row:
        return dict(row)
    return {
        "fleet_enabled": True,
        "health_threshold": 70,
        "storage_enabled": True,
        "storage_threshold": 90,
        "production_enabled": True,
        "supply_days_threshold": 1.0,
        "contracts_enabled": True,
        "cx_enabled": True,
        "cx_market_watchers": []
    }


async def create_user_notification(
    conn,
    account_id: str,
    category: str,
    notif_type: str,
    title: str,
    message: str,
    dedup_key: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Inserts a user notification bound to accountid with deduplication check and broadcasts via WebSocket instantly.
    """
    try:
        data_json = json.dumps(data, default=str) if data else None
        inserted_row = None

        if dedup_key:
            row = await conn.fetchrow(
                """
                INSERT INTO user_notifications (accountid, category, type, title, message, dedup_key, data, is_read, is_deleted, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, FALSE, FALSE, CURRENT_TIMESTAMP)
                ON CONFLICT (dedup_key) DO NOTHING
                RETURNING id::text, category, type, title, message, data, is_read, created_at;
                """,
                account_id, category, notif_type, title, message, dedup_key, data_json
            )
            inserted_row = dict(row) if row else None
        else:
            row = await conn.fetchrow(
                """
                INSERT INTO user_notifications (accountid, category, type, title, message, data, is_read, is_deleted, created_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, FALSE, FALSE, CURRENT_TIMESTAMP)
                RETURNING id::text, category, type, title, message, data, is_read, created_at;
                """,
                account_id, category, notif_type, title, message, data_json
            )
            inserted_row = dict(row) if row else None

        if inserted_row:
            # INSTANT WEBSOCKET BROADCAST TO CONNECTED USER (Both account_id and userdata_id if mapped)
            ws_message = {
                "type": "USER_NOTIFICATION",
                "data": {
                    "id": inserted_row["id"],
                    "category": inserted_row["category"],
                    "type": inserted_row["type"],
                    "title": inserted_row["title"],
                    "message": inserted_row["message"],
                    "data": inserted_row["data"],
                    "is_read": False,
                    "created_at": inserted_row["created_at"].isoformat() if inserted_row["created_at"] else None
                }
            }
            logger.info(f"WEBSOCKET NOTIFICATION PUSH for user {account_id}: title='{inserted_row['title']}'")
            try:
                await manager.send_personal_message(account_id, ws_message)
            except Exception as e:
                logger.error(f"Failed sending WS notification to {account_id}: {e}")
            
            # If account_id maps to a distinct userdataid, send to that connection as well
            try:
                row_ud = await conn.fetchrow("SELECT userdataid FROM users WHERE accountid::text = $1 OR userdataid = $1;", account_id)
                if row_ud and row_ud["userdataid"] and row_ud["userdataid"] != account_id:
                    await manager.send_personal_message(row_ud["userdataid"], ws_message)
            except Exception as e:
                logger.error(f"Failed sending WS notification to userdataid: {e}")

            return True
        return False
    except Exception as e:
        logger.error(f"Error inserting user notification for {account_id}: {e}", exc_info=True)
        return False


async def evaluate_user_telemetry_notifications(pool, user_accountid: str, userdata_id: Optional[str] = None):
    """
    Evaluates telemetry thresholds for a specific user upon data sync or periodic loop.
    """
    if not userdata_id:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT userdataid FROM users WHERE accountid::text = $1;", user_accountid)
            if row and row["userdataid"]:
                userdata_id = row["userdataid"]
            else:
                userdata_id = user_accountid

    today_str = datetime.date.today().isoformat()

    async with pool.acquire() as conn:
        # TEST NOTIFICATION (Commented out after testing)
        # minute_str = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
        # test_dedup = f"test_ws_heartbeat_{user_accountid}_{minute_str}"
        # await create_user_notification(
        #     conn, user_accountid, "system", "ws_test",
        #     "Test WebSocket Heartbeat",
        #     f"Real-time WebSocket connection is working cleanly.",
        #     dedup_key=test_dedup,
        #     data={"test_time": minute_str}
        # )

        rules = await get_user_rules(conn, user_accountid)

        # 1. Fleet & Ship Condition Checks
        if rules.get("fleet_enabled", True):
            health_thresh = (rules.get("health_threshold", 70) or 70) / 100.0
            low_health_ships = await conn.fetch(
                "SELECT shipid, name, registration, condition FROM ships WHERE (userid = $1 OR userid = $2) AND condition < $3;",
                user_accountid, userdata_id, health_thresh
            )
            for s in low_health_ships:
                cond_pct = round((s["condition"] or 0) * 100, 1)
                dedup = f"health_low_{s['shipid']}_{today_str}"
                await create_user_notification(
                    conn, user_accountid, "fleet", "health_low",
                    f"Low Ship Health: {s['name'] or 'Ship'}",
                    f"Ship {s['name'] or ''} ({s['registration'] or 'N/A'}) health is at {cond_pct}%. Maintenance recommended.",
                    dedup_key=dedup,
                    data={"shipid": s["shipid"], "condition": s["condition"]}
                )

        # 2. Storage Capacity Checks (Exclude Ship Storages)
        if rules.get("storage_enabled", True):
            storage_thresh = (rules.get("storage_threshold", 90) or 90) / 100.0
            full_storages = await conn.fetch(
                """
                SELECT DISTINCT st.storageid, st.name as storage_name, st.volumeload, st.volumecapacity,
                       COALESCE(p_site.name, p_wh.name, p_direct.name, station.name, st.name, 'Warehouse') as location_name,
                       COALESCE(p_site.naturalid, p_wh.naturalid, p_direct.naturalid, station.naturalid) as location_naturalid
                FROM storages st
                LEFT JOIN sites s ON st.addressableid = s.siteid
                LEFT JOIN planets p_site ON s.addressplanetid = p_site.planetid
                LEFT JOIN warehouses wh ON st.storageid = wh.storeid OR st.addressableid = wh.warehouseid
                LEFT JOIN planets p_wh ON wh.addressplanet = p_wh.planetid
                LEFT JOIN planets p_direct ON st.addressableid = p_direct.planetid
                LEFT JOIN stations station ON st.addressableid = station.stationid OR st.addressableid = station.warehouseid
                WHERE (st.userid = $1 OR st.userid = $2) 
                  AND st.volumecapacity > 0 
                  AND (st.volumeload / st.volumecapacity::double precision) >= $3
                  AND st.type NOT IN ('SHIP_STORE', 'STL_FUEL_STORE', 'FTL_FUEL_STORE', 'VORTEX_FUEL_STORE')
                  AND st.addressableid NOT IN (SELECT shipid FROM ships WHERE userid = $1 OR userid = $2);
                """,
                user_accountid, userdata_id, storage_thresh
            )
            for st in full_storages:
                pct = round((st["volumeload"] / st["volumecapacity"]) * 100, 1)
                loc = f"{st['location_name']} ({st['location_naturalid']})" if st.get('location_naturalid') and st['location_naturalid'] != st['location_name'] else st['location_name']
                dedup = f"storage_90_{st['storageid']}_{today_str}"
                await create_user_notification(
                    conn, user_accountid, "storage", "storage_90_full",
                    f"Storage Nearly Full: {loc}",
                    f"Storage at {loc} is at {pct}% capacity ({int(st['volumeload']):,} / {int(st['volumecapacity']):,} m³).",
                    dedup_key=dedup,
                    data={"storageid": st["storageid"], "pct": pct, "location": loc}
                )

        # 3. Comprehensive Contract & Loan Condition Evaluator
        if rules.get("contracts_enabled", True):
            try:
                evaluator = ContractNotificationEvaluator(conn, create_user_notification)
                await evaluator.evaluate_user_contracts(user_accountid, userdata_id)
            except Exception as e:
                logger.error(f"ContractNotificationEvaluator failed for {user_accountid}: {e}", exc_info=True)

        # 4. Exchange Trade Orders & CX Market Watchers
        if rules.get("cx_enabled", True):
            sold_orders = await conn.fetch(
                """
                SELECT orderid, materialticker, quantity, reserved, location FROM user_vendor_orders 
                WHERE vendorid IN (SELECT vendorid FROM user_vendors WHERE userid = $1 OR userid = $2)
                  AND quantity > 0 AND (reserved::double precision / quantity::double precision) <= 0.10;
                """,
                user_accountid, userdata_id
            )
            for ord_row in sold_orders:
                dedup = f"order_90_sold_{ord_row['orderid']}_{today_str}"
                await create_user_notification(
                    conn, user_accountid, "cx", "cx_order_90_sold",
                    f"CX Order 90% Sold: {ord_row['materialticker']}",
                    f"Your order for {ord_row['materialticker']} is 90% fulfilled (only {ord_row['reserved']} units remaining).",
                    dedup_key=dedup,
                    data={"orderid": ord_row["orderid"], "ticker": ord_row["materialticker"]}
                )

            # Custom CX Market Watchers (e.g. RAT < 178 ICA)
            watchers = rules.get("cx_market_watchers")
            if watchers and isinstance(watchers, list):
                for w in watchers:
                    ticker = w.get("ticker")
                    exchange = w.get("exchange", "ICA")
                    target = float(w.get("target_price", 0))
                    direction = w.get("direction", "below")

                    if ticker and target > 0:
                        price_row = await conn.fetchrow(
                            "SELECT price FROM comex_trade_orders WHERE materialticker = $1 AND exchange = $2 ORDER BY created_at DESC LIMIT 1;",
                            ticker, exchange
                        )
                        if price_row and price_row["price"]:
                            curr_price = float(price_row["price"])
                            triggered = (direction == "below" and curr_price <= target) or (direction == "above" and curr_price >= target)
                            if triggered:
                                dedup = f"cx_watch_{ticker}_{exchange}_{today_str}"
                                await create_user_notification(
                                    conn, user_accountid, "cx", "cx_market_watch",
                                    f"CX Watch Triggered: {ticker} ({exchange})",
                                    f"{ticker} price is now {curr_price} {exchange} (target: {direction} {target} {exchange}).",
                                    dedup_key=dedup,
                                    data={"ticker": ticker, "exchange": exchange, "price": curr_price}
                                )

        # 5. Production Site Supply Checks
        if rules.get("production_enabled", True):
            WORKFORCE_TICKERS = {'RAT', 'DW', 'O', 'COF', 'PWO', 'VEG', 'MEAT', 'MED', 'PCO', 'RHO'}
            
            site_settings_rows = await conn.fetch("SELECT entity_id, settings FROM user_entity_settings WHERE accountid = $1 AND domain = 'site';", user_accountid)
            site_settings_map = {r["entity_id"]: (json.loads(r["settings"]) if isinstance(r["settings"], str) else r["settings"]) for r in site_settings_rows}

            sites = await conn.fetch(
                """
                SELECT s.siteid, s.addressplanetid, COALESCE(p.name, p.naturalid, s.siteid) as planet_name, p.naturalid 
                FROM sites s 
                LEFT JOIN planets p ON s.addressplanetid = p.planetid 
                WHERE s.userid = $1 OR s.userid = $2;
                """,
                user_accountid, userdata_id
            )

            for site in sites:
                site_cfg = site_settings_map.get(site["siteid"], {})
                notif_cfg = site_cfg.get("notification_settings", {})
                if site_cfg and not notif_cfg.get("enabled", True):
                    continue

                planet_disp = f"{site['planet_name']} ({site['naturalid']})" if site.get('naturalid') and site['naturalid'] != site['planet_name'] else site['planet_name']

                # 5a. Workforce Check
                if notif_cfg.get("workforce_alert", True):
                    wf = await conn.fetchrow("SELECT pioneer, settler, technician FROM site_available_reserve_populations WHERE siteid = $1;", site["siteid"])
                    if wf and any(val and val < 5 for val in [wf["pioneer"], wf["settler"], wf["technician"]]):
                        dedup = f"site_workforce_low_{site['siteid']}_{today_str}"
                        await create_user_notification(
                            conn, user_accountid, "production", "site_supply_low",
                            f"Workforce Supply Low: {site['planet_name']}",
                            f"Site workforce supply critical on planet {planet_disp}. Less than 1 day reserve remaining.",
                            dedup_key=dedup,
                            data={"siteid": site["siteid"], "planet_name": site["planet_name"]}
                        )

                # 5b. Per-Material (CONS) Reserve Days Evaluation
                mat_targets = site_cfg.get("material_target_days", {})
                default_site_days = float(site_cfg.get("global_target_days", rules.get("supply_days_threshold", 1.0) or 1.0))

                stored_items = await conn.fetch(
                    """
                    SELECT materialid, quantity FROM storage_items 
                    WHERE storageid IN (SELECT storageid FROM storages WHERE addressableid = $1 OR userid = $2);
                    """,
                    site["siteid"], user_accountid
                )
                stored_map = {r["materialid"]: r["quantity"] for r in stored_items}

                if mat_targets and isinstance(mat_targets, dict):
                    for mat_ticker, target_days in mat_targets.items():
                        mat_amount = stored_map.get(mat_ticker, 0)
                        target_d = float(target_days)
                        
                        is_workforce = mat_ticker in WORKFORCE_TICKERS
                        # Determine category label
                        category_label = "Workforce & Material Supply" if is_workforce else "Input Material Supply"

                        # Threshold checks: 1 day before target (warning) or below target (critical)
                        # We proxy daily consumption estimate; if mat_amount < target_d * 10 or 0
                        if mat_amount < (target_d * 10):
                            dedup_crit = f"site_mat_crit_{site['siteid']}_{mat_ticker}_{today_str}"
                            await create_user_notification(
                                conn, user_accountid, "production", "site_supply_low",
                                f"Critical {category_label}: {mat_ticker}",
                                f"{planet_disp}: {mat_ticker} supply reserve is below your custom target of {target_d} day(s) ({int(mat_amount):,} units remaining).",
                                dedup_key=dedup_crit,
                                data={"siteid": site["siteid"], "ticker": mat_ticker, "target_days": target_d, "amount": mat_amount}
                            )

        # 6. Daily Financial Summary (Midnight Summary - Once per day)
        finance_accs = await conn.fetch("SELECT balanceamount, balancecurrencycode FROM user_currency_accounts WHERE userid = $1 OR userid = $2;", user_accountid, userdata_id)
        if finance_accs:
            balances_str = ", ".join([f"{int(a['balanceamount']):,} {a['balancecurrencycode']}" for a in finance_accs[:3]])
            dedup = f"financial_daily_{user_accountid}_{today_str}"
            await create_user_notification(
                conn, user_accountid, "financial", "financial_daily",
                "Daily Financial Balance Summary",
                f"Your active liquid balance summary for today: {balances_str}.",
                dedup_key=dedup,
                data={"balances": [dict(a) for a in finance_accs]}
            )


async def evaluate_all_users_notifications(pool):
    """
    Periodic background task running every 6 hours (or once daily) across all active users.
    Ensures contract conditions transitioning into due-date threshold windows (3d, 2d, 1d, today, overdue)
    get evaluated automatically with deduplication checks.
    """
    try:
        async with pool.acquire() as conn:
            users = await conn.fetch("SELECT DISTINCT accountid FROM users WHERE accountid IS NOT NULL AND accountid != '';")
            for u in users:
                acc_id = str(u["accountid"])
                try:
                    await evaluate_user_telemetry_notifications(pool, acc_id)
                except Exception as e:
                    logger.error(f"Periodic notification evaluation failed for user {acc_id}: {e}")
    except Exception as e:
        logger.error(f"Error in evaluate_all_users_notifications: {e}", exc_info=True)
