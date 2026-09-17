import argparse
import asyncio
import glob
import json
import logging
import os
import random
import sys
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_db")

# DSN resolution
DATABASE_URL = os.environ.get("XATA_DATABASE_URL") or os.environ.get("DATABASE_URL")

DEFAULT_TEST_USER_ID = "usr_dev_test_001"
DEFAULT_TEST_USER_EMAIL = "dev@punoted.local"
DEFAULT_TEST_USER_NAME = "Dev Tester"
DEFAULT_TEST_API_TOKEN = "pun_dev_token_1234567890abcdef"
DEFAULT_TEST_COMPANY_CODE = "DEV"
DEFAULT_TEST_COMPANY_NAME = "Developer Enterprises"

# Core static seed data
MATERIALS_SEED = [
    ("RAT", "Rations", "Consumable", 10.0),
    ("DW", "Drinking Water", "Consumable", 5.0),
    ("O", "Oxygen", "Gas", 8.0),
    ("H2O", "Water", "Liquid", 4.0),
    ("FE", "Iron Ore", "Raw Material", 15.0),
    ("CU", "Copper Ore", "Raw Material", 20.0),
    ("STEEL", "Steel Plates", "Refined Metal", 45.0),
    ("AL", "Aluminum", "Refined Metal", 35.0),
    ("BCH", "Basic Chem", "Chemicals", 50.0),
]

EXCHANGES_SEED = [
    ("CI1", "Castillo Station CX", "Castillo", "NCC"),
    ("NC1", "New Horizon CX", "New Horizon", "AIC"),
    ("IC1", "Inner Circle CX", "Benten", "ICA"),
]

SYSTEMS_SEED = [
    ("SYS-001", "Khor", "Khor Sector", 12.5, -45.2, 3.0),
    ("SYS-002", "Benten", "Benten Sector", 0.0, 0.0, 0.0),
    ("SYS-003", "Castillo", "Castillo Sector", -32.1, 14.8, -1.2),
]


async def get_pg_connection(dsn: str):
    import asyncpg
    try:
        return await asyncpg.connect(dsn=dsn)
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)


async def run_init(conn):
    logger.info("Initializing database schemas...")
    schema_dir = os.path.join(os.path.dirname(__file__), "schemas")
    sql_files = sorted(glob.glob(os.path.join(schema_dir, "*.sql")))

    if not sql_files:
        logger.warning(f"No SQL schema files found in {schema_dir}")
        return

    logger.info(f"Found {len(sql_files)} SQL files to execute.")
    executed = 0
    for filepath in sql_files:
        filename = os.path.basename(filepath)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                sql = f.read().strip()
                if sql:
                    await conn.execute(sql)
                    executed += 1
        except Exception as e:
            logger.error(f"Error executing {filename}: {e}")

    logger.info(f"Successfully executed {executed}/{len(sql_files)} schema files.")


async def run_reset(conn):
    logger.info("Resetting/truncating database tables...")
    # Fetch all table names in public schema
    rows = await conn.fetch("""
        SELECT tablename FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename NOT LIKE 'pg_%' 
        AND tablename NOT LIKE '_xata_%'
    """)
    tables = [r["tablename"] for r in rows]
    if not tables:
        logger.info("No tables found to truncate.")
        return

    table_list = ", ".join(f'"{t}"' for t in tables)
    await conn.execute(f"TRUNCATE TABLE {table_list} CASCADE;")
    logger.info(f"Successfully truncated {len(tables)} tables.")


async def run_seed_base(conn):
    logger.info("Seeding base reference data and default test user...")
    now = datetime.now(timezone.utc)

    # 1. Seed Users, Users_Data, Public_Users_Data & Extension Tokens
    await conn.execute("""
        INSERT INTO users (id, email, username, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4)
        ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, username = EXCLUDED.username;
    """, DEFAULT_TEST_USER_ID, DEFAULT_TEST_USER_EMAIL, DEFAULT_TEST_USER_NAME, now)

    await conn.execute("""
        INSERT INTO users_data (userid, displayname, companyid, created, xata_createdat, xata_updatedat)
        VALUES ($1, $2, $3, $4, $4, $4)
        ON CONFLICT (userid) DO UPDATE SET displayname = EXCLUDED.displayname, companyid = EXCLUDED.companyid;
    """, DEFAULT_TEST_USER_ID, DEFAULT_TEST_USER_NAME, f"cmp_{DEFAULT_TEST_COMPANY_CODE}", now)

    await conn.execute("""
        INSERT INTO public_users_data (userid, displayname, companyid, created)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING;
    """, DEFAULT_TEST_USER_ID, DEFAULT_TEST_USER_NAME, f"cmp_{DEFAULT_TEST_COMPANY_CODE}", now)

    await conn.execute("""
        INSERT INTO company_data (id, name, code, user_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO NOTHING;
    """, f"cmp_{DEFAULT_TEST_COMPANY_CODE}", DEFAULT_TEST_COMPANY_NAME, DEFAULT_TEST_COMPANY_CODE, DEFAULT_TEST_USER_ID)

    await conn.execute("""
        INSERT INTO user_api_tokens (user_id, token_hash, name, created_at)
        VALUES ($1, $2, 'Dev Test Key', $3)
        ON CONFLICT (token_hash) DO NOTHING;
    """, DEFAULT_TEST_USER_ID, DEFAULT_TEST_API_TOKEN, now)

    await conn.execute("""
        INSERT INTO user_contexts (user_id, context_id, company_code, company_name, updated_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (user_id, context_id) DO NOTHING;
    """, DEFAULT_TEST_USER_ID, f"ctx_{DEFAULT_TEST_COMPANY_CODE}", DEFAULT_TEST_COMPANY_CODE, DEFAULT_TEST_COMPANY_NAME, now)

    # 2. Seed Materials
    for code, name, category, default_price in MATERIALS_SEED:
        await conn.execute("""
            INSERT INTO materials (ticker, name, category, default_price)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING;
        """, code, name, category, default_price)

    # 3. Seed Commodity Exchanges
    for code, name, location, currency in EXCHANGES_SEED:
        await conn.execute("""
            INSERT INTO commodity_exchanges (code, name, location_name, currency_code)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING;
        """, code, name, location, currency)

    # 4. Seed Systems
    for sys_id, sys_name, sector, x, y, z in SYSTEMS_SEED:
        await conn.execute("""
            INSERT INTO systems (system_id, name, sector_name, pos_x, pos_y, pos_z)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING;
        """, sys_id, sys_name, sector, x, y, z)

    logger.info("Base reference data & test user successfully seeded!")


async def run_seed_random(conn):
    logger.info("Generating random mock contracts, trade orders & telemetry data...")
    now = datetime.now(timezone.utc)

    # Seed Mock Contracts
    contract_types = ["PURCHASE_AGREEMENT", "LOAN", "SHIPPING", "PRODUCTION"]
    parties = ["CUSTOMER", "VENDOR", "PARTNER"]

    for i in range(1, 11):
        contract_id = f"cnt_test_{i:03d}"
        c_type = random.choice(contract_types)
        party = random.choice(parties)
        total_amount = float(random.randint(5000, 150000))
        status = random.choice(["ACTIVE", "FULFILLED", "PENDING"])

        await conn.execute("""
            INSERT INTO contracts (id, user_id, party, type, total_amount, status, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING;
        """, contract_id, DEFAULT_TEST_USER_ID, party, c_type, total_amount, status, now)

        # Add contract conditions
        cond_id = f"cond_test_{i:03d}"
        amount = total_amount / 2
        await conn.execute("""
            INSERT INTO contract_conditions (id, contract_id, party, type, status, amountmoney)
            VALUES ($1, $2, $3, 'PAYMENT', $4, $5)
            ON CONFLICT (id) DO NOTHING;
        """, cond_id, contract_id, party, status, amount)

    # Seed Comex Orders
    exchanges = ["CI1", "NC1", "IC1"]
    for i in range(1, 15):
        order_id = f"ord_test_{i:03d}"
        ticker = random.choice([m[0] for m in MATERIALS_SEED])
        exchange = random.choice(exchanges)
        amount = random.randint(10, 500)
        price = round(random.uniform(5.0, 100.0), 2)
        order_type = random.choice(["BUY", "SELL"])

        await conn.execute("""
            INSERT INTO comex_trade_orders (id, exchange_code, ticker, order_type, count, item_cost)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO NOTHING;
        """, order_id, exchange, ticker, order_type, amount, price)

    logger.info("Random mock telemetry data seeded successfully!")


async def main():
    parser = argparse.ArgumentParser(description="PUNoted Database Seeding & Setup CLI")
    parser.add_argument("command", choices=["init", "reset", "seed", "seed-random", "full-setup"],
                        help="Command to run: init (create tables), reset (truncate tables), seed (base data), seed-random (mock telemetry), full-setup (all)")
    parser.add_argument("--dsn", default=DATABASE_URL, help="PostgreSQL DSN string (defaults to XATA_DATABASE_URL env variable)")

    args = parser.parse_args()

    if not args.dsn:
        logger.error("No PostgreSQL DSN provided. Set XATA_DATABASE_URL environment variable or pass --dsn parameter.")
        sys.exit(1)

    conn = await get_pg_connection(args.dsn)

    try:
        if args.command == "init":
            await run_init(conn)
        elif args.command == "reset":
            await run_reset(conn)
        elif args.command == "seed":
            await run_seed_base(conn)
        elif args.command == "seed-random":
            await run_seed_base(conn)
            await run_seed_random(conn)
        elif args.command == "full-setup":
            await run_init(conn)
            await run_reset(conn)
            await run_seed_base(conn)
            await run_seed_random(conn)
    finally:
        await conn.close()
        logger.info("Database CLI task complete!")


if __name__ == "__main__":
    asyncio.run(main())
