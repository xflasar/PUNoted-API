import asyncio
import os
import sys

from db import Database

SQL_COMMANDS = """
CREATE TABLE IF NOT EXISTS corp_ship_orders (
    id SERIAL PRIMARY KEY,
    corporation_id VARCHAR(100),
    customer_username VARCHAR(255),
    customer_company_code VARCHAR(100),
    owner_type VARCHAR(50),
    owner_id VARCHAR(255),
    guest_pin VARCHAR(255),
    ship_config JSONB,
    price NUMERIC,
    wait_time_days INT,
    status VARCHAR(50) DEFAULT 'QUEUED',
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ship_build_presets (
    id SERIAL PRIMARY KEY,
    corporation_id VARCHAR(100),
    name VARCHAR(255) NOT NULL,
    price NUMERIC NOT NULL,
    price_corp NUMERIC NOT NULL,
    parts JSONB NOT NULL,
    is_admin_preset BOOLEAN DEFAULT FALSE,
    created_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

async def main():
    db = Database()
    await db.create_pool()
    
    try:
        print("Executing SQL to create ship build management tables...")
        await db.execute(SQL_COMMANDS)
        print("Tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        await db.close_pool()

if __name__ == "__main__":
    asyncio.run(main())
