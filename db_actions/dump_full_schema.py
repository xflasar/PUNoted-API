import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db import Database

OUTPUT_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'full_schema_dump.sql'))

async def dump_schema():
    db = Database()
    await db.create_pool()

    print("Extracting full database schema (DDL & Indexes, NO DATA)...")

    sql_output = []
    sql_output.append("-- ============================================================================")
    sql_output.append("-- Complete PUNoted Database Full Schema & Indexes Dump")
    sql_output.append("-- Generated automatically for production database environment deployment")
    sql_output.append("-- Safe to run on existing database (All statements use IF NOT EXISTS)")
    sql_output.append("-- ============================================================================\n")
    sql_output.append("SET statement_timeout = 0;")
    sql_output.append("SET client_encoding = 'UTF8';\n")

    # 1. Fetch tables
    tables = await db.fetch_rows(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name;"
    )

    table_names = [t["table_name"] for t in tables]
    print(f"Found {len(table_names)} tables in public schema.")

    for table in table_names:
        sql_output.append(f"-- ----------------------------------------------------------------------------")
        sql_output.append(f"-- Table: {table}")
        sql_output.append(f"-- ----------------------------------------------------------------------------")
        sql_output.append(f"CREATE TABLE IF NOT EXISTS {table} (")

        # Fetch columns
        cols = await db.fetch_rows(
            """
            SELECT 
                column_name, 
                data_type, 
                character_maximum_length,
                is_nullable, 
                column_default,
                udt_name
            FROM information_schema.columns 
            WHERE table_schema='public' AND table_name=$1 
            ORDER BY ordinal_position;
            """,
            table
        )

        # Fetch primary keys
        pk_rows = await db.fetch_rows(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = $1
            ORDER BY kcu.ordinal_position;
            """,
            table
        )
        pk_cols = [r["column_name"] for r in pk_rows]

        col_defs = []
        for c in cols:
            name = c["column_name"]
            dtype = c["data_type"].upper()
            udt = c["udt_name"].lower()

            if dtype == "USER-DEFINED":
                dtype = udt.upper()
            elif dtype == "ARRAY":
                dtype = f"{udt.replace('_', '').upper()}[]"
            elif dtype == "CHARACTER VARYING":
                if c["character_maximum_length"]:
                    dtype = f"VARCHAR({c['character_maximum_length']})"
                else:
                    dtype = "VARCHAR"

            default_val = c["column_default"]
            nullable = "" if c["is_nullable"] == "YES" else " NOT NULL"

            col_str = f"    {name} {dtype}"
            if default_val:
                col_str += f" DEFAULT {default_val}"
            col_str += nullable
            col_defs.append(col_str)

        if pk_cols:
            pk_str = f"    PRIMARY KEY ({', '.join(pk_cols)})"
            col_defs.append(pk_str)

        sql_output.append(",\n".join(col_defs))
        sql_output.append(");\n")

        # Fetch indexes (excluding PK indexes)
        indexes = await db.fetch_rows(
            """
            SELECT indexname, indexdef 
            FROM pg_indexes 
            WHERE schemaname='public' AND tablename=$1 
              AND indexname NOT IN (
                  SELECT constraint_name 
                  FROM information_schema.table_constraints 
                  WHERE table_schema='public' AND constraint_type='PRIMARY KEY'
              );
            """,
            table
        )

        if indexes:
            for idx in indexes:
                idxdef = idx["indexdef"]
                # Convert to IF NOT EXISTS index statement
                if "CREATE INDEX " in idxdef and "IF NOT EXISTS" not in idxdef:
                    idxdef = idxdef.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")
                elif "CREATE UNIQUE INDEX " in idxdef and "IF NOT EXISTS" not in idxdef:
                    idxdef = idxdef.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ")
                sql_output.append(f"{idxdef};")
            sql_output.append("")

    # Write output to full_schema_dump.sql
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(sql_output))

    print(f"Full schema successfully dumped to: {OUTPUT_FILE}")
    await db.close_pool()

if __name__ == "__main__":
    asyncio.run(dump_schema())
