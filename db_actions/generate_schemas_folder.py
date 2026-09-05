import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db import Database

SCHEMAS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'schemas'))

async def split_schemas_into_files():
    db = Database()
    await db.create_pool()

    os.makedirs(SCHEMAS_DIR, exist_ok=True)
    print(f"Exporting individual table schemas to: {SCHEMAS_DIR}...")

    tables = await db.fetch_rows(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name;"
    )

    table_names = [t["table_name"] for t in tables]
    print(f"Found {len(table_names)} tables in database.")

    for table in table_names:
        sql_lines = []
        sql_lines.append(f"-- ============================================================================")
        sql_lines.append(f"-- PostgreSQL Table & Index Schema: {table}")
        sql_lines.append(f"-- ============================================================================\n")
        sql_lines.append(f"CREATE TABLE IF NOT EXISTS {table} (")

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

        sql_lines.append(",\n".join(col_defs))
        sql_lines.append(");\n")

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
            sql_lines.append("-- Performance & Optimization Indexes")
            for idx in indexes:
                idxdef = idx["indexdef"]
                if "CREATE INDEX " in idxdef and "IF NOT EXISTS" not in idxdef:
                    idxdef = idxdef.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")
                elif "CREATE UNIQUE INDEX " in idxdef and "IF NOT EXISTS" not in idxdef:
                    idxdef = idxdef.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ")
                sql_lines.append(f"{idxdef};")
            sql_lines.append("")

        file_path = os.path.join(SCHEMAS_DIR, f"{table}.sql")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(sql_lines))

    print(f"Successfully created {len(table_names)} schema SQL files in {SCHEMAS_DIR}!")
    await db.close_pool()

if __name__ == "__main__":
    asyncio.run(split_schemas_into_files())
