import asyncio
import os
import re
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db import Database

REPOS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def parse_sql_files(root_dir: str):
    """
    Dynamically scans all .sql files in the root project directory for:
      - CREATE TABLE statements (discovers table names and declared column names)
      - CREATE INDEX statements (discovers index names)
    """
    expected_tables = {}  # { table_name: set(column_names) }
    expected_indexes = {} # { table_name: set(index_names) }

    sql_files = [
        os.path.join(root_dir, f)
        for f in os.listdir(root_dir)
        if f.endswith(".sql")
    ]

    table_create_regex = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL
    )

    index_create_regex = re.compile(
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\s+ON\s+([a-zA-Z0-9_]+)",
        re.IGNORECASE
    )

    for filepath in sql_files:
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            for match in table_create_regex.finditer(content):
                table_name = match.group(1).lower()
                body = match.group(2)

                if table_name not in expected_tables:
                    expected_tables[table_name] = set()

                lines = body.split("\n")
                for line in lines:
                    line_clean = line.strip().rstrip(",")
                    if not line_clean or line_clean.upper().startswith(("PRIMARY KEY", "CONSTRAINT", "FOREIGN KEY", "UNIQUE", "CHECK")):
                        continue
                    tokens = line_clean.split()
                    if tokens:
                        col_name = tokens[0].strip('"').lower()
                        if re.match(r"^[a-zA-Z0-9_]+$", col_name):
                            expected_tables[table_name].add(col_name)

            for match in index_create_regex.finditer(content):
                idx_name = match.group(1).lower()
                tbl_name = match.group(2).lower()
                if tbl_name not in expected_indexes:
                    expected_indexes[tbl_name] = set()
                expected_indexes[tbl_name].add(idx_name)

        except Exception as e:
            pass

    return expected_tables, expected_indexes


async def inspect_db_diff():
    db = Database()
    await db.create_pool()

    print("=" * 70)
    print("        PUNoted Automatic Database Schema & Migration Diff")
    print("=" * 70)

    # 1. Dynamically parse all SQL definitions in root .sql files
    declared_tables, declared_indexes = parse_sql_files(REPOS_ROOT)

    # 2. Fetch active DB state
    existing_tables_rows = await db.fetch_rows(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    )
    existing_table_names = {r["table_name"].lower() for r in existing_tables_rows}

    missing_tables = []
    incomplete_tables = {}

    for table_name, expected_cols in declared_tables.items():
        if table_name not in existing_table_names:
            missing_tables.append(table_name)
        else:
            cols_rows = await db.fetch_rows(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1;",
                table_name
            )
            existing_cols = {r["column_name"].lower() for r in cols_rows}

            idx_rows = await db.fetch_rows(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename=$1;",
                table_name
            )
            existing_idxs = {r["indexname"].lower() for r in idx_rows}

            expected_idxs = declared_indexes.get(table_name, set())

            missing_cols = [c for c in sorted(expected_cols) if c not in existing_cols]
            missing_idxs = [i for i in sorted(expected_idxs) if i not in existing_idxs]

            if missing_cols or missing_idxs:
                incomplete_tables[table_name] = {
                    "missing_columns": missing_cols,
                    "missing_indexes": missing_idxs
                }

    print(f"\nScanned SQL Schema Files: {len(declared_tables)} tables automatically parsed from .sql files")

    if not missing_tables and not incomplete_tables:
        print("\nSUCCESS: Production database schema is 100% in sync with .sql files!")
    else:
        if missing_tables:
            print("\nMISSING TABLES (Need creation script in production):")
            for t in missing_tables:
                print(f" - [MISSING TABLE] {t}")

        if incomplete_tables:
            print("\nINCOMPLETE TABLES (Missing columns or indexes in production):")
            for t, details in incomplete_tables.items():
                print(f" - [UPDATE NEEDED] {t}:")
                if details["missing_columns"]:
                    print(f"     Missing Columns: {', '.join(details['missing_columns'])}")
                if details["missing_indexes"]:
                    print(f"     Missing Indexes: {', '.join(details['missing_indexes'])}")

    print("\n" + "=" * 70)
    await db.close_pool()
    return missing_tables, incomplete_tables


if __name__ == "__main__":
    asyncio.run(inspect_db_diff())
