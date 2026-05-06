import os
import re
from typing import Any, List, Tuple

import asyncpg
import requests
from bs4 import BeautifulSoup


# --- Parsing Logic ---
def parse_ship_type(text):
    """Parses the text from the cell and returns the ship type and price."""
    text = text.strip().lower()
    if "starter ship" in text and "-" in text:
        parts = text.split("-", 1)
        # We need to extract 'upgrade' and prepend it with 'starter'
        ship_name_part = parts[0].strip().lower().replace("starter ship wcb ", "")
        final_ship_type = "starter" + ship_name_part

        price_text = parts[1].strip()
        cleaned_price = re.sub(r"[^0-9]", "", price_text)
        price_int = int(cleaned_price) if cleaned_price else 0
        return final_ship_type, price_int

    match = re.search(r"^(.*?)\s+\((.*?)\)\s+-\s+(.*)", text)
    if match:
        ship_name = match.group(1).strip()
        modifier_text = match.group(2).strip()
        price_text = match.group(3).strip()
        composed_ship = ship_name
        if "ftl" in modifier_text:
            composed_ship += "ftl"
        elif "stl" in modifier_text:
            composed_ship += "stl"
        cleaned_price = re.sub(r"[^0-9]", "", price_text)
        price_int = int(cleaned_price) if cleaned_price else 0
        return composed_ship.replace(" ", ""), price_int
    parts = text.split("-", 1)
    if len(parts) == 2:
        ship_name = parts[0].strip()
        price_text = parts[1].strip()
        cleaned_price = re.sub(r"[^0-9]", "", price_text)
        price_int = int(cleaned_price) if cleaned_price else 0
        return ship_name.replace("shipwcb", ""), price_int
    return text.replace(" ", ""), 0


# --- Asynchronous Scraping and Saving Task ---
async def scrape_and_save_data(pool: asyncpg.pool.Pool):
    """
    Scrapes data from the sheet, TRUNCATES the existing table,
    and inserts the new, scraped data in a single atomic transaction.
    """
    print("Starting scheduled scrape and overwrite job...")

    scraped_data = []

    # --- 1. SCRAPE DATA (Outside of the DB transaction) ---
    try:
        # Fetch the data from the sheet
        response = requests.get(os.environ.get("SHIPS_PRODUCTION_GOOGLE_SHEET"))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("tbody")

        if not table:
            print("Table not found in HTML.")
            return

        rows = table.find_all("tr")

        for i, row in enumerate(rows):
            # The row index (i + 1) will be the unique order ID for THIS scrape
            row_id = i + 1

            cells = row.find_all("td")
            # Skip invalid or empty rows
            if len(cells) < 4 or not any(cell.get_text(strip=True) for cell in cells[:4]):
                continue

            ship_and_price_text = cells[1].get_text(strip=True)
            ship_type, price = parse_ship_type(ship_and_price_text)

            if ship_type == "model":
                continue

            username = cells[0].get_text(strip=True)
            position = cells[2].get_text(strip=True)
            eta = cells[3].get_text(strip=True)

            scraped_data.append(
                {
                    "orderid": row_id,
                    "username": username,
                    "shiptype": ship_type,
                    "price": price,
                    "position": int(position),
                    "orderwaittime": int(eta),
                }
            )

    except requests.exceptions.RequestException as e:
        print(f"Error scraping data: {e}")
        return
    except Exception as e:
        print(f"Error processing sheet data: {e}")
        return

    # --- 2. DATABASE TRANSACTION (ATOMIC TRUNCATE AND INSERT) ---
    try:
        async with pool.acquire() as con:
            async with con.transaction():
                # A. TRUNCATE: Delete ALL existing data in the ship_production table
                # This ensures the database table is now empty.
                await con.execute("TRUNCATE TABLE ship_production;")
                print("Old data truncated.")

                # B. INSERT: Add all the newly scraped data
                insert_count = 0
                for data in scraped_data:
                    await con.execute(
                        """
                        INSERT INTO ship_production (orderid, username, shiptype, price, position, orderwaittime)
                        VALUES ($1, $2, $3, $4, $5, $6);
                        """,
                        data["orderid"],
                        data["username"],
                        data["shiptype"],
                        data["price"],
                        data["position"],
                        data["orderwaittime"],
                    )
                    insert_count += 1

                print(f"Successfully inserted {insert_count} new records.")
                print("Data scraped and saved successfully.")

    except Exception as e:
        print(f"Error in scheduled scrape job: {e}. Changes were rolled back.")


# --- Parsing Logic ---
def parse_financial_data_row(cells: List[Any], headers: List[str]) -> Tuple[str, float | None]:
    """
    Parses a single row from the HTML table to extract the 'MAT' and 'Price'.
    Handles non-numeric values and formula errors gracefully.
    """
    try:
        mat_index = headers.index("MAT")
        price_index = headers.index("Price")

        mat_ticker = cells[mat_index].get_text(strip=True)
        price_text = cells[price_index].get_text(strip=True)

        # Remove both ',' and '$' from the price string
        cleaned_price = re.sub(r"[$,]", "", price_text)

        try:
            price = float(cleaned_price)
        except ValueError:
            price = None  # Set price to None if conversion fails (e.g., '#DIV/0!', empty string)

        return mat_ticker, price

    except (ValueError, IndexError):
        # Catch errors if 'MAT' or 'Price' columns are missing or if the cell content is unexpected.
        return "", None


async def scrape_prices_and_save_data(pool: asyncpg.pool.Pool):
    """
    Scrapes data from the Google Sheet and saves it to the database
    using bulk operations.
    """
    print("Starting scheduled ticker and price scrape job...")

    try:
        response = requests.get(
            "https://docs.google.com/spreadsheets/u/0/d/e/2PACX-1vSG_rqZ_TCSTe12_FzJRw0_mbDCYJ5HnGvnmgI3Sd-CFd0AxkSzc88e3glLeM-5ZpI2ILcFSzEX8Nvg/pubhtml/sheet?plix3d1x26headersx3dfalse&gid=0"
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        main_table = soup.find("table", class_="waffle")
        if not main_table:
            print("The financial data table could not be found.")
            return

        rows = main_table.find_all("tr")
        if len(rows) < 6:
            print("Table has no data rows.")
            return

        header_row_index = None
        headers = []
        for i, row in enumerate(rows):
            cells = row.find_all("td")
            current_headers = [c.get_text(strip=True) for c in cells]
            if "MAT" in current_headers and "Price" in current_headers:
                header_row_index = i
                headers = current_headers
                break

        if header_row_index is None:
            print("Could not find the header row with 'MAT' and 'Price'.")
            return

        records_to_upsert = []

        for row in rows[header_row_index + 1 :]:
            cells = row.find_all("td")
            if not cells:
                continue

            mat_ticker, price = parse_financial_data_row(cells, headers)

            # Skip rows where the ticker is empty or price is not a valid number
            if not mat_ticker or price is None:
                continue

            records_to_upsert.append((mat_ticker, price))

        if not records_to_upsert:
            print("No valid data records found to save.")
            return

        async with pool.acquire() as con:
            async with con.transaction():
                try:
                    await con.executemany(
                        """
                        INSERT INTO material_prices (ticker, price)
                        VALUES ($1, $2)
                        ON CONFLICT (ticker) DO UPDATE
                        SET price = EXCLUDED.price;
                        """,
                        records_to_upsert,
                    )
                    print(f"Data scraped and {len(records_to_upsert)} records saved successfully.")
                except Exception as e:
                    print(f"Database error during UPSERT: {e}")
                    raise

    except requests.exceptions.RequestException as e:
        print(f"HTTP error during scraping: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
