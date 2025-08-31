# main.py

import asyncio
import os
import re
import asyncpg
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi.middleware.cors import CORSMiddleware
import requests
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
import logging

from db import Database

from auth import auth_router
from data_handlers import data_router

from starlette.middleware.base import BaseHTTPMiddleware

# --- GLOBAL LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create the FastAPI app instance
app = FastAPI(title="PUNoted API", description="API for managing data", version="1.0.0")

# Configure CORS
origins = ["*"] # will get changed to host correct extension ids
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Debugging for request size
class RequestSizeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        print(f"Received a request to: {request.url}")
        content_length = request.headers.get("Content-Length")
        if content_length:
            print(f"Request body size: {content_length} bytes")
        response = await call_next(request)
        return response

app.add_middleware(RequestSizeMiddleware)

# Temporary fix for not yet signed extensions
class DynamicCORSMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin")
        
        # Check if the origin is from a browser extension (Chrome or Firefox)
        if origin and (origin.startswith("chrome-extension://") or origin.startswith("moz-extension://")):
            response = await call_next(request)
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
            return response
        else:
            response = await call_next(request)
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "*"
            return response

app.add_middleware(DynamicCORSMiddleware)

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")


db = Database()
scheduler = AsyncIOScheduler()

import debugpy
DEBUG_PORT = 5680
if not debugpy.is_client_connected():
    try:
        debugpy.listen(("0.0.0.0", DEBUG_PORT))
        print(f"Debugpy listening on port {DEBUG_PORT}. Waiting for client...")
    except Exception as e:
        print(f"Failed to start debugpy listener on port {DEBUG_PORT}: {e}")

# STARTUP EVENT: Initializes the connection pool once when the app starts
@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    try:
        await db.create_pool(loop=loop)

        # Cron job set to run every 30 minutes
        scheduler.add_job(scrape_and_save_data, 'interval', minutes=30, args=[db.pool])
        scheduler.start()
        print('Scheduler started.')
    except Exception as e:
        print(f"Failed to create database pool or start scheduler: {e}")

# SHUTDOWN EVENT: Closes the connection pool when the app shuts down
@app.on_event("shutdown")
async def shutdown_event():
    await db.close_pool()
    print('Closed DB.')
    pass

@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    request.state.db = db
    response = await call_next(request)
    return response

# Include the routers
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(data_router, tags=["data"])

@app.get('/status')
async def status_check(request: Request):
  try:
      # Get the client IP address from the request object
      client_host = request.client.host if request and request.client else 'N/A'

      # Check for the X-Forwarded-For header to get the real client IP
      x_forwarded_for = request.headers.get('x-forwarded-for')
    
      # If the header exists, take the first IP in the list
      if x_forwarded_for:
          real_client_ip = x_forwarded_for.split(',')[0].strip()
          logger.info(f"IP: {real_client_ip} (via proxy: {client_host}) - Received batch...")
      else:
          # If the header is not present, use the direct connection's IP
          real_client_ip = client_host
          logger.info(f"IP: {real_client_ip} - Received Status Check")

      return {"status": "online", "db_check": "success"}
  except Exception as e:
      logger.error(f"Database connection issue during status check: {e}")
      raise HTTPException(status_code=500, detail="Database connection error.")

# --- Parsing Logic ---
def parse_ship_type(text):
    """Parses the text from the cell and returns the ship type and price."""
    text = text.strip().lower()
    if "starter ship" in text and "-" in text:
        parts = text.split('-', 1)
        # We need to extract 'upgrade' and prepend it with 'starter'
        ship_name_part = parts[0].strip().lower().replace("starter ship wcb ", "")
        final_ship_type = "starter" + ship_name_part
        
        price_text = parts[1].strip()
        cleaned_price = re.sub(r'[^0-9]', '', price_text)
        price_int = int(cleaned_price) if cleaned_price else 0
        return final_ship_type, price_int
    
    match = re.search(r'^(.*?)\s+\((.*?)\)\s+-\s+(.*)', text)
    if match:
        ship_name = match.group(1).strip()
        modifier_text = match.group(2).strip()
        price_text = match.group(3).strip()
        composed_ship = ship_name
        if "ftl" in modifier_text:
            composed_ship += "ftl"
        elif "stl" in modifier_text:
            composed_ship += "stl"
        cleaned_price = re.sub(r'[^0-9]', '', price_text)
        price_int = int(cleaned_price) if cleaned_price else 0
        return composed_ship.replace(" ", ""), price_int
    parts = text.split('-', 1)
    if len(parts) == 2:
        ship_name = parts[0].strip()
        price_text = parts[1].strip()
        cleaned_price = re.sub(r'[^0-9]', '', price_text)
        price_int = int(cleaned_price) if cleaned_price else 0
        return ship_name.replace("shipwcb", ""), price_int
    return text.replace(" ", ""), 0

# --- Asynchronous Scraping and Saving Task ---
async def scrape_and_save_data(pool: asyncpg.pool.Pool):
    """Scrapes data from the sheet and saves it to the database."""
    print("Starting scheduled scrape job...")
    
    try:
        conn = await pool.acquire()
        try:
            response = requests.get(os.environ.get("SHIPS_PRODUCTION_GOOGLE_SHEET"))
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('tbody')
            if not table:
                print("Table not found in HTML.")
                return
            rows = table.find_all('tr')
            
            async with conn.transaction():
                for i, row in enumerate(rows):
                    row_id = i + 1
                    
                    cells = row.find_all('td')
                    if len(cells) < 4:
                        continue
                    if not cells[0].get_text(strip=True) and not cells[1].get_text(strip=True) and not cells[3].get_text(strip=True):
                        continue
                    
                    ship_and_price_text = cells[1].get_text(strip=True)
                    ship_type, price = parse_ship_type(ship_and_price_text)
                    
                    if ship_type == "model":
                        continue

                    username = cells[0].get_text(strip=True)
                    completed_cell = cells[2]
                    is_completed = bool(completed_cell.find('use', href='#checked-checkbox-id'))
                    notes = cells[3].get_text(strip=True)
                    
                    await conn.execute(
                        """
                        INSERT INTO ship_production (orderid, username, shiptype, price, completed, notes)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (orderid) DO UPDATE
                        SET
                            username = EXCLUDED.username,
                            shiptype = EXCLUDED.shiptype,
                            price = EXCLUDED.price,
                            completed = EXCLUDED.completed,
                            notes = EXCLUDED.notes;
                        """,
                        row_id, username, ship_type, price, is_completed, notes
                    )
            print("Data scraped and saved successfully.")
        finally:
            await pool.release(conn)
    except Exception as e:
        print(f"Error in cron job: {e}")

@app.get('/')
async def base():
   return {"status": "online"}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=9601, reload=True, log_level="info")