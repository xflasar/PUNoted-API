import gzip
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from prometheus_fastapi_instrumentator import Instrumentator
from redis import asyncio as aioredis
from slowapi.errors import RateLimitExceeded
from starlette.types import ASGIApp, Receive, Scope, Send
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from app.core.event_manager import EventManager
from app.core.limiter import limiter, rate_limit_exceeded_handler
from app.routers.buildings import buildings_router as internal_buildings_router
from app.routers.materials import materials_router as internal_materials_router
from app.services.background import scrape_and_save_data, scrape_prices_and_save_data
from auth import auth_router
from data_handlers import BackgroundTasks, data_router
from db import Database
from discord_bot.webhook import router as discord_router
from endpoints.Protected.routers.accounting import (
    accounting_router as api_useraccounting_external_router,
)
from endpoints.Protected.routers.contracts import (
    contracts_router as api_usercontracts_external_router,
)
from endpoints.Protected.routers.corporation import corporation_router as corporation_external_router
from endpoints.Protected.routers.cxuser import cxuser_router as api_cxuser_router
from endpoints.Protected.routers.flights import (
    flights_router as api_userflights_external_router,
)
from endpoints.Protected.routers.production import (
    production_router as api_userproduction_external_router,
)
from endpoints.Protected.routers.ships import (
    ships_router as api_userships_external_router,
)
from endpoints.Protected.routers.sites import (
    sites_router as api_usersites_external_router,
)
from endpoints.Protected.routers.storageuser import (
    storage_router as api_storageuser_router,
)
from endpoints.Protected.routers.user import user_router as api_user_router
from endpoints.Protected.routers.workforce import (
    workforce_router as api_userworkforce_external_router,
)
from endpoints.Public.routers.buildings import buildings_router as buildings_public_external_router
from endpoints.Public.routers.company import company_router as company_public_external_router
from endpoints.Public.routers.corp import corporation_router as corporation_public_external_router
from endpoints.Public.routers.cx import cx_router as api_cx_external_router
from endpoints.Public.routers.materials import materials_router as api_materials_external_router
from endpoints.Public.routers.planets import planets_router as api_planets_external_router
from endpoints.Public.routers.vendors import (
    vendors_router as api_vendors_external_router,
)
from routers.cxuser import cx_router
from routers.flights import flights_router
from routers.governance import governance_router
from routers.group import group_router
from routers.internal.contracts import contracts_router
from routers.internal.corporation import corporation_internal_router
from routers.internal.corporation_ships import corp_ships_internal_router
from routers.internal.cx import cx_internal_router
from routers.internal.data_group import group_router as internal_data_group_router
from routers.internal.finances import finances_router
from routers.internal.leaderboard import leaderboard_router
from routers.internal.production import production_router
from routers.internal.storage import storage_router
from routers.internal.users import users_router
from routers.internal.ships import ships_router
from routers.internal.sites import sites_router
from routers.logistics import logistics_router
from routers.map import map_router
from routers.planets import planets_router
from routers.user import user_router
from routers.usersettings import user_settings_router as settings_router
from routers.vendor import vendor_router
from routers.websocket_router import ws_router

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Lifecycle Events ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Database
    await db.create_pool()
    app.state.db = db
    v1_app.state.db = db  # Propagate to sub-app
    print("Database connected.")

    # 2. Redis Cache
    redis = aioredis.from_url("redis://localhost:6379/0", encoding="utf8", decode_responses=True)
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
    print("Redis initialized.")

    event_manager = EventManager(db.pool)
    app.state.event_manager = event_manager
    v1_app.state.event_manager = event_manager  # Propagate to sub-app

    print("System initialized successfully.")

    yield

    print("Shutting down...")
    if hasattr(db, 'pool') and db.pool:
        await db.pool.close()

# --- App Initialization ---
import os

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# Main application (internal)
app = FastAPI(
    title="PUNoted API (Internal)",
    description="Internal endpoints and main frontend API.",
    version="1.0.0",
    lifespan=lifespan,
    openapi_url="/openapi.json" if DEBUG_MODE else None,
    docs_url="/docs" if DEBUG_MODE else None,
    redoc_url=None,
)

# V1 Sub-application for public external API
v1_app = FastAPI(
    title="PUNoted API (v1)",
    description="Public API for PrUn data.",
    version="1.0.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url=None,
)

app.state.limiter = limiter
v1_app.state.limiter = limiter

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    try:
        body = await request.json()
    except Exception:
        body = await request.body()
    logger.error(f"Validation error on {request.method} {request.url.path}: {exc.errors()} | Received body: {body}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": str(body)}
    )

app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
v1_app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

Instrumentator().instrument(app).expose(app)

class SecurityLoggerMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        start_time = time.perf_counter()
        status_code = [200]

        async def wrapped_send(message: dict):
            if message["type"] == "http.response.start":
                status_code[0] = message["status"]
            await send(message)

        # Process request
        await self.app(scope, receive, wrapped_send)

        # Post-processing
        process_time = (time.perf_counter() - start_time) * 1000
        path = scope.get("path", "")

        if not any(p in path for p in ["/status", "/metrics", "/favicon.ico"]):
            # Look for the tags created by DecompressMiddleware
            orig_size = scope.get("original_gzip_size")
            decomp_size = scope.get("decompressed_size")

            headers = dict(scope.get("headers", []))
            # Safe IP detection from b"headers" list
            xf = headers.get(b"x-forwarded-for", b"").decode().split(",")[0].strip()
            real_ip = xf if xf else (scope.get("client")[0] if scope.get("client") else "unknown")

            if orig_size is not None:
                size_mb = orig_size / (1024 * 1024)
                ratio = (decomp_size / orig_size) if orig_size > 0 else 0
                tag = f"GZIP ({ratio:.1f}x to {decomp_size / (1024*1024):.2f}MB)"
            else:
                # Fallback for RAW requests
                try:
                    cl = int(headers.get(b"content-length", 0))
                except: cl = 0
                size_mb = cl / (1024 * 1024)
                tag = "RAW"

            log_msg = (
                f"SECURITY: {real_ip} | {scope['method']} {path} | "
                f"SIZE: {size_mb:.2f}MB | {tag} | "
                f"STATUS: {status_code[0]} | TIME: {process_time:.2f}ms"
            )

            if size_mb > 5 or status_code[0] >= 400:
                logger.warning(log_msg)
            else:
                logger.info(log_msg)


class DecompressMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        content_encoding = headers.get(b"content-encoding", b"")

        if b"gzip" in content_encoding:
            # 1. Strip the encoding headers so downstream doesn't try to decompress again
            new_headers = [
                (k, v) for k, v in scope["headers"]
                if k.lower() not in (b"content-encoding", b"content-length")
            ]
            scope["headers"] = new_headers

            # 2. Collect the full compressed body
            body_chunks = []
            while True:
                message = await receive()
                if message["type"] == "http.request":
                    body_chunks.append(message.get("body", b""))
                    if not message.get("more_body", False):
                        break
                else:
                    # If we get a disconnect or other event, pass it through
                    await self.app(scope, lambda: message, send)
                    return

            full_body = b"".join(body_chunks)

            # 3. Decompress
            try:
                if not full_body:
                    decompressed_body = b""
                else:
                    decompressed_body = gzip.decompress(full_body)

                # Metrics for your logs
                scope["original_gzip_size"] = len(full_body)
                scope["decompressed_size"] = len(decompressed_body)
            except Exception as e:
                logger.error(f"GZIP DECOMPRESSION FAILED: {e}")
                # Fallback to original body if decompression fails
                decompressed_body = full_body

            # 4. Create a stateful receive function
            # Downstream apps (and Sentry) call receive() multiple times.
            # First call gets the data. Second call gets empty + more_body: False.
            has_sent_body = False

            async def _receive() -> dict:
                nonlocal has_sent_body
                if not has_sent_body:
                    has_sent_body = True
                    return {
                        "type": "http.request",
                        "body": decompressed_body,
                        "more_body": False
                    }
                # Subsequent calls return a disconnect or empty state
                return {"type": "http.request", "body": b"", "more_body": False}

            await self.app(scope, _receive, send)
            return

        # Not GZIP? Just pass through
        await self.app(scope, receive, send)

app.add_middleware(DecompressMiddleware)
app.add_middleware(SecurityLoggerMiddleware)


# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174", "http://127.0.0.1:5174", "https://punoted.net", "*"],
    allow_origin_regex=r"(chrome|moz)-extension://.*", 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["Authorization", "Content-Type", "X-Data-Token"],
)

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(GZipMiddleware, minimum_size=1000)

db = Database()

@app.on_event("shutdown")
async def shutdown_event():
    for task in BackgroundTasks:
        task.cancel()
    await db.close_pool()
    # await bot.close()
    print("System shutdown complete.")

# --- Route Includes ---
# Core
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(data_router, tags=["data"])
app.include_router(discord_router, prefix="/discord")
app.include_router(ws_router)

# Application Logic
app.include_router(group_router, prefix="/groups")
app.include_router(map_router, prefix="/map")
#app.include_router(contracts_router)
app.include_router(logistics_router)
app.include_router(cx_router, prefix="/cx", tags=["cx"])
app.include_router(governance_router, prefix="/governance", tags=["governance"])
app.include_router(planets_router, prefix="/planets", tags=["planets"])

# Internal
app.include_router(
    corporation_internal_router,
    prefix="/internal/corporation",
    tags=["API_corporation"],
)
app.include_router(
    corp_ships_internal_router,
    prefix="/internal/corporation",
    tags=["API_corporation_ships"],
)
app.include_router(storage_router, prefix="/internal/storage", tags=["API_storage"])
app.include_router(production_router, prefix="/internal/production", tags=["API_production"])
app.include_router(internal_data_group_router, prefix="/internal/datagroup", tags=["API_group"])
app.include_router(user_router, prefix="/internal/users", tags=["users"])
app.include_router(settings_router, prefix="/internal/settings", tags=["User Settings"])
app.include_router(contracts_router, prefix="/internal/contracts", tags=["Contracts"])
app.include_router(leaderboard_router, prefix="/internal/leaderboard", tags=["Leaderboard"])
app.include_router(finances_router, prefix="/internal/finances", tags=["Finances"])
app.include_router(internal_buildings_router, prefix="/internal/buildings", tags=["Buildings"])
app.include_router(internal_materials_router, prefix="/internal/materials", tags=["Materials"])
app.include_router(cx_internal_router, prefix="/internal/cx", tags=["CX"])
app.include_router(users_router, prefix="/internal/users", tags=["Users"])
app.include_router(ships_router, prefix="/internal/ships", tags=["Ships"])
app.include_router(sites_router, prefix="/internal/sites", tags=["Sites"])
app.include_router(vendor_router, prefix="/internal/vendor", tags=["Vendor"])


# Protected External API v1 (Registered to v1_app sub-app)
v1_app.include_router(api_user_router, prefix="/user", tags=["User Data"])
v1_app.include_router(api_cxuser_router, prefix="/cxuser", tags=["CX User Data"])
v1_app.include_router(api_storageuser_router, prefix="/storages", tags=["Storage User Data"])
v1_app.include_router(api_usercontracts_external_router, prefix="/contracts", tags=["Contracts Data"])
v1_app.include_router(api_userflights_external_router, prefix="/flights", tags=["Flights Data"])
v1_app.include_router(
    api_useraccounting_external_router,
    prefix="/accounting",
    tags=["Accounting Data"],
)
v1_app.include_router(api_usersites_external_router, prefix="/sites", tags=["Sites Data"])
v1_app.include_router(api_userships_external_router, prefix="/ships", tags=["Ships Data"])
v1_app.include_router(
    api_userproduction_external_router,
    prefix="/production",
    tags=["Production Data"],
)
v1_app.include_router(api_userworkforce_external_router, prefix="/workforce", tags=["Workforce Data"])
v1_app.include_router(corporation_external_router, prefix="/corporation", tags=["Corporation Data"])

# -- Public External API v1 (Registered to v1_app sub-app)
v1_app.include_router(api_vendors_external_router, prefix="/vendors", tags=["Vendors Data"])
v1_app.include_router(api_cx_external_router, prefix="/cx", tags=["CX Data"])
v1_app.include_router(api_materials_external_router, prefix="/materials", tags=["Materials Data"])
v1_app.include_router(api_planets_external_router, prefix="/planets", tags=["Planets Data"])
v1_app.include_router(corporation_public_external_router, prefix="/corporation", tags=["Corporation Data"])
v1_app.include_router(buildings_public_external_router, prefix="/buildings", tags=["Buildings Data"])
v1_app.include_router(company_public_external_router, prefix="/company", tags=["Company Data"])

# Flights router is registered under /public/flights to the main app
app.include_router(flights_router, prefix="/public/flights", tags=["flights"])

# If in development mode, register them to the main app as well with "/v1" prefixes,
# so that the main app's /docs page shows ALL endpoints (both internal and public).
if DEBUG_MODE:
    app.include_router(api_user_router, prefix="/v1/user", tags=["User Data"])
    app.include_router(api_cxuser_router, prefix="/v1/cxuser", tags=["CX User Data"])
    app.include_router(api_storageuser_router, prefix="/v1/storages", tags=["Storage User Data"])
    app.include_router(api_usercontracts_external_router, prefix="/v1/contracts", tags=["Contracts Data"])
    app.include_router(api_userflights_external_router, prefix="/v1/flights", tags=["Flights Data"])
    app.include_router(
        api_useraccounting_external_router,
        prefix="/v1/accounting",
        tags=["Accounting Data"],
    )
    app.include_router(api_usersites_external_router, prefix="/v1/sites", tags=["Sites Data"])
    app.include_router(api_userships_external_router, prefix="/v1/ships", tags=["Ships Data"])
    app.include_router(
        api_userproduction_external_router,
        prefix="/v1/production",
        tags=["Production Data"],
    )
    app.include_router(api_userworkforce_external_router, prefix="/v1/workforce", tags=["Workforce Data"])
    app.include_router(corporation_external_router, prefix="/v1/corporation", tags=["Corporation Data"])
    
    app.include_router(api_vendors_external_router, prefix="/v1/vendors", tags=["Vendors Data"])
    app.include_router(api_cx_external_router, prefix="/v1/cx", tags=["CX Data"])
    app.include_router(api_materials_external_router, prefix="/v1/materials", tags=["Materials Data"])
    app.include_router(api_planets_external_router, prefix="/v1/planets", tags=["Planets Data"])
    app.include_router(corporation_public_external_router, prefix="/v1/corporation", tags=["Corporation Data"])
    app.include_router(buildings_public_external_router, prefix="/v1/buildings", tags=["Buildings Data"])
    app.include_router(company_public_external_router, prefix="/v1/company", tags=["Company Data"])

# Mount the v1 sub-app under /v1 prefix
app.mount("/v1", v1_app)


@app.get("/")
def read_main():
    return Response("frontend: https://punoted.net\ndocs: https://api.punoted.net/v1/docs\n")


@app.get("/status")
async def status_check():
    return {"status": "online", "db_check": "success"}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=9901, reload=True, log_level="info")
