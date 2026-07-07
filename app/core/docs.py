# app/core/docs.py

from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi


def custom_openapi(app: FastAPI):
    if app.openapi_schema and app.openapi_schema.get("paths"):
        return app.openapi_schema

    root_path = (app.root_path or "").rstrip("/")
    v1_routes = []
    for route in app.routes:
        path = getattr(route, "path", "")
        # Match routes starting with /v1, or starting with root_path/v1
        if path.startswith("/v1") or (root_path and path.startswith(f"{root_path}/v1")):
            v1_routes.append(route)

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=v1_routes,
    )

    # 1. Force OpenAPI 3.0.2
    schema["openapi"] = "3.0.2"

    # 2. Define Servers so "Try it out" works
    schema["servers"] = [
        {"url": "/", "description": "Production Proxy"},
        {"url": "/dev/", "description": "Development Proxy"},
        {"url": "/", "description": "Local / Direct"},
    ]

    if "components" not in schema:
        schema["components"] = {}

    schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        }
    }
    schema["security"] = [{"BearerAuth": []}]

    app.openapi_schema = schema
    return app.openapi_schema


async def api_v1_docs(request: Request):
    """
    Serves Swagger UI with an absolute path to the schema.
    """
    root_path = request.scope.get("root_path", "").rstrip("/")
    return get_swagger_ui_html(
        openapi_url=f"{root_path}/v1/openapi.json",
        title="PUNoted API - Swagger UI",
        swagger_js_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css",
        swagger_ui_parameters={
            "syntaxHighlight": False,
            "defaultModelsExpandDepth": -1,
        },
    )