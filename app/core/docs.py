# app/core/docs.py

from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi


def custom_openapi(app: FastAPI):
    if app.openapi_schema:
        return app.openapi_schema

    v1_routes = [route for route in app.routes if getattr(route, "path", "").startswith("/v1")]

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=v1_routes,
    )

    # 1. Force OpenAPI 3.0.2
    schema["openapi"] = "3.0.2"

    # 2. Define Servers so "Try it out" works
    # These paths will be prepended to the API calls
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
    Serves Swagger UI with a RELATIVE path to the schema.
    """
    return get_swagger_ui_html(
        openapi_url="../openapi.json",
        title="PUNoted API - Swagger UI",
        swagger_js_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css",
        swagger_ui_parameters={
            "syntaxHighlight": False,
            "defaultModelsExpandDepth": -1,
        },
    )
