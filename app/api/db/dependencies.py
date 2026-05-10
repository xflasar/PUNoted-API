# app/api/dependencies.py (or wherever you keep your dependencies)
from fastapi import Request


def get_db(request: Request):
    """
    Safely retrieves the initialized Database wrapper from application state.
    """
    return request.app.state.db
