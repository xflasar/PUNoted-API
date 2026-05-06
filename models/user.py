# Pydantic models
from typing import Optional

from pydantic import BaseModel


class UserSettingsUpdate(BaseModel):
    displayName: Optional[str]
    fioApiKey: Optional[str]


class UserSettingsOut(BaseModel):
    username: str
    displayName: Optional[str]
    companyName: Optional[str]
    companyCode: Optional[str]
    isVerified: bool
    isSynchronized: bool
    fioApiKey: Optional[str]
