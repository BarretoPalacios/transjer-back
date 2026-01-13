from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class PermissionCreate(BaseModel):
    name: str
    description: str
    resource: str
    action: str

class PermissionResponse(BaseModel):
    id: str
    name: str
    description: str
    resource: str
    action: str
    created_at: datetime

class PermissionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    resource: Optional[str] = None
    action: Optional[str] = None