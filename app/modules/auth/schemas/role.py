from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class RoleCreate(BaseModel):
    name: str
    description: str
    permission_ids: List[str] = []

class RoleResponse(BaseModel):
    id: str
    name: str
    description: str
    permission_ids: List[str]
    created_at: datetime

class RoleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    permission_ids: Optional[List[str]] = None