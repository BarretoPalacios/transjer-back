from pydantic import BaseModel, EmailStr
from typing import List, Optional
from datetime import datetime

class UserCreate(BaseModel):
    email: EmailStr
    username: str
    password: str
    full_name: str
    role_ids: List[str] = []

class UserResponse(BaseModel):
    id: str
    email: EmailStr
    username: str
    full_name: str
    role_ids: List[str]
    is_active: bool
    created_at: datetime

class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    role_ids: Optional[List[str]] = None
    is_active: Optional[bool] = None