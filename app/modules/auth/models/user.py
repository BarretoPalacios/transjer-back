from datetime import datetime
from typing import Optional, List

class UserModel:
    def __init__(
        self,
        email: str,
        username: str,
        hashed_password: str,
        full_name: str,
        role_ids: List[str] = None,
        is_active: bool = True,
        created_at: datetime = None,
        _id: str = None
    ):
        self._id = _id
        self.email = email
        self.username = username
        self.hashed_password = hashed_password
        self.full_name = full_name
        self.role_ids = role_ids or []
        self.is_active = is_active
        self.created_at = created_at or datetime.utcnow()

    def to_dict(self):
        return {
            "email": self.email,
            "username": self.username,
            "hashed_password": self.hashed_password,
            "full_name": self.full_name,
            "role_ids": self.role_ids,
            "is_active": self.is_active,
            "created_at": self.created_at
        }