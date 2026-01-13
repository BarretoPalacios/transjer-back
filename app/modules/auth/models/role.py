
from datetime import datetime
from typing import List

class RoleModel:
    def __init__(
        self,
        name: str,
        description: str,
        permission_ids: List[str] = None,
        created_at: datetime = None,
        _id: str = None
    ):
        self._id = _id
        self.name = name
        self.description = description
        self.permission_ids = permission_ids or []
        self.created_at = created_at or datetime.utcnow()

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "permission_ids": self.permission_ids,
            "created_at": self.created_at
        }