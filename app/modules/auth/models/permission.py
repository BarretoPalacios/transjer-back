from datetime import datetime

class PermissionModel:
    def __init__(
        self,
        name: str,
        description: str,
        resource: str,
        action: str,
        created_at: datetime = None,
        _id: str = None
    ):
        self._id = _id
        self.name = name
        self.description = description
        self.resource = resource
        self.action = action
        self.created_at = created_at or datetime.utcnow()

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "resource": self.resource,
            "action": self.action,
            "created_at": self.created_at
        }