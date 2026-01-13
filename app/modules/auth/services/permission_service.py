from bson import ObjectId
from typing import List
from app.modules.auth.models.permission import PermissionModel

class PermissionService:
    def __init__(self, db):
        self.db = db
        self.collection = db["permissions"]

    def create_permission(self, permission_data: dict):
        existing = self.collection.find_one({
            "resource": permission_data["resource"],
            "action": permission_data["action"]
        })
        
        if existing:
            raise ValueError("El permiso ya existe")
        
        permission_model = PermissionModel(
            name=permission_data["name"],
            description=permission_data["description"],
            resource=permission_data["resource"],
            action=permission_data["action"]
        )
        
        result = self.collection.insert_one(permission_model.to_dict())
        
        created_permission = self.collection.find_one({"_id": result.inserted_id})
        created_permission["id"] = str(created_permission["_id"])
        
        return created_permission

    def get_permission_by_id(self, permission_id: str):
        return self.collection.find_one({"_id": ObjectId(permission_id)})

    def get_all_permissions(self) -> List[dict]:
        permissions = list(self.collection.find())
        for permission in permissions:
            permission["id"] = str(permission["_id"])
        return permissions

    def update_permission(self, permission_id: str, update_data: dict):
        update_dict = {k: v for k, v in update_data.items() if v is not None}
        
        if not update_dict:
            return self.get_permission_by_id(permission_id)
        
        self.collection.update_one(
            {"_id": ObjectId(permission_id)},
            {"$set": update_dict}
        )
        
        return self.get_permission_by_id(permission_id)

    def delete_permission(self, permission_id: str):
        result = self.collection.delete_one({"_id": ObjectId(permission_id)})
        return result.deleted_count > 0