from bson import ObjectId
from typing import List
from app.modules.auth.models.role import RoleModel

class RoleService:
    def __init__(self, db):
        self.db = db
        self.collection = db["roles"]

    def create_role(self, role_data: dict):
        existing_role = self.collection.find_one({"name": role_data["name"]})
        
        if existing_role:
            raise ValueError("El rol ya existe")
        
        role_model = RoleModel(
            name=role_data["name"],
            description=role_data["description"],
            permission_ids=[str(pid) for pid in role_data.get("permission_ids", [])]
        )
        
        result = self.collection.insert_one(role_model.to_dict())
        
        created_role = self.collection.find_one({"_id": result.inserted_id})
        created_role["id"] = str(created_role["_id"])
        
        return created_role

    def get_role_by_id(self, role_id: str):
        return self.collection.find_one({"_id": ObjectId(role_id)})

    def get_all_roles(self) -> List[dict]:
        roles = list(self.collection.find())
        for role in roles:
            role["id"] = str(role["_id"])
        return roles

    def update_role(self, role_id: str, update_data: dict):
        update_dict = {k: v for k, v in update_data.items() if v is not None}
        
        if not update_dict:
            return self.get_role_by_id(role_id)
        
        self.collection.update_one(
            {"_id": ObjectId(role_id)},
            {"$set": update_dict}
        )
        
        return self.get_role_by_id(role_id)

    def delete_role(self, role_id: str):
        result = self.collection.delete_one({"_id": ObjectId(role_id)})
        return result.deleted_count > 0