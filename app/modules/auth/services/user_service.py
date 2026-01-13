from bson import ObjectId
from typing import List, Optional
from app.modules.auth.utils.security import get_password_hash
from app.modules.auth.models.user import UserModel

class UserService:
    def __init__(self, db):
        self.db = db
        self.collection = db["users"]
        self.roles_collection = db["roles"]
        self.permissions_collection = db["permissions"]

    def create_user(self, user_data: dict):
        existing_user = self.collection.find_one({
            "$or": [
                {"email": user_data["email"]},
                {"username": user_data["username"]}
            ]
        })
        
        if existing_user:
            raise ValueError("Usuario o email ya existe")
        
        user_model = UserModel(
            email=user_data["email"],
            username=user_data["username"],
            hashed_password=get_password_hash(user_data["password"]),
            full_name=user_data["full_name"],
            role_ids=[str(rid) for rid in user_data.get("role_ids", [])]
        )
        
        result = self.collection.insert_one(user_model.to_dict())
        
        created_user = self.collection.find_one({"_id": result.inserted_id})
        created_user["id"] = str(created_user["_id"])
        
        return created_user

    def get_user_by_username(self, username: str):
        return self.collection.find_one({"username": username})
    
    def get_user_by_email(self, email: str):
        return self.collection.find_one({"email": email})

    def get_user_by_id(self, user_id: str):
        return self.collection.find_one({"_id": ObjectId(user_id)})

    def update_user(self, user_id: str, update_data: dict):
        update_dict = {k: v for k, v in update_data.items() if v is not None}
        
        if not update_dict:
            return self.get_user_by_id(user_id)
        
        self.collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": update_dict}
        )
        
        return self.get_user_by_id(user_id)

    def get_user_roles(self, user_id: str) -> List[dict]:
        user = self.get_user_by_id(user_id)
        
        if not user or not user.get("role_ids"):
            return []
        
        role_object_ids = [ObjectId(rid) for rid in user["role_ids"]]
        roles = list(self.roles_collection.find({"_id": {"$in": role_object_ids}}))
        
        return roles

    def user_has_permission(self, user_id: str, resource: str, action: str) -> bool:
        roles = self.get_user_roles(user_id)
        
        all_permission_ids = []
        for role in roles:
            all_permission_ids.extend(role.get("permission_ids", []))
        
        if not all_permission_ids:
            return False
        
        permission_object_ids = [ObjectId(pid) for pid in all_permission_ids]
        
        permission = self.permissions_collection.find_one({
            "_id": {"$in": permission_object_ids},
            "resource": resource,
            "action": action
        })
        
        return permission is not None