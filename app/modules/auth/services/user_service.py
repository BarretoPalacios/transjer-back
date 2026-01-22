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

            role_names = user_data.get("role_ids", [])
            role_ids = []
            
            if role_names:
                db_roles = list(self.roles_collection.find({"name": {"$in": role_names}}))
                role_ids = [str(role["_id"]) for role in db_roles]

                if len(role_ids) != len(role_names):
                    found_names = [r["name"] for r in db_roles]
                    missing = list(set(role_names) - set(found_names))
                    raise ValueError(f"Roles no válidos: {', '.join(missing)}")

            user_model = UserModel(
                email=user_data["email"],
                username=user_data["username"],
                hashed_password=get_password_hash(user_data["password"]),
                full_name=user_data["full_name"],
                role_ids=role_ids
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

    def delete_user(self, user_id: str) -> bool:
        """
        Elimina un usuario por su ID.
        Retorna True si se eliminó, False si no se encontró.
        """
        result = self.collection.delete_one({"_id": ObjectId(user_id)})
        return result.deleted_count > 0

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
    
    def get_user_permissions_and_roles(self, user_id: str):
        user = self.collection.find_one({"_id": ObjectId(user_id)})
        if not user: return None
        
        user["id"] = str(user.pop("_id"))
        user.pop("hashed_password", None)

        # 2. Traer todos sus roles
        role_ids = [ObjectId(rid) for rid in user.get("role_ids", [])]
        roles = list(self.roles_collection.find({"_id": {"$in": role_ids}}))

        # 3. Traer todos los permisos que pertenecen a esos roles
        all_p_ids = []
        for r in roles: all_p_ids.extend([ObjectId(pid) for pid in r.get("permission_ids", [])])
        
        permissions = list(self.permissions_collection.find({"_id": {"$in": list(set(all_p_ids))}}))
        
        # Mapear permisos por ID para insertarlos fácil en los roles
        p_map = {str(p["_id"]): {"id": str(p["_id"]), "resource": p["resource"], "action": p["action"]} for p in permissions}

        # 4. Estructurar la respuesta final (importante)
        user["roles"] = []
        for r in roles:
            role_data = {
                "id": str(r["_id"]),
                "name": r["name"],
                "permissions": [p_map[str(pid)] for pid in r.get("permission_ids", []) if str(pid) in p_map]
            }
            user["roles"].append(role_data)

        user.pop("role_ids", None) # Quitamos la lista de IDs simple
        return user
    def get_all_users_with_roles(self) -> List[dict]:
        """
        Obtiene todos los usuarios uniendo la información completa de sus roles.
        """
        pipeline = [
            # 1. Convertimos los IDs de roles (que guardas como strings) a ObjectId para el join
            {
                "$addFields": {
                    "role_object_ids": {
                        "$map": {
                            "input": "$role_ids",
                            "as": "rid",
                            "in": {"$toObjectId": "$$rid"}
                        }
                    }
                }
            },
            # 2. Realizamos el 'lookup' (JOIN) con la colección de roles
            {
                "$lookup": {
                    "from": "roles",              # Colección destino
                    "localField": "role_object_ids", # Campo en 'users'
                    "foreignField": "_id",         # Campo en 'roles'
                    "as": "roles_info"             # Nombre del nuevo campo con los resultados
                }
            },
            # 3. Proyectamos (limpiamos) el resultado final
            {
                "$project": {
                    "_id": 0,
                    "id": {"$toString": "$_id"},
                    "username": 1,
                    "email": 1,
                    "full_name": 1,
                    "is_active": 1,
                    "roles": {
                        "$map": {
                            "input": "$roles_info",
                            "as": "role",
                            "in": {
                                "id": {"$toString": "$$role._id"},
                                "name": "$$role.name"
                            }
                        }
                    }
                }
            }
        ]
        
        return list(self.collection.aggregate(pipeline))