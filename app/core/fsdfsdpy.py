# # Módulo de Autenticación FastAPI + MongoDB

# ## Estructura de Carpetas

# ```
# auth_module/
# ├── app/
# │   ├── __init__.py
# │   ├── main.py
# │   ├── config.py
# │   ├── database.py
# │   ├── models/
# │   │   ├── __init__.py
# │   │   ├── user.py
# │   │   ├── role.py
# │   │   └── permission.py
# │   ├── schemas/
# │   │   ├── __init__.py
# │   │   ├── user.py
# │   │   ├── role.py
# │   │   ├── permission.py
# │   │   └── auth.py
# │   ├── services/
# │   │   ├── __init__.py
# │   │   ├── auth_service.py
# │   │   ├── user_service.py
# │   │   ├── role_service.py
# │   │   └── permission_service.py
# │   ├── utils/
# │   │   ├── __init__.py
# │   │   ├── security.py
# │   │   └── dependencies.py
# │   └── routes/
# │       ├── __init__.py
# │       ├── auth.py
# │       ├── users.py
# │       ├── roles.py
# │       └── permissions.py
# ├── requirements.txt
# └── .env
# ```

# ## Archivos de Configuración

# ### requirements.txt
# ```
# fastapi==0.104.1
# uvicorn==0.24.0
# pymongo==4.6.0
# python-jose[cryptography]==3.3.0
# passlib[bcrypt]==1.7.4
# python-multipart==0.0.6
# python-decouple==3.8
# pydantic==2.5.0
# pydantic-settings==2.1.0
# ```

# ### .env
# ```
# MONGODB_URL=mongodb://localhost:27017
# DATABASE_NAME=auth_db
# SECRET_KEY=tu-clave-secreta-super-segura-cambiar-en-produccion
# ALGORITHM=HS256
# ACCESS_TOKEN_EXPIRE_MINUTES=30
# ```

# ## Código de los Archivos

# ### app/config.py
# ```python
# from pydantic_settings import BaseSettings

# class Settings(BaseSettings):
#     MONGODB_URL: str
#     DATABASE_NAME: str
#     SECRET_KEY: str
#     ALGORITHM: str = "HS256"
#     ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

#     class Config:
#         env_file = ".env"

# settings = Settings()
# ```

# ### app/database.py
# ```python
# from pymongo import MongoClient
# from app.config import settings

# class Database:
#     client: MongoClient = None
    
# db = Database()

# def get_database():
#     return db.client[settings.DATABASE_NAME]

# def connect_to_mongo():
#     db.client = MongoClient(settings.MONGODB_URL)
#     print("Conectado a MongoDB")

# def close_mongo_connection():
#     if db.client:
#         db.client.close()
#         print("Conexión cerrada")
# ```

# ### app/models/user.py
# ```python
# from datetime import datetime
# from typing import Optional, List

# class UserModel:
#     def __init__(
#         self,
#         email: str,
#         username: str,
#         hashed_password: str,
#         full_name: str,
#         role_ids: List[str] = None,
#         is_active: bool = True,
#         created_at: datetime = None,
#         _id: str = None
#     ):
#         self._id = _id
#         self.email = email
#         self.username = username
#         self.hashed_password = hashed_password
#         self.full_name = full_name
#         self.role_ids = role_ids or []
#         self.is_active = is_active
#         self.created_at = created_at or datetime.utcnow()

#     def to_dict(self):
#         return {
#             "email": self.email,
#             "username": self.username,
#             "hashed_password": self.hashed_password,
#             "full_name": self.full_name,
#             "role_ids": self.role_ids,
#             "is_active": self.is_active,
#             "created_at": self.created_at
#         }
# ```

# ### app/models/role.py
# ```python
# from datetime import datetime
# from typing import List

# class RoleModel:
#     def __init__(
#         self,
#         name: str,
#         description: str,
#         permission_ids: List[str] = None,
#         created_at: datetime = None,
#         _id: str = None
#     ):
#         self._id = _id
#         self.name = name
#         self.description = description
#         self.permission_ids = permission_ids or []
#         self.created_at = created_at or datetime.utcnow()

#     def to_dict(self):
#         return {
#             "name": self.name,
#             "description": self.description,
#             "permission_ids": self.permission_ids,
#             "created_at": self.created_at
#         }
# ```

# ### app/models/permission.py
# ```python
# from datetime import datetime

# class PermissionModel:
#     def __init__(
#         self,
#         name: str,
#         description: str,
#         resource: str,
#         action: str,
#         created_at: datetime = None,
#         _id: str = None
#     ):
#         self._id = _id
#         self.name = name
#         self.description = description
#         self.resource = resource
#         self.action = action
#         self.created_at = created_at or datetime.utcnow()

#     def to_dict(self):
#         return {
#             "name": self.name,
#             "description": self.description,
#             "resource": self.resource,
#             "action": self.action,
#             "created_at": self.created_at
#         }
# ```

# ### app/schemas/user.py
# ```python
# from pydantic import BaseModel, EmailStr
# from typing import List, Optional
# from datetime import datetime

# class UserCreate(BaseModel):
#     email: EmailStr
#     username: str
#     password: str
#     full_name: str
#     role_ids: List[str] = []

# class UserResponse(BaseModel):
#     id: str
#     email: EmailStr
#     username: str
#     full_name: str
#     role_ids: List[str]
#     is_active: bool
#     created_at: datetime

# class UserUpdate(BaseModel):
#     email: Optional[EmailStr] = None
#     full_name: Optional[str] = None
#     role_ids: Optional[List[str]] = None
#     is_active: Optional[bool] = None
# ```

# ### app/schemas/role.py
# ```python
# from pydantic import BaseModel
# from typing import List, Optional
# from datetime import datetime

# class RoleCreate(BaseModel):
#     name: str
#     description: str
#     permission_ids: List[str] = []

# class RoleResponse(BaseModel):
#     id: str
#     name: str
#     description: str
#     permission_ids: List[str]
#     created_at: datetime

# class RoleUpdate(BaseModel):
#     name: Optional[str] = None
#     description: Optional[str] = None
#     permission_ids: Optional[List[str]] = None
# ```

# ### app/schemas/permission.py
# ```python
# from pydantic import BaseModel
# from typing import Optional
# from datetime import datetime

# class PermissionCreate(BaseModel):
#     name: str
#     description: str
#     resource: str
#     action: str

# class PermissionResponse(BaseModel):
#     id: str
#     name: str
#     description: str
#     resource: str
#     action: str
#     created_at: datetime

# class PermissionUpdate(BaseModel):
#     name: Optional[str] = None
#     description: Optional[str] = None
#     resource: Optional[str] = None
#     action: Optional[str] = None
# ```

# ### app/schemas/auth.py
# ```python
# from pydantic import BaseModel
# from typing import Optional

# class Token(BaseModel):
#     access_token: str
#     token_type: str

# class TokenData(BaseModel):
#     username: Optional[str] = None

# class LoginRequest(BaseModel):
#     username: str
#     password: str
# ```

# ### app/utils/security.py
# ```python
# from datetime import datetime, timedelta
# from typing import Optional
# from jose import JWTError, jwt
# from passlib.context import CryptContext
# from app.config import settings

# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# def verify_password(plain_password: str, hashed_password: str) -> bool:
#     return pwd_context.verify(plain_password, hashed_password)

# def get_password_hash(password: str) -> str:
#     return pwd_context.hash(password)

# def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
#     to_encode = data.copy()
#     if expires_delta:
#         expire = datetime.utcnow() + expires_delta
#     else:
#         expire = datetime.utcnow() + timedelta(minutes=15)
#     to_encode.update({"exp": expire})
#     encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
#     return encoded_jwt

# def decode_token(token: str):
#     try:
#         payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
#         return payload
#     except JWTError:
#         return None
# ```

# ### app/utils/dependencies.py
# ```python
# from fastapi import Depends, HTTPException, status
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from typing import List
# from app.utils.security import decode_token
# from app.services.user_service import UserService
# from app.database import get_database

# security = HTTPBearer()

# def get_current_user(
#     credentials: HTTPAuthorizationCredentials = Depends(security)
# ):
#     token = credentials.credentials
#     payload = decode_token(token)
    
#     if payload is None:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token inválido o expirado"
#         )
    
#     username = payload.get("sub")
#     if username is None:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token inválido"
#         )
    
#     db = get_database()
#     user_service = UserService(db)
#     user = user_service.get_user_by_username(username)
    
#     if user is None:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Usuario no encontrado"
#         )
    
#     return user

# def require_role(allowed_roles: List[str]):
#     def role_checker(current_user: dict = Depends(get_current_user)):
#         db = get_database()
#         user_service = UserService(db)
        
#         user_roles = user_service.get_user_roles(current_user["_id"])
#         user_role_names = [role["name"] for role in user_roles]
        
#         if not any(role in user_role_names for role in allowed_roles):
#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail="No tienes permisos suficientes"
#             )
        
#         return current_user
    
#     return role_checker

# def require_permission(resource: str, action: str):
#     def permission_checker(current_user: dict = Depends(get_current_user)):
#         db = get_database()
#         user_service = UserService(db)
        
#         if user_service.user_has_permission(current_user["_id"], resource, action):
#             return current_user
        
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail=f"No tienes permiso para {action} en {resource}"
#         )
    
#     return permission_checker
# ```

# ### app/services/auth_service.py
# ```python
# from datetime import timedelta
# from fastapi import HTTPException, status
# from app.utils.security import verify_password, create_access_token
# from app.services.user_service import UserService
# from app.config import settings

# class AuthService:
#     def __init__(self, db):
#         self.db = db
#         self.user_service = UserService(db)

#     def authenticate_user(self, username: str, password: str):
#         user = self.user_service.get_user_by_username(username)
        
#         if not user:
#             return None
        
#         if not verify_password(password, user["hashed_password"]):
#             return None
        
#         return user

#     def login(self, username: str, password: str):
#         user = self.authenticate_user(username, password)
        
#         if not user:
#             raise HTTPException(
#                 status_code=status.HTTP_401_UNAUTHORIZED,
#                 detail="Credenciales incorrectas"
#             )
        
#         if not user["is_active"]:
#             raise HTTPException(
#                 status_code=status.HTTP_400_BAD_REQUEST,
#                 detail="Usuario inactivo"
#             )
        
#         access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
#         access_token = create_access_token(
#             data={"sub": user["username"]},
#             expires_delta=access_token_expires
#         )
        
#         return {
#             "access_token": access_token,
#             "token_type": "bearer"
#         }
# ```

# ### app/services/user_service.py
# ```python
# from bson import ObjectId
# from typing import List, Optional
# from app.utils.security import get_password_hash
# from app.models.user import UserModel

# class UserService:
#     def __init__(self, db):
#         self.db = db
#         self.collection = db["users"]
#         self.roles_collection = db["roles"]
#         self.permissions_collection = db["permissions"]

#     def create_user(self, user_data: dict):
#         existing_user = self.collection.find_one({
#             "$or": [
#                 {"email": user_data["email"]},
#                 {"username": user_data["username"]}
#             ]
#         })
        
#         if existing_user:
#             raise ValueError("Usuario o email ya existe")
        
#         user_model = UserModel(
#             email=user_data["email"],
#             username=user_data["username"],
#             hashed_password=get_password_hash(user_data["password"]),
#             full_name=user_data["full_name"],
#             role_ids=[str(rid) for rid in user_data.get("role_ids", [])]
#         )
        
#         result = self.collection.insert_one(user_model.to_dict())
        
#         created_user = self.collection.find_one({"_id": result.inserted_id})
#         created_user["id"] = str(created_user["_id"])
        
#         return created_user

#     def get_user_by_username(self, username: str):
#         return self.collection.find_one({"username": username})

#     def get_user_by_id(self, user_id: str):
#         return self.collection.find_one({"_id": ObjectId(user_id)})

#     def update_user(self, user_id: str, update_data: dict):
#         update_dict = {k: v for k, v in update_data.items() if v is not None}
        
#         if not update_dict:
#             return self.get_user_by_id(user_id)
        
#         self.collection.update_one(
#             {"_id": ObjectId(user_id)},
#             {"$set": update_dict}
#         )
        
#         return self.get_user_by_id(user_id)

#     def get_user_roles(self, user_id: str) -> List[dict]:
#         user = self.get_user_by_id(user_id)
        
#         if not user or not user.get("role_ids"):
#             return []
        
#         role_object_ids = [ObjectId(rid) for rid in user["role_ids"]]
#         roles = list(self.roles_collection.find({"_id": {"$in": role_object_ids}}))
        
#         return roles

#     def user_has_permission(self, user_id: str, resource: str, action: str) -> bool:
#         roles = self.get_user_roles(user_id)
        
#         all_permission_ids = []
#         for role in roles:
#             all_permission_ids.extend(role.get("permission_ids", []))
        
#         if not all_permission_ids:
#             return False
        
#         permission_object_ids = [ObjectId(pid) for pid in all_permission_ids]
        
#         permission = self.permissions_collection.find_one({
#             "_id": {"$in": permission_object_ids},
#             "resource": resource,
#             "action": action
#         })
        
#         return permission is not None
# ```

# ### app/services/role_service.py
# ```python
# from bson import ObjectId
# from typing import List
# from app.models.role import RoleModel

# class RoleService:
#     def __init__(self, db):
#         self.db = db
#         self.collection = db["roles"]

#     def create_role(self, role_data: dict):
#         existing_role = self.collection.find_one({"name": role_data["name"]})
        
#         if existing_role:
#             raise ValueError("El rol ya existe")
        
#         role_model = RoleModel(
#             name=role_data["name"],
#             description=role_data["description"],
#             permission_ids=[str(pid) for pid in role_data.get("permission_ids", [])]
#         )
        
#         result = self.collection.insert_one(role_model.to_dict())
        
#         created_role = self.collection.find_one({"_id": result.inserted_id})
#         created_role["id"] = str(created_role["_id"])
        
#         return created_role

#     def get_role_by_id(self, role_id: str):
#         return self.collection.find_one({"_id": ObjectId(role_id)})

#     def get_all_roles(self) -> List[dict]:
#         roles = list(self.collection.find())
#         for role in roles:
#             role["id"] = str(role["_id"])
#         return roles

#     def update_role(self, role_id: str, update_data: dict):
#         update_dict = {k: v for k, v in update_data.items() if v is not None}
        
#         if not update_dict:
#             return self.get_role_by_id(role_id)
        
#         self.collection.update_one(
#             {"_id": ObjectId(role_id)},
#             {"$set": update_dict}
#         )
        
#         return self.get_role_by_id(role_id)

#     def delete_role(self, role_id: str):
#         result = self.collection.delete_one({"_id": ObjectId(role_id)})
#         return result.deleted_count > 0
# ```

# ### app/services/permission_service.py
# ```python
# from bson import ObjectId
# from typing import List
# from app.models.permission import PermissionModel

# class PermissionService:
#     def __init__(self, db):
#         self.db = db
#         self.collection = db["permissions"]

#     def create_permission(self, permission_data: dict):
#         existing = self.collection.find_one({
#             "resource": permission_data["resource"],
#             "action": permission_data["action"]
#         })
        
#         if existing:
#             raise ValueError("El permiso ya existe")
        
#         permission_model = PermissionModel(
#             name=permission_data["name"],
#             description=permission_data["description"],
#             resource=permission_data["resource"],
#             action=permission_data["action"]
#         )
        
#         result = self.collection.insert_one(permission_model.to_dict())
        
#         created_permission = self.collection.find_one({"_id": result.inserted_id})
#         created_permission["id"] = str(created_permission["_id"])
        
#         return created_permission

#     def get_permission_by_id(self, permission_id: str):
#         return self.collection.find_one({"_id": ObjectId(permission_id)})

#     def get_all_permissions(self) -> List[dict]:
#         permissions = list(self.collection.find())
#         for permission in permissions:
#             permission["id"] = str(permission["_id"])
#         return permissions

#     def update_permission(self, permission_id: str, update_data: dict):
#         update_dict = {k: v for k, v in update_data.items() if v is not None}
        
#         if not update_dict:
#             return self.get_permission_by_id(permission_id)
        
#         self.collection.update_one(
#             {"_id": ObjectId(permission_id)},
#             {"$set": update_dict}
#         )
        
#         return self.get_permission_by_id(permission_id)

#     def delete_permission(self, permission_id: str):
#         result = self.collection.delete_one({"_id": ObjectId(permission_id)})
#         return result.deleted_count > 0
# ```

# ### app/routes/auth.py
# ```python
# from fastapi import APIRouter, Depends, HTTPException
# from app.schemas.auth import LoginRequest, Token
# from app.schemas.user import UserCreate, UserResponse
# from app.services.auth_service import AuthService
# from app.services.user_service import UserService
# from app.database import get_database

# router = APIRouter(prefix="/auth", tags=["Authentication"])

# @router.post("/register", response_model=UserResponse)
# def register(user: UserCreate):
#     db = get_database()
#     user_service = UserService(db)
    
#     try:
#         created_user = user_service.create_user(user.dict())
#         return created_user
#     except ValueError as e:
#         raise HTTPException(status_code=400, detail=str(e))

# @router.post("/login", response_model=Token)
# def login(credentials: LoginRequest):
#     db = get_database()
#     auth_service = AuthService(db)
    
#     return auth_service.login(credentials.username, credentials.password)
# ```

# ### app/routes/users.py
# ```python
# from fastapi import APIRouter, Depends, HTTPException
# from typing import List
# from app.schemas.user import UserResponse, UserUpdate
# from app.services.user_service import UserService
# from app.database import get_database
# from app.utils.dependencies import get_current_user, require_role

# router = APIRouter(prefix="/users", tags=["Users"])

# @router.get("/me", response_model=UserResponse)
# def get_current_user_info(current_user: dict = Depends(get_current_user)):
#     current_user["id"] = str(current_user["_id"])
#     return current_user

# @router.put("/me", response_model=UserResponse)
# def update_current_user(
#     user_update: UserUpdate,
#     current_user: dict = Depends(get_current_user)
# ):
#     db = get_database()
#     user_service = UserService(db)
    
#     updated_user = user_service.update_user(
#         str(current_user["_id"]),
#         user_update.dict(exclude_unset=True)
#     )
#     updated_user["id"] = str(updated_user["_id"])
#     return updated_user

# @router.get("/{user_id}", response_model=UserResponse)
# def get_user(
#     user_id: str,
#     current_user: dict = Depends(require_role(["admin"]))
# ):
#     db = get_database()
#     user_service = UserService(db)
    
#     user = user_service.get_user_by_id(user_id)
    
#     if not user:
#         raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
#     user["id"] = str(user["_id"])
#     return user
# ```

# ### app/routes/roles.py
# ```python
# from fastapi import APIRouter, Depends, HTTPException
# from typing import List
# from app.schemas.role import RoleCreate, RoleResponse, RoleUpdate
# from app.services.role_service import RoleService
# from app.database import get_database
# from app.utils.dependencies import require_permission

# router = APIRouter(prefix="/roles", tags=["Roles"])

# @router.post("/", response_model=RoleResponse)
# def create_role(
#     role: RoleCreate,
#     current_user: dict = Depends(require_permission("roles", "create"))
# ):
#     db = get_database()
#     role_service = RoleService(db)
    
#     try:
#         created_role = role_service.create_role(role.dict())
#         return created_role
#     except ValueError as e:
#         raise HTTPException(status_code=400, detail=str(e))

# @router.get("/", response_model=List[RoleResponse])
# def get_all_roles(
#     current_user: dict = Depends(require_permission("roles", "read"))
# ):
#     db = get_database()
#     role_service = RoleService(db)
    
#     return role_service.get_all_roles()

# @router.get("/{role_id}", response_model=RoleResponse)
# def get_role(
#     role_id: str,
#     current_user: dict = Depends(require_permission("roles", "read"))
# ):
#     db = get_database()
#     role_service = RoleService(db)
    
#     role = role_service.get_role_by_id(role_id)
    
#     if not role:
#         raise HTTPException(status_code=404, detail="Rol no encontrado")
    
#     role["id"] = str(role["_id"])
#     return role

# @router.put("/{role_id}", response_model=RoleResponse)
# def update_role(
#     role_id: str,
#     role_update: RoleUpdate,
#     current_user: dict = Depends(require_permission("roles", "update"))
# ):
#     db = get_database()
#     role_service = RoleService(db)
    
#     updated_role = role_service.update_role(
#         role_id,
#         role_update.dict(exclude_unset=True)
#     )
    
#     if not updated_role:
#         raise HTTPException(status_code=404, detail="Rol no encontrado")
    
#     updated_role["id"] = str(updated_role["_id"])
#     return updated_role

# @router.delete("/{role_id}")
# def delete_role(
#     role_id: str,
#     current_user: dict = Depends(require_permission("roles", "delete"))
# ):
#     db = get_database()
#     role_service = RoleService(db)
    
#     deleted = role_service.delete_role(role_id)
    
#     if not deleted:
#         raise HTTPException(status_code=404, detail="Rol no encontrado")
    
#     return {"message": "Rol eliminado exitosamente"}
# ```

# ### app/routes/permissions.py
# ```python
# from fastapi import APIRouter, Depends, HTTPException
# from typing import List
# from app.schemas.permission import PermissionCreate, PermissionResponse, PermissionUpdate
# from app.services.permission_service import PermissionService
# from app.database import get_database
# from app.utils.dependencies import require_permission

# router = APIRouter(prefix="/permissions", tags=["Permissions"])

# @router.post("/", response_model=PermissionResponse)
# def create_permission(
#     permission: PermissionCreate,
#     current_user: dict = Depends(require_permission("permissions", "create"))
# ):
#     db = get_database()
#     permission_service = PermissionService(db)
    
#     try:
#         created_permission = permission_service.create_permission(permission.dict())
#         return created_permission
#     except ValueError as e:
#         raise HTTPException(status_code=400, detail=str(e))

# @router.get("/", response_model=List[PermissionResponse])
# def get_all_permissions(
#     current_user: dict = Depends(require_permission("permissions", "read"))
# ):
#     db = get_database()
#     permission_service = PermissionService(db)
    
#     return permission_service.get_all_permissions()

# @router.get("/{permission_id}", response_model=PermissionResponse)
# def get_permission(
#     permission_id: str,
#     current_user: dict = Depends(require_permission("permissions", "read"))
# ):
#     db = get_database()
#     permission_service = PermissionService(db)
    
#     permission = permission_service.get_permission_by_id(permission_id)
    
#     if not permission:
#         raise HTTPException(status_code=404, detail="Permiso no encontrado")
    
#     permission["id"] = str(permission["_id"])
#     return permission

# @router.put("/{permission_id}", response_model=PermissionResponse)
# def update_permission(
#     permission_id: str,
#     permission_update: PermissionUpdate,
#     current_user: dict = Depends(require_permission("permissions", "update"))
# ):
#     db = get_database()
#     permission_service = PermissionService(db)
    
#     updated_permission = permission_service.update_permission(
#         permission_id,
#         permission_update.dict(exclude_unset=True)
#     )
    
#     if not updated_permission:
#         raise HTTPException(status_code=404, detail="Permiso no encontrado")
    
#     updated_permission["id"] = str(updated_permission["_id"])
#     return updated_permission

# @router.delete("/{permission_id}")
# def delete_permission(
#     permission_id: str,
#     current_user: dict = Depends(require_permission("permissions", "delete"))
# ):
#     db = get_database()
#     permission_service = PermissionService(db)
    
#     deleted = permission_service.delete_permission(permission_id)
    
#     if not deleted:
#         raise HTTPException(status_code=404, detail="Permiso no encontrado")
    
#     return {"message": "Permiso eliminado exitosamente"}
# ```

# ### app/main.py
# ```python
# from fastapi import FastAPI
# from app.database import connect_to_mongo, close_mongo_connection
# from app.routes import auth, users, roles, permissions

# app = FastAPI(title="API de Autenticación", version="1.0.0")

# @app.on_event("startup")
# def startup_db_client():
#     connect_to_mongo()

# @app.on_event("shutdown")
# def shutdown_db_client():
#     close_mongo_connection()

# app.include_router(auth.router)
# app.include_router(users.router)
# app.include_router(roles.router)
# app.include_router(permissions.router)

# @app.get("/")
# def read_root():
#     return {"message": "API de Autenticación con FastAPI y MongoDB"}
# ```

# ## Instrucciones de Uso

# 1. **Instalación:**
# ```bash
# pip install -r requirements.txt
# ```

# 2. **Configurar .env** con tus credenciales

# 3. **Iniciar el servidor:**
# ```bash
# uvicorn app.main:app --reload
# ```

# 4. **Crear datos iniciales (ejecutar una vez):**
# ```python
# # Script para crear administrador y permisos básicos
# from pymongo import MongoClient
# from app.utils.security import get_password_hash

# client = MongoClient("mongodb://localhost:27017")
# db = client["auth_db"]

# # Crear permisos
# permissions = [
#     {"name": "Crear Roles", "description": "Permite crear roles", "resource": "roles", "action": "create"},
#     {"name": "Leer Roles", "description": "Permite ver roles", "resource": "roles", "action": "read"},
#     {"name