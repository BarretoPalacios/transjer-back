from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List
from app.modules.auth.utils.security import decode_token
from app.modules.auth.services.user_service import UserService
from app.core.database import get_database

security = HTTPBearer()

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    token = credentials.credentials
    payload = decode_token(token)
    
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado"
        )
    
    username = payload.get("sub")
    if username is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido"
        )
    
    db = get_database()
    user_service = UserService(db)
    user = user_service.get_user_by_username(username)
    
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario no encontrado"
        )
    
    return user

def require_role(allowed_roles: List[str]):
    def role_checker(current_user: dict = Depends(get_current_user)):
        db = get_database()
        user_service = UserService(db)
        
        user_roles = user_service.get_user_roles(current_user["_id"])
        user_role_names = [role["name"] for role in user_roles]
        
        if not any(role in user_role_names for role in allowed_roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos suficientes"
            )
        
        return current_user
    
    return role_checker

def require_permission(resource: str, action: str):
    def permission_checker(current_user: dict = Depends(get_current_user)):
        db = get_database()
        user_service = UserService(db)
        
        if user_service.user_has_permission(current_user["_id"], resource, action):
            return current_user
        
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"No tienes permiso para {action} en {resource}"
        )
    
    return permission_checker