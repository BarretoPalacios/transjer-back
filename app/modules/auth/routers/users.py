from fastapi import APIRouter, Depends, HTTPException
from typing import List
from app.modules.auth.schemas.user import UserResponse, UserUpdate
from app.modules.auth.services.user_service import UserService
from app.core.database import get_database
from app.modules.auth.utils.dependencies import get_current_user, require_role

router = APIRouter(prefix="/users", tags=["Users"])

@router.get("/me", response_model=UserResponse)
def get_current_user_info(current_user: dict = Depends(get_current_user)):
    current_user["id"] = str(current_user["_id"])
    return current_user

@router.put("/me", response_model=UserResponse)
def update_current_user(
    user_update: UserUpdate,
    current_user: dict = Depends(get_current_user)
):
    db = get_database()
    user_service = UserService(db)
    
    updated_user = user_service.update_user(
        str(current_user["_id"]),
        user_update.dict(exclude_unset=True)
    )
    updated_user["id"] = str(updated_user["_id"])
    return updated_user

@router.get("/{user_id}", response_model=UserResponse)
def get_user(
    user_id: str,
    current_user: dict = Depends(require_role(["admin"]))
):
    db = get_database()
    user_service = UserService(db)
    
    user = user_service.get_user_by_id(user_id)
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    user["id"] = str(user["_id"])
    return user