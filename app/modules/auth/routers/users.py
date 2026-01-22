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

@router.get("/me/advance")
async def get_permissions(current_user: dict = Depends(get_current_user)):
    id_user = str(current_user["_id"])
    db = get_database()
    service = UserService(db)
    return service.get_user_permissions_and_roles(id_user)

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

@router.put("/{user_id}", response_model=UserResponse)
def update_any_user(
    user_id: str,
    user_update: UserUpdate,
    # Esta dependencia detendrá la ejecución si el usuario no es admin
    current_admin: dict = Depends(require_role(["administrador"]))
):
    db = get_database()
    user_service = UserService(db)
    
    updated_user = user_service.update_user(
        user_id,
        user_update.dict(exclude_unset=True)
    )
    
    if not updated_user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    updated_user["id"] = str(updated_user["_id"])
    return updated_user

@router.delete("/{user_id}")
def delete_any_user(
    user_id: str,
    current_admin: dict = Depends(require_role(["administrador"]))
):
    db = get_database()
    user_service = UserService(db)
    
    # Opcional: Evitar que el admin se borre a sí mismo
    if str(current_admin["_id"]) == user_id:
        raise HTTPException(
            status_code=400, 
            detail="No puedes eliminar tu propia cuenta de administrador"
        )

    success = user_service.delete_user(user_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    return None

@router.get("/", response_model=List[dict])
async def get_all_users(db = Depends(get_database),current_admin: dict = Depends(require_role(["administrador"]))):
    """
    Obtiene la lista de todos los usuarios con sus roles anidados.
    """
    try:
        user_service = UserService(db)
        users = user_service.get_all_users_with_roles()
        return users
    except Exception as e:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
