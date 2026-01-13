from fastapi import APIRouter, Depends, HTTPException
from typing import List
from app.modules.auth.schemas.role import RoleCreate, RoleResponse, RoleUpdate
from app.modules.auth.services.role_service import RoleService
from app.core.database import get_database
from app.modules.auth.utils.dependencies import require_permission

router = APIRouter(prefix="/roles", tags=["Roles"])

@router.post("/", response_model=RoleResponse)
def create_role(
    role: RoleCreate,
    current_user: dict = Depends(require_permission("roles", "create"))
):
    db = get_database()
    role_service = RoleService(db)
    
    try:
        created_role = role_service.create_role(role.dict())
        return created_role
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/", response_model=List[RoleResponse])
def get_all_roles(
    current_user: dict = Depends(require_permission("roles", "read"))
):
    db = get_database()
    role_service = RoleService(db)
    
    return role_service.get_all_roles()

@router.get("/{role_id}", response_model=RoleResponse)
def get_role(
    role_id: str,
    current_user: dict = Depends(require_permission("roles", "read"))
):
    db = get_database()
    role_service = RoleService(db)
    
    role = role_service.get_role_by_id(role_id)
    
    if not role:
        raise HTTPException(status_code=404, detail="Rol no encontrado")
    
    role["id"] = str(role["_id"])
    return role

@router.put("/{role_id}", response_model=RoleResponse)
def update_role(
    role_id: str,
    role_update: RoleUpdate,
    current_user: dict = Depends(require_permission("roles", "update"))
):
    db = get_database()
    role_service = RoleService(db)
    
    updated_role = role_service.update_role(
        role_id,
        role_update.dict(exclude_unset=True)
    )
    
    if not updated_role:
        raise HTTPException(status_code=404, detail="Rol no encontrado")
    
    updated_role["id"] = str(updated_role["_id"])
    return updated_role

@router.delete("/{role_id}")
def delete_role(
    role_id: str,
    current_user: dict = Depends(require_permission("roles", "delete"))
):
    db = get_database()
    role_service = RoleService(db)
    
    deleted = role_service.delete_role(role_id)
    
    if not deleted:
        raise HTTPException(status_code=404, detail="Rol no encontrado")
    
    return {"message": "Rol eliminado exitosamente"}