from fastapi import APIRouter, Depends, HTTPException
from typing import List
from app.modules.auth.schemas.permission import PermissionCreate, PermissionResponse, PermissionUpdate
from app.modules.auth.services.permission_service import PermissionService
from app.core.database import get_database
from app.modules.auth.utils.dependencies import require_permission

router = APIRouter(prefix="/permissions", tags=["Permissions"])

@router.post("/", response_model=PermissionResponse)
def create_permission(
    permission: PermissionCreate,
    current_user: dict = Depends(require_permission("permissions", "create"))
):
    db = get_database()
    permission_service = PermissionService(db)
    
    try:
        created_permission = permission_service.create_permission(permission.dict())
        return created_permission
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/", response_model=List[PermissionResponse])
def get_all_permissions(
    current_user: dict = Depends(require_permission("permissions", "read"))
):
    db = get_database()
    permission_service = PermissionService(db)
    
    return permission_service.get_all_permissions()

@router.get("/{permission_id}", response_model=PermissionResponse)
def get_permission(
    permission_id: str,
    current_user: dict = Depends(require_permission("permissions", "read"))
):
    db = get_database()
    permission_service = PermissionService(db)
    
    permission = permission_service.get_permission_by_id(permission_id)
    
    if not permission:
        raise HTTPException(status_code=404, detail="Permiso no encontrado")
    
    permission["id"] = str(permission["_id"])
    return permission

@router.put("/{permission_id}", response_model=PermissionResponse)
def update_permission(
    permission_id: str,
    permission_update: PermissionUpdate,
    current_user: dict = Depends(require_permission("permissions", "update"))
):
    db = get_database()
    permission_service = PermissionService(db)
    
    updated_permission = permission_service.update_permission(
        permission_id,
        permission_update.dict(exclude_unset=True)
    )
    
    if not updated_permission:
        raise HTTPException(status_code=404, detail="Permiso no encontrado")
    
    updated_permission["id"] = str(updated_permission["_id"])
    return updated_permission

@router.delete("/{permission_id}")
def delete_permission(
    permission_id: str,
    current_user: dict = Depends(require_permission("permissions", "delete"))
):
    db = get_database()
    permission_service = PermissionService(db)
    
    deleted = permission_service.delete_permission(permission_id)
    
    if not deleted:
        raise HTTPException(status_code=404, detail="Permiso no encontrado")
    
    return {"message": "Permiso eliminado exitosamente"}