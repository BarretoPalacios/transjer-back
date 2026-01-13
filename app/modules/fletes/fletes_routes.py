from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from app.core.database import get_database
from app.modules.fletes.fletes_services import FleteService
from app.modules.fletes.fletes_schemas import (
    FleteCreate, FleteUpdate, FleteResponse, 
    FleteFilter, PaginatedResponse
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fletes", tags=["Fletes"])

@router.post("/", response_model=FleteResponse)
def crear_flete(flete: FleteCreate):
    """
    Crear un nuevo flete de forma manual
    """
    try:
        db = get_database()
        flete_service = FleteService(db)
        
        created_flete = flete_service.create_flete(flete.model_dump())
        return created_flete
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear flete: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=PaginatedResponse[FleteResponse])
def listar_fletes(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    # Filtros específicos de fletes
    codigo_flete: Optional[str] = Query(None),
    servicio_id: Optional[str] = Query(None),
    codigo_servicio: Optional[str] = Query(None),
    estado_flete: Optional[str] = Query(None),
    pertenece_a_factura: Optional[bool] = Query(None, description="Filtrar por fletes facturados o pendientes"),
    codigo_factura: Optional[str] = Query(None),
    monto_min: Optional[float] = Query(None),
    monto_max: Optional[float] = Query(None)
):
    """
    Listar fletes con filtros y paginación.
    Ideal para ver fletes pendientes de facturar usando pertenece_a_factura=false
    """
    try:
        db = get_database()
        flete_service = FleteService(db)
        
        filter_params = FleteFilter(
            codigo_flete=codigo_flete,
            servicio_id=servicio_id,
            codigo_servicio=codigo_servicio,
            estado_flete=estado_flete,
            pertenece_a_factura=pertenece_a_factura,
            codigo_factura=codigo_factura,
            monto_flete_min=monto_min,
            monto_flete_max=monto_max
        )
        
        return flete_service.get_all_fletes(filter_params, page, page_size)
        
    except Exception as e:
        logger.error(f"Error al listar fletes: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{flete_id}", response_model=FleteResponse)
def obtener_flete(flete_id: str):
    """
    Obtener detalle de un flete por ID
    """
    try:
        db = get_database()
        flete_service = FleteService(db)
        
        flete = flete_service.get_flete_by_id(flete_id)
        if not flete:
            raise HTTPException(status_code=404, detail="Flete no encontrado")
        
        return flete
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error al obtener flete: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{codigo_flete}", response_model=FleteResponse)
def obtener_flete_por_codigo(codigo_flete: str):
    """
    Buscar un flete por su código correlativo (Ej: FLT-001)
    """
    try:
        db = get_database()
        flete_service = FleteService(db)
        
        flete = flete_service.get_flete_by_codigo(codigo_flete)
        if not flete:
            raise HTTPException(status_code=404, detail="Flete no encontrado")
        
        return flete
    except Exception as e:
        logger.error(f"Error al obtener flete por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{flete_id}", response_model=FleteResponse)
def actualizar_flete(flete_id: str, flete_update: FleteUpdate):
    """
    Actualizar montos, estados u observaciones de un flete
    """
    try:
        db = get_database()
        flete_service = FleteService(db)
        
        flete = flete_service.update_flete(flete_id, flete_update.model_dump(exclude_unset=True))
        if not flete:
            raise HTTPException(status_code=404, detail="Flete no encontrado")
        
        return flete
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al actualizar flete: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{flete_id}")
def eliminar_flete(flete_id: str):
    """
    Eliminar un registro de flete
    """
    try:
        db = get_database()
        flete_service = FleteService(db)
        
        success = flete_service.delete_flete(flete_id)
        if not success:
            raise HTTPException(status_code=404, detail="Flete no encontrado")
        
        return {"message": "Flete eliminado correctamente"}
    except Exception as e:
        logger.error(f"Error al eliminar flete: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/stats/estadisticas")
def obtener_estadisticas_fletes():
    """
    Obtener resumen financiero de fletes (pendientes, facturados, montos totales)
    """
    try:
        db = get_database()
        flete_service = FleteService(db)
        return flete_service.get_stats()
    except Exception as e:
        logger.error(f"Error al obtener estadísticas de fletes: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/servicio/{servicio_id}", response_model=List[FleteResponse])
def listar_fletes_por_servicio(servicio_id: str):
    """
    Obtener todos los fletes vinculados a un servicio específico
    """
    try:
        db = get_database()
        flete_service = FleteService(db)
        return flete_service.get_fletes_by_servicio(servicio_id)
    except Exception as e:
        logger.error(f"Error al listar fletes por servicio: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")