from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from datetime import datetime
from app.core.database import get_database
from app.modules.monitoreo.service import MonitoreoGerencia
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/monitoreo", tags=["Monitoreo Gerencia"])


def get_monitoreo_service():
    db = get_database()
    return MonitoreoGerencia(db)


@router.get("/fletes")
def buscar_fletes_avanzado(
    service: MonitoreoGerencia = Depends(get_monitoreo_service),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    
    # Filtros de Flete
    codigo_flete: Optional[str] = Query(None, description="Código del flete"),
    estado_flete: Optional[str] = Query(None, description="Estado: PENDIENTE, VALORIZADO, etc."),
    pertenece_a_factura: Optional[bool] = Query(None, description="Si está facturado o no"),
    codigo_factura: Optional[str] = Query(None, description="Código de factura asociada"),
    monto_min: Optional[float] = Query(None, description="Monto mínimo del flete"),
    monto_max: Optional[float] = Query(None, description="Monto máximo del flete"),
    
    # Filtros del Servicio asociado
    cliente: Optional[str] = Query(None, description="Nombre del cliente o razón social"),
    proveedor: Optional[str] = Query(None, description="Nombre del proveedor o razón social"),
    placa: Optional[str] = Query(None, description="Placa del vehículo"),
    conductor: Optional[str] = Query(None, description="Nombre del conductor"),
    tipo_servicio: Optional[str] = Query(None, description="Tipo: REGULAR, URGENTE, etc."),
    zona: Optional[str] = Query(None, description="Zona: LIMA, CALLAO, etc."),
    estado_servicio: Optional[str] = Query(None, description="Estado del servicio: Completado, Programado, etc."),
    
    # Filtros de Fecha del Servicio
    fecha_servicio_desde: Optional[datetime] = Query(None, description="Fecha de servicio desde (ISO format)"),
    fecha_servicio_hasta: Optional[datetime] = Query(None, description="Fecha de servicio hasta (ISO format)"),
    
    # Filtros de Fecha del Flete
    fecha_creacion_desde: Optional[datetime] = Query(None, description="Fecha de creación del flete desde"),
    fecha_creacion_hasta: Optional[datetime] = Query(None, description="Fecha de creación del flete hasta")
):
    try:
        
        
        result = service.get_fletes_and_metrics(
            # Filtros de Flete
            codigo_flete=codigo_flete,
            estado_flete=estado_flete,
            pertenece_a_factura=pertenece_a_factura,
            codigo_factura=codigo_factura,
            monto_min=monto_min,
            monto_max=monto_max,
            
            # Filtros del Servicio
            cliente=cliente,
            proveedor=proveedor,
            placa=placa,
            conductor=conductor,
            tipo_servicio=tipo_servicio,
            zona=zona,
            estado_servicio=estado_servicio,
            
            # Filtros de Fecha
            fecha_servicio_desde=fecha_servicio_desde,
            fecha_servicio_hasta=fecha_servicio_hasta,
            fecha_creacion_desde=fecha_creacion_desde,
            fecha_creacion_hasta=fecha_creacion_hasta,
            
            # Paginación
            page=page,
            page_size=page_size
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error en búsqueda avanzada de fletes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")


@router.get("/get_metrics_by_client")
def get_metrics_by_client(
    service: MonitoreoGerencia = Depends(get_monitoreo_service),
    month: int = Query(default=1, ge=1),
    year: int = Query(default=2026),
):
    try:
        result = service.get_metrics_by_client(
            month=month,
            year=year
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error en búsqueda avanzada de fletes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")


@router.get("/get_metrics_by_specific_plates")
def get_metrics_by_specific_plates(
    service: MonitoreoGerencia = Depends(get_monitoreo_service),
    month: int = Query(default=1, ge=1),
    year: int = Query(default=2026),
):
    try:
        result = service.get_metrics_by_specific_plates(
            month=month,
            year=year
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error en búsqueda avanzada de fletes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")



@router.get("/get_metrics_by_provider")
def get_metrics_by_provider(
    service: MonitoreoGerencia = Depends(get_monitoreo_service),
    month: int = Query(default=1, ge=1),
    year: int = Query(default=2026),
):
    try:
        result = service.get_metrics_by_provider(
            month=month,
            year=year
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error en búsqueda avanzada de fletes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")