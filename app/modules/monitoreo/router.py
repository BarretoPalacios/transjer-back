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
def get_fletes_por_placa(
    placa: Optional[str] = Query(None, description="Placa del vehículo. Si no se pasa, trae todas las placas"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Página"),
    page_size: int = Query(50, ge=1, le=200, description="Registros por página"),
    service: MonitoreoGerencia = Depends(get_monitoreo_service)
):
    """
    Listado paginado de fletes filtrado por placa y/o rango de fechas.
    """
    try:
        return service.facturacion_de_placas(
            placa=placa,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            page=page,
            page_size=page_size
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error en get_fletes_por_placa: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.get("/metricas")
def get_metricas(
    placa: Optional[str] = Query(None, description="Placa del vehículo. Si no se pasa, calcula todas las placas"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD)"),
    service: MonitoreoGerencia = Depends(get_monitoreo_service)
):
    """
    Métricas agregadas: montos, estados, facturación, desglose por mes y tipo servicio.
    """
    try:
        return service.get_metricas_placa(
            placa=placa,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error en get_metricas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.get("/reporte-completo")
def get_reporte_completo(
    placa: Optional[str] = Query(None, description="Placa del vehículo. Si no se pasa, calcula todas las placas"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Página"),
    page_size: int = Query(50, ge=1, le=200, description="Registros por página"),
    service: MonitoreoGerencia = Depends(get_monitoreo_service)
):
    """
    Reporte completo: métricas agregadas + listado paginado de fletes.
    """
    try:
        return service.get_reporte_completo(
            placa=placa,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            page=page,
            page_size=page_size
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error en get_reporte_completo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/get_reporte_pendientes_por_placa")
def get_reporte_pendientes_por_placa(
    placa: Optional[str] = Query(None, description="Placa del vehículo. Si no se pasa, calcula todas las placas"),
    fecha_servicio_desde: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_servicio_hasta: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD)"),
    service: MonitoreoGerencia = Depends(get_monitoreo_service)
):
    """
    Reporte de fletes pendientes por placa.
    """
    try:
        return service.get_reporte_pendientes_por_placa(
            placa=placa,
            fecha_servicio_desde=fecha_servicio_desde,
            fecha_servicio_hasta=fecha_servicio_hasta
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error en get_reporte_pendientes_por_placa: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")