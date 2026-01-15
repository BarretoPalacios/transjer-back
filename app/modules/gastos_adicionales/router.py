from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from datetime import datetime
from app.core.database import get_database
from app.modules.gastos_adicionales.service import GastoAdicionalService
from app.modules.gastos_adicionales.schema import (
    GastoAdicionalCreate,
    GastoAdicionalUpdate,
    GastoAdicionalResponse,
    GastoAdicionalFilter,
    PaginatedResponse,
    ResumenGastosFlete
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/gastos-adicionales", tags=["Gastos Adicionales"])

@router.post("/", response_model=GastoAdicionalResponse)
def crear_gasto(gasto: GastoAdicionalCreate):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        created_gasto = gasto_service.create_gasto(gasto.model_dump())
        return created_gasto
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear gasto adicional: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=PaginatedResponse[GastoAdicionalResponse])
def listar_gastos(
    page: int = Query(default=1, ge=1, description="Número de página"),
    page_size: int = Query(default=10, ge=1, le=100, description="Elementos por página"),
    id_flete: Optional[str] = Query(None, description="Filtrar por ID de flete"),
    codigo_gasto: Optional[str] = Query(None, description="Filtrar por código de gasto"),
    tipo_gasto: Optional[str] = Query(None, description="Filtrar por tipo de gasto"),
    se_factura_cliente: Optional[bool] = Query(None, description="Filtrar por si se factura al cliente"),
    estado_facturacion: Optional[str] = Query(None, description="Filtrar por estado de facturación"),
    estado_aprobacion: Optional[str] = Query(None, description="Filtrar por estado de aprobación"),
    usuario_registro: Optional[str] = Query(None, description="Filtrar por usuario que registró"),
    fecha_inicio: Optional[datetime] = Query(None, description="Filtrar por fecha inicio"),
    fecha_fin: Optional[datetime] = Query(None, description="Filtrar por fecha fin")
):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        filter_params = GastoAdicionalFilter(
            id_flete=id_flete,
            codigo_gasto=codigo_gasto,
            tipo_gasto=tipo_gasto,
            se_factura_cliente=se_factura_cliente,
            estado_facturacion=estado_facturacion,
            estado_aprobacion=estado_aprobacion,
            usuario_registro=usuario_registro,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin
        )
        
        result = gasto_service.get_all_gastos(filter_params, page, page_size)
        return result
        
    except Exception as e:
        logger.error(f"Error al listar gastos: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{gasto_id}", response_model=GastoAdicionalResponse)
def obtener_gasto(gasto_id: str):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        gasto = gasto_service.get_gasto_by_id(gasto_id)
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto adicional no encontrado")
        
        return gasto
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener gasto: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{codigo_gasto}", response_model=GastoAdicionalResponse)
def obtener_gasto_por_codigo(codigo_gasto: str):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        gasto = gasto_service.get_gasto_by_codigo(codigo_gasto)
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto adicional no encontrado")
        
        return gasto
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener gasto por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/flete/{id_flete}")
def obtener_gastos_por_flete(id_flete: str):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        resumen = gasto_service.get_gastos_by_flete(id_flete)
        if not resumen:
            raise HTTPException(status_code=404, detail="No se encontraron gastos para este flete")
        
        return resumen
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener gastos por flete: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/flete-code/{id_flete}")
def obtener_gastos_por_flete_code(id_flete: str):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        resumen = gasto_service.get_gastos_by_code_flete(id_flete)
        if not resumen:
            raise HTTPException(status_code=404, detail="No se encontraron gastos para este flete")
        
        return resumen
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener gastos por flete: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{gasto_id}", response_model=GastoAdicionalResponse)
def actualizar_gasto(gasto_id: str, gasto_update: GastoAdicionalUpdate):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        gasto = gasto_service.update_gasto(gasto_id, gasto_update.model_dump(exclude_unset=True))
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto adicional no encontrado")
        
        return gasto
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar gasto: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{gasto_id}")
def eliminar_gasto(gasto_id: str):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        success = gasto_service.delete_gasto(gasto_id)
        if not success:
            raise HTTPException(status_code=404, detail="Gasto adicional no encontrado")
        
        return {"message": "Gasto adicional eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar gasto: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_gastos_excel(
    id_flete: Optional[str] = Query(None, description="Filtrar por ID de flete"),
    codigo_gasto: Optional[str] = Query(None, description="Filtrar por código de gasto"),
    tipo_gasto: Optional[str] = Query(None, description="Filtrar por tipo de gasto"),
    se_factura_cliente: Optional[bool] = Query(None, description="Filtrar por si se factura al cliente"),
    estado_facturacion: Optional[str] = Query(None, description="Filtrar por estado de facturación"),
    estado_aprobacion: Optional[str] = Query(None, description="Filtrar por estado de aprobación"),
    usuario_registro: Optional[str] = Query(None, description="Filtrar por usuario que registró"),
    fecha_inicio: Optional[datetime] = Query(None, description="Filtrar por fecha inicio"),
    fecha_fin: Optional[datetime] = Query(None, description="Filtrar por fecha fin")
):
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)

        filter_params = GastoAdicionalFilter(
            id_flete=id_flete,
            codigo_gasto=codigo_gasto,
            tipo_gasto=tipo_gasto,
            se_factura_cliente=se_factura_cliente,
            estado_facturacion=estado_facturacion,
            estado_aprobacion=estado_aprobacion,
            usuario_registro=usuario_registro,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin
        )

        excel_file = gasto_service.export_to_excel(filter_params)
        excel_file.seek(0)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=gastos_adicionales.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"Error al exportar gastos a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/stats/estadisticas")
def obtener_estadisticas():
    try:
        db = get_database()
        gasto_service = GastoAdicionalService(db)
        
        stats = gasto_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")