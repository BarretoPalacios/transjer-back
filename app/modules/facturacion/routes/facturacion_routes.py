from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query, Path
from typing import List, Optional, Dict, Any
from datetime import date
from decimal import Decimal
from app.core.database import get_database
from app.modules.facturacion.services.facturacion_service import FacturacionService
from app.modules.facturacion.schemas.facturacion_schema import (
    FacturacionCreate, 
    FacturacionUpdate, 
    FacturacionResponse, 
    FacturacionFilter
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/facturas", tags=["Facturas"])

@router.post("/", response_model=FacturacionResponse) 
def crear_factura(factura: FacturacionCreate):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        created_factura = facturacion_service.create_factura(factura.model_dump())
        return created_factura
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear factura: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=Dict[str, Any], name="listar_facturas")
def listar_facturas(
    numero_factura: Optional[str] = Query(None, description="Filtrar por número de factura"),
    estado: Optional[str] = Query(None, description="Filtrar por estado (Pendiente, Pagada, Vencida, Anulada, Parcial, Borrador)"),
    moneda: Optional[str] = Query(None, description="Filtrar por moneda (PEN, USD, EUR)"),
    es_borrador: Optional[bool] = Query(None, description="Filtrar por borrador"),
    
    periodo: Optional[str] = Query(None, description="Filtrar por período: hoy, semana, mes, año"),
    fecha_emision: Optional[date] = Query(None, description="Filtrar por fecha específica de emisión"),
    fecha_emision_inicio: Optional[date] = Query(None, description="Fecha inicio de emisión"),
    fecha_emision_fin: Optional[date] = Query(None, description="Fecha fin de emisión"),
    
    fecha_vencimiento: Optional[date] = Query(None, description="Filtrar por fecha específica de vencimiento"),
    fecha_vencimiento_inicio: Optional[date] = Query(None, description="Fecha inicio de vencimiento"),
    fecha_vencimiento_fin: Optional[date] = Query(None, description="Fecha fin de vencimiento"),
    
    fecha_pago: Optional[date] = Query(None, description="Filtrar por fecha específica de pago"),
    fecha_pago_inicio: Optional[date] = Query(None, description="Fecha inicio de pago"),
    fecha_pago_fin: Optional[date] = Query(None, description="Fecha fin de pago"),
    
    monto_total_minimo: Optional[Decimal] = Query(None, description="Monto total mínimo"),
    monto_total_maximo: Optional[Decimal] = Query(None, description="Monto total máximo"),
    
    flete_id: Optional[str] = Query(None, description="Filtrar por ID de flete"),

    nombre_cliente: Optional[str] = Query(None, description="Filtrar por nombre de cliente"),
    
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(10, ge=1, le=100, description="Cantidad de registros por página"),
    sort_by: str = Query("fecha_emision", description="Campo para ordenar"),
    sort_order: int = Query(-1, description="Orden: 1 ascendente, -1 descendente")
):
    try:
        if periodo and periodo not in ['hoy', 'semana', 'mes', 'año']:
            raise HTTPException(status_code=400, detail="Período inválido. Use: hoy, semana, mes, año")
        
        estados_validos = ["Pendiente", "Pagada", "Vencida", "Anulada", "Parcial", "Borrador","Emitida"]
        if estado and estado not in estados_validos:
            raise HTTPException(status_code=400, detail=f"Estado inválido. Use: {', '.join(estados_validos)}")
        
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        filter_params = FacturacionFilter(
            numero_factura=numero_factura,
            estado=estado,
            moneda=moneda,
            es_borrador=es_borrador,
            periodo=periodo,
            fecha_emision=fecha_emision,
            fecha_emision_inicio=fecha_emision_inicio,
            fecha_emision_fin=fecha_emision_fin,
            fecha_vencimiento=fecha_vencimiento,
            fecha_vencimiento_inicio=fecha_vencimiento_inicio,
            fecha_vencimiento_fin=fecha_vencimiento_fin,
            fecha_pago=fecha_pago,
            fecha_pago_inicio=fecha_pago_inicio,
            fecha_pago_fin=fecha_pago_fin,
            monto_total_minimo=monto_total_minimo,
            monto_total_maximo=monto_total_maximo,
            flete_id=flete_id,
            nombre_cliente=nombre_cliente
        )
        
        result = facturacion_service.get_all_facturas(
            filter_params=filter_params,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_order=sort_order
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al listar facturas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/periodo/{periodo}", response_model=Dict[str, Any])
def obtener_facturas_por_periodo(
    periodo: str = Path(..., description="Período: hoy, semana, mes, año"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(10, ge=1, le=100, description="Cantidad de registros por página")
):
    try:
        if periodo not in ['hoy', 'semana', 'mes', 'año']:
            raise HTTPException(status_code=400, detail="Período inválido. Use: hoy, semana, mes, año")
        
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        result = facturacion_service.get_facturas_por_periodo(
            periodo=periodo,
            page=page,
            page_size=page_size
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener facturas por período: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/rango-fechas/", response_model=Dict[str, Any])
def obtener_facturas_por_rango_fechas(
    fecha_inicio: date = Query(..., description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: date = Query(..., description="Fecha fin (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(10, ge=1, le=100, description="Cantidad de registros por página")
):
    try:
        if fecha_inicio > fecha_fin:
            raise HTTPException(status_code=400, detail="La fecha de inicio no puede ser mayor a la fecha fin")
        
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        result = facturacion_service.get_facturas_por_fecha_rango(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            page=page,
            page_size=page_size
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener facturas por rango de fechas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/estado/{estado}", response_model=Dict[str, Any])
def obtener_facturas_por_estado(
    estado: str = Path(..., description="Estado de la factura"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(10, ge=1, le=100, description="Cantidad de registros por página")
):
    try:
        estados_validos = ["Pendiente", "Pagada", "Vencida", "Anulada", "Parcial", "Borrador"]
        if estado not in estados_validos:
            raise HTTPException(
                status_code=400, 
                detail=f"Estado inválido. Use: {', '.join(estados_validos)}"
            )
        
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        result = facturacion_service.get_facturas_por_estado(
            estado=estado,
            page=page,
            page_size=page_size
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener facturas por estado: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/vencidas", response_model=Dict[str, Any])
def obtener_facturas_vencidas(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(10, ge=1, le=100, description="Cantidad de registros por página")
):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        result = facturacion_service.get_facturas_vencidas(
            page=page,
            page_size=page_size
        )
        return result
        
    except Exception as e:
        logger.error(f"Error al obtener facturas vencidas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/por-vencer", response_model=Dict[str, Any])
def obtener_facturas_por_vencer(
    dias: int = Query(7, description="Días próximos a vencer (default: 7)"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(10, ge=1, le=100, description="Cantidad de registros por página")
):
    try:
        if dias < 0:
            raise HTTPException(status_code=400, detail="Los días deben ser un número positivo")
        
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        result = facturacion_service.get_facturas_por_vencer(
            dias=dias,
            page=page,
            page_size=page_size
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener facturas por vencer: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/numero/{numero_factura}", response_model=FacturacionResponse)
def obtener_factura_por_numero(
    numero_factura: str = Path(..., description="Número de factura")
):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        factura = facturacion_service.get_factura_by_numero(numero_factura)
        if not factura:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        return factura
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener factura por número: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{factura_id}", response_model=FacturacionResponse)
def obtener_factura(factura_id: str):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        factura = facturacion_service.get_factura_by_id(factura_id)
        if not factura:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        return factura
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener factura: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{factura_id}", response_model=FacturacionResponse)
def actualizar_factura(factura_id: str, factura_update: FacturacionUpdate):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        factura = facturacion_service.update_factura(factura_id, factura_update.model_dump(exclude_unset=True))
        if not factura:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        return factura
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar factura: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.patch("/{factura_id}/marcar-pagada", response_model=FacturacionResponse)
def marcar_factura_como_pagada(
    factura_id: str,
    fecha_pago: Optional[date] = Query(None, description="Fecha de pago (default: hoy)")
):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        factura = facturacion_service.marcar_como_pagada(factura_id, fecha_pago)
        if not factura:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        return factura
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al marcar factura como pagada: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.post("/{factura_id}/emitir", response_model=FacturacionResponse)
def emitir_factura(
    factura_id: str,
    numero_factura: str,
    fecha_emision: Optional[date] = Query(None, description="Fecha de emisión (default: hoy)"),
    fecha_vencimiento: Optional[date] = Query(None, description="Fecha de vencimiento (default: hoy + 30 días)")
):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        factura = facturacion_service.emitir_factura(
            factura_id=factura_id,
            numero_factura=numero_factura,
            fecha_emision=fecha_emision,
            fecha_vencimiento=fecha_vencimiento
        )
        if not factura:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        return factura
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al emitir factura: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{factura_id}")
def eliminar_factura(factura_id: str):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        success = facturacion_service.delete_factura(factura_id)
        if not success:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        return {"message": "Factura eliminada correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar factura: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_facturas_excel(
    numero_factura: Optional[str] = Query(None),
    estado: Optional[str] = Query(None),
    moneda: Optional[str] = Query(None),
    es_borrador: Optional[bool] = Query(None),
    periodo: Optional[str] = Query(None, description="Filtrar por período: hoy, semana, mes, año"),
    fecha_emision_inicio: Optional[date] = Query(None),
    fecha_emision_fin: Optional[date] = Query(None),
    monto_total_minimo: Optional[Decimal] = Query(None),
    monto_total_maximo: Optional[Decimal] = Query(None),
    flete_id: Optional[str] = Query(None)
):
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)

        filter_params = FacturacionFilter(
            numero_factura=numero_factura,
            estado=estado,
            moneda=moneda,
            es_borrador=es_borrador,
            periodo=periodo,
            fecha_emision_inicio=fecha_emision_inicio,
            fecha_emision_fin=fecha_emision_fin,
            monto_total_minimo=monto_total_minimo,
            monto_total_maximo=monto_total_maximo,
            flete_id=flete_id
        )

        excel_file = facturacion_service.export_to_excel(filter_params)
        excel_file.seek(0)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=facturas.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"Error al exportar facturas a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/stats/estadisticas")
def obtener_estadisticas_facturas():
    try:
        db = get_database()
        facturacion_service = FacturacionService(db)
        
        stats = facturacion_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

