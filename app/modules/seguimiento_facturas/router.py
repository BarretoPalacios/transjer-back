from datetime import date
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from decimal import Decimal
import logging

from app.core.database import get_database
from app.modules.seguimiento_facturas.service import FacturacionGestionService
from app.modules.seguimiento_facturas.schema import (
    FacturacionGestionCreate,
    FacturacionGestionUpdate,
    FacturacionGestionResponse,
    FacturacionGestionFilter,
    PaginatedResponse,
    EstadoPagoNeto,
    EstadoDetraccion,
    PrioridadPago
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/facturacion-gestion", tags=["Facturación - Gestión"])

@router.post("/", response_model=FacturacionGestionResponse)
def crear_gestion(gestion: FacturacionGestionCreate):
    """
    Crear nueva gestión de facturación
    
    Crea una nueva gestión para el seguimiento de pagos de una factura.
    Validaciones automáticas:
    - Verifica que la factura exista
    - Evita duplicados por código de factura
    - Calcula detracción automática si no se especifica
    - Establece estado de detracción según monto (<400 = No Aplica)
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        created_gestion = gestion_service.create_gestion(gestion.model_dump())
        return created_gestion
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear gestión: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=PaginatedResponse[FacturacionGestionResponse])
def listar_gestiones(
    # Parámetros de paginación
    page: int = Query(default=1, ge=1, description="Número de página"),
    page_size: int = Query(default=10, ge=1, le=100, description="Elementos por página"),
    
    # Filtros básicos
    codigo_factura: Optional[str] = Query(None, description="Código de factura"),
    numero_factura: Optional[str] = Query(None, description="Número de factura"),
    
    # Estados y prioridad
    estado_detraccion: Optional[EstadoDetraccion] = Query(None, description="Estado de detracción"),
    estado_pago_neto: Optional[EstadoPagoNeto] = Query(None, description="Estado de pago neto"),
    prioridad: Optional[PrioridadPago] = Query(None, description="Prioridad"),
    
    # Gestión administrativa
    centro_costo: Optional[str] = Query(None, description="Centro de costo"),
    responsable_gestion: Optional[str] = Query(None, description="Responsable"),
    
    # Fechas - Probable pago
    fecha_probable_inicio: Optional[date] = Query(None, description="Fecha probable pago inicial"),
    fecha_probable_fin: Optional[date] = Query(None, description="Fecha probable pago final"),
    
    # Fechas - Emisión
    fecha_emision_inicio: Optional[date] = Query(None, description="Fecha emisión inicial"),
    fecha_emision_fin: Optional[date] = Query(None, description="Fecha emisión final"),
    
    # Fechas - Vencimiento
    fecha_vencimiento_inicio: Optional[date] = Query(None, description="Fecha vencimiento inicial"),
    fecha_vencimiento_fin: Optional[date] = Query(None, description="Fecha vencimiento final"),
    
    # Fechas - Servicio
    fecha_servicio_inicio: Optional[date] = Query(None, description="Fecha servicio inicial"),
    fecha_servicio_fin: Optional[date] = Query(None, description="Fecha servicio final"),
    
    # Fechas - Pago detracción
    fecha_pago_detraccion_inicio: Optional[date] = Query(None, description="Fecha pago detracción inicial"),
    fecha_pago_detraccion_fin: Optional[date] = Query(None, description="Fecha pago detracción final"),
    
    # Filtros de entidades (snapshots)
    nombre_cliente: Optional[str] = Query(None, description="Cliente"),
    nombre_cuenta: Optional[str] = Query(None, description="Cuenta"),
    nombre_proveedor: Optional[str] = Query(None, description="Proveedor"),
    
    # Filtros de flota y personal
    placa_flota: Optional[str] = Query(None, description="Placa de vehículo"),
    nombre_conductor: Optional[str] = Query(None, description="Conductor"),
    nombre_auxiliar: Optional[str] = Query(None, description="Auxiliar"),
    
    # Filtros de servicio
    tipo_servicio: Optional[str] = Query(None, description="Tipo de servicio"),
    modalidad: Optional[str] = Query(None, description="Modalidad"),
    zona: Optional[str] = Query(None, description="Zona"),
    origen: Optional[str] = Query(None, description="Origen"),
    destino: Optional[str] = Query(None, description="Destino"),
    
    # Filtros de montos
    monto_total_min: Optional[Decimal] = Query(None, description="Monto total mínimo"),
    monto_total_max: Optional[Decimal] = Query(None, description="Monto total máximo"),
    monto_neto_min: Optional[Decimal] = Query(None, description="Monto neto mínimo"),
    monto_neto_max: Optional[Decimal] = Query(None, description="Monto neto máximo"),
    monto_detraccion_min: Optional[Decimal] = Query(None, description="Monto detracción mínimo"),
    monto_detraccion_max: Optional[Decimal] = Query(None, description="Monto detracción máximo"),
    
    # Filtros de saldo
    tiene_saldo_pendiente: Optional[bool] = Query(None, description="Tiene saldo pendiente"),
    saldo_pendiente_min: Optional[Decimal] = Query(None, description="Saldo pendiente mínimo"),
    saldo_pendiente_max: Optional[Decimal] = Query(None, description="Saldo pendiente máximo"),
    
    # Filtros de GIA
    gia_rr: Optional[str] = Query(None, description="GIA RR"),
    gia_rt: Optional[str] = Query(None, description="GIA RT"),
    
    # Búsqueda general
    search: Optional[str] = Query(None, description="Búsqueda general en múltiples campos")
):
    """
    Obtener todas las gestiones con filtros extendidos y paginación
    
    **Filtros disponibles:**
    - Básicos: código_factura, numero_factura, estados, prioridad
    - Fechas: probable_pago, emisión, vencimiento, servicio, pago_detracción
    - Entidades: cliente, proveedor, cuenta, conductor, auxiliar
    - Servicio: tipo, modalidad, zona, origen, destino
    - Montos: rangos de monto_total, monto_neto, detracción, saldo
    - GIA: gia_rr, gia_rt
    - Búsqueda: search (busca en múltiples campos)
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        filter_params = FacturacionGestionFilter(
            codigo_factura=codigo_factura,
            numero_factura=numero_factura,
            estado_detraccion=estado_detraccion,
            estado_pago_neto=estado_pago_neto,
            prioridad=prioridad,
            centro_costo=centro_costo,
            responsable_gestion=responsable_gestion,
            fecha_probable_inicio=fecha_probable_inicio,
            fecha_probable_fin=fecha_probable_fin,
            fecha_emision_inicio=fecha_emision_inicio,
            fecha_emision_fin=fecha_emision_fin,
            fecha_vencimiento_inicio=fecha_vencimiento_inicio,
            fecha_vencimiento_fin=fecha_vencimiento_fin,
            fecha_servicio_inicio=fecha_servicio_inicio,
            fecha_servicio_fin=fecha_servicio_fin,
            fecha_pago_detraccion_inicio=fecha_pago_detraccion_inicio,
            fecha_pago_detraccion_fin=fecha_pago_detraccion_fin,
            nombre_cliente=nombre_cliente,
            nombre_cuenta=nombre_cuenta,
            nombre_proveedor=nombre_proveedor,
            placa_flota=placa_flota,
            nombre_conductor=nombre_conductor,
            nombre_auxiliar=nombre_auxiliar,
            tipo_servicio=tipo_servicio,
            modalidad=modalidad,
            zona=zona,
            origen=origen,
            destino=destino,
            monto_total_min=monto_total_min,
            monto_total_max=monto_total_max,
            monto_neto_min=monto_neto_min,
            monto_neto_max=monto_neto_max,
            monto_detraccion_min=monto_detraccion_min,
            monto_detraccion_max=monto_detraccion_max,
            tiene_saldo_pendiente=tiene_saldo_pendiente,
            saldo_pendiente_min=saldo_pendiente_min,
            saldo_pendiente_max=saldo_pendiente_max,
            gia_rr=gia_rr,
            gia_rt=gia_rt,
            search=search
        )
        
        result = gestion_service.get_all_gestiones(filter_params, page, page_size)
        return result
        
    except Exception as e:
        logger.error(f"Error al listar gestiones: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{gestion_id}", response_model=FacturacionGestionResponse)
def obtener_gestion(gestion_id: str):
    """
    Obtener una gestión por ID
    
    Retorna todos los detalles de la gestión incluyendo:
    - Información de detracción
    - Estado de pagos
    - Datos completos (snapshots de factura, fletes y servicios)
    - Saldo pendiente calculado
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        gestion = gestion_service.get_gestion_by_id(gestion_id)
        if not gestion:
            raise HTTPException(status_code=404, detail="Gestión no encontrada")
        
        return gestion
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener gestión: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/factura/{codigo_factura}", response_model=FacturacionGestionResponse)
def obtener_gestion_por_factura(codigo_factura: str):
    """
    Obtener gestión por código de factura
    
    Útil para buscar la gestión asociada a una factura específica.
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        gestion = gestion_service.get_gestion_by_codigo_factura(codigo_factura)
        if not gestion:
            raise HTTPException(status_code=404, detail="Gestión no encontrada para esta factura")
        
        return gestion
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener gestión por factura: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{gestion_id}", response_model=FacturacionGestionResponse)
def actualizar_gestion(gestion_id: str, gestion_update: FacturacionGestionUpdate):
    """
    Actualizar una gestión existente
    
    Actualiza campos específicos de la gestión.
    Validaciones automáticas:
    - Actualiza estado de pago según monto pagado
    - Actualiza estado de detracción al registrar fecha de pago
    - Actualiza timestamp de última actualización
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        gestion = gestion_service.update_gestion(gestion_id, gestion_update.model_dump(exclude_unset=True))
        if not gestion:
            raise HTTPException(status_code=404, detail="Gestión no encontrada")
        
        return gestion
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar gestión: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{gestion_id}")
def eliminar_gestion(gestion_id: str):
    """
    Eliminar una gestión
    
    Elimina permanentemente la gestión de facturación.
    ⚠️ **Atención**: Esta acción no se puede deshacer.
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        success = gestion_service.delete_gestion(gestion_id)
        if not success:
            raise HTTPException(status_code=404, detail="Gestión no encontrada")
        
        return {"message": "Gestión eliminada correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar gestión: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.post("/{gestion_id}/pago-parcial", response_model=FacturacionGestionResponse)
def registrar_pago_parcial(
    gestion_id: str,
    monto_pago: Decimal = Query(..., description="Monto del pago parcial", gt=0),
    nro_operacion: Optional[str] = Query(None, description="Número de operación bancaria")
):
    """
    Registrar un pago parcial a la factura
    
    Permite registrar abonos parciales a la factura.
    Validaciones:
    - Verifica que el pago no exceda el monto neto
    - Actualiza automáticamente el estado de pago
    - Permite registrar número de operación bancaria
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        gestion = gestion_service.registrar_pago_parcial(gestion_id, monto_pago, nro_operacion)
        return gestion
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al registrar pago parcial: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/dashboard/estadisticas")
def obtener_estadisticas_dashboard():
    """
    Obtener estadísticas para el dashboard
    
    Retorna métricas clave para el seguimiento de facturas:
    - Totales por estado de pago
    - Totales por estado de detracción
    - Distribución por prioridad
    - Montos totales (neto, pagado, detracción, pendiente)
    - Cantidad de gestiones vencidas
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        stats = gestion_service.get_dashboard_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/alertas/vencidas", response_model=List[FacturacionGestionResponse])
def obtener_gestiones_vencidas():
    """
    Obtener gestiones con pagos vencidos
    
    Retorna todas las gestiones donde la fecha probable de pago ya pasó
    y el estado no es "Pagado". Automáticamente actualiza el estado a "Vencido".
    
    **Acción automática**: Cambia el estado a "Vencido" para todas las gestiones encontradas.
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        gestiones = gestion_service.get_gestiones_vencidas()
        return gestiones
        
    except Exception as e:
        logger.error(f"Error al obtener gestiones vencidas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/alertas/por-vencer", response_model=List[FacturacionGestionResponse])
def obtener_gestiones_por_vencer(
    dias: int = Query(default=7, ge=1, le=30, description="Días previos al vencimiento para alertar")
):
    """
    Obtener gestiones que están por vencer
    
    Retorna gestiones que tienen fecha de pago en los próximos X días
    y aún no están pagadas. Útil para alertas preventivas.
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        gestiones = gestion_service.get_gestiones_por_vencer(dias)
        return gestiones
        
    except Exception as e:
        logger.error(f"Error al obtener gestiones por vencer: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_gestiones_excel(
    # Filtros básicos
    codigo_factura: Optional[str] = Query(None),
    numero_factura: Optional[str] = Query(None),
    estado_pago_neto: Optional[EstadoPagoNeto] = Query(None),
    estado_detraccion: Optional[EstadoDetraccion] = Query(None),
    prioridad: Optional[PrioridadPago] = Query(None),
    centro_costo: Optional[str] = Query(None),
    responsable_gestion: Optional[str] = Query(None),
    
    # Fechas
    fecha_probable_inicio: Optional[date] = Query(None),
    fecha_probable_fin: Optional[date] = Query(None),
    fecha_emision_inicio: Optional[date] = Query(None),
    fecha_emision_fin: Optional[date] = Query(None),
    
    # Entidades
    nombre_cliente: Optional[str] = Query(None),
    nombre_proveedor: Optional[str] = Query(None),
    placa_flota: Optional[str] = Query(None),
    
    # Servicio
    tipo_servicio: Optional[str] = Query(None),
    zona: Optional[str] = Query(None),
    
    # Búsqueda general
    search: Optional[str] = Query(None)
):
    """
    Exportar gestiones a Excel con filtros extendidos
    
    Genera un archivo Excel con todas las gestiones que coincidan con los filtros.
    Incluye columnas detalladas con información de snapshots (cliente, proveedor, servicio, etc.).
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        filter_params = FacturacionGestionFilter(
            codigo_factura=codigo_factura,
            numero_factura=numero_factura,
            estado_detraccion=estado_detraccion,
            estado_pago_neto=estado_pago_neto,
            prioridad=prioridad,
            centro_costo=centro_costo,
            responsable_gestion=responsable_gestion,
            fecha_probable_inicio=fecha_probable_inicio,
            fecha_probable_fin=fecha_probable_fin,
            fecha_emision_inicio=fecha_emision_inicio,
            fecha_emision_fin=fecha_emision_fin,
            nombre_cliente=nombre_cliente,
            nombre_proveedor=nombre_proveedor,
            placa_flota=placa_flota,
            tipo_servicio=tipo_servicio,
            zona=zona,
            search=search
        )
        
        excel_file = gestion_service.export_to_excel(filter_params)
        
        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=gestion_facturacion.xlsx"
            }
        )
        
    except Exception as e:
        logger.error(f"Error al exportar gestiones a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/busqueda/avanzada", response_model=PaginatedResponse[FacturacionGestionResponse])
def busqueda_avanzada(
    # Búsqueda por cliente y proveedor
    cliente: Optional[str] = Query(None, description="Buscar por cliente"),
    proveedor: Optional[str] = Query(None, description="Buscar por proveedor"),
    
    # Búsqueda por flota
    placa: Optional[str] = Query(None, description="Buscar por placa"),
    conductor: Optional[str] = Query(None, description="Buscar por conductor"),
    
    # Búsqueda por ubicación
    zona: Optional[str] = Query(None, description="Buscar por zona"),
    origen: Optional[str] = Query(None, description="Buscar por origen"),
    destino: Optional[str] = Query(None, description="Buscar por destino"),
    
    # Búsqueda por rangos de monto
    monto_min: Optional[Decimal] = Query(None, description="Monto mínimo"),
    monto_max: Optional[Decimal] = Query(None, description="Monto máximo"),
    
    # Estado y prioridad
    estado: Optional[EstadoPagoNeto] = Query(None, description="Estado de pago"),
    prioridad: Optional[PrioridadPago] = Query(None, description="Prioridad"),
    
    # Solo con saldo pendiente
    solo_pendientes: Optional[bool] = Query(None, description="Solo facturas con saldo pendiente"),
    
    # Paginación
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100)
):
    """
    Búsqueda avanzada con filtros combinados
    
    Permite realizar búsquedas complejas combinando múltiples criterios:
    - Por cliente, proveedor, conductor
    - Por ubicación (zona, origen, destino)
    - Por rangos de montos
    - Por estado y prioridad
    - Solo facturas con saldo pendiente
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        filter_params = FacturacionGestionFilter(
            nombre_cliente=cliente,
            nombre_proveedor=proveedor,
            placa_flota=placa,
            nombre_conductor=conductor,
            zona=zona,
            origen=origen,
            destino=destino,
            monto_neto_min=monto_min,
            monto_neto_max=monto_max,
            estado_pago_neto=estado,
            prioridad=prioridad,
            tiene_saldo_pendiente=solo_pendientes
        )
        
        result = gestion_service.get_all_gestiones(filter_params, page, page_size)
        return result
        
    except Exception as e:
        logger.error(f"Error en búsqueda avanzada: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/analisis/por-cliente/{cliente}")
def analisis_por_cliente(cliente: str):
    """
    Análisis de gestiones por cliente
    
    Retorna estadísticas y listado de facturas de un cliente específico.
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        filter_params = FacturacionGestionFilter(nombre_cliente=cliente)
        gestiones = gestion_service._get_all_gestiones_sin_paginacion(filter_params)
        
        # Calcular estadísticas
        total_monto = sum(g.get("monto_neto", 0) for g in gestiones)
        total_pagado = sum(g.get("monto_pagado_acumulado", 0) for g in gestiones)
        total_pendiente = sum(g.get("saldo_pendiente", 0) for g in gestiones)
        
        return {
            "cliente": cliente,
            "total_facturas": len(gestiones),
            "monto_total": str(total_monto),
            "monto_pagado": str(total_pagado),
            "saldo_pendiente": str(total_pendiente),
            "facturas": gestiones
        }
        
    except Exception as e:
        logger.error(f"Error en análisis por cliente: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/analisis/por-proveedor/{proveedor}")
def analisis_por_proveedor(proveedor: str):
    """
    Análisis de gestiones por proveedor
    
    Retorna estadísticas y listado de facturas de un proveedor específico.
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        filter_params = FacturacionGestionFilter(nombre_proveedor=proveedor)
        gestiones = gestion_service._get_all_gestiones_sin_paginacion(filter_params)
        
        total_monto = sum(g.get("monto_neto", 0) for g in gestiones)
        total_pagado = sum(g.get("monto_pagado_acumulado", 0) for g in gestiones)
        total_pendiente = sum(g.get("saldo_pendiente", 0) for g in gestiones)
        
        return {
            "proveedor": proveedor,
            "total_facturas": len(gestiones),
            "monto_total": str(total_monto),
            "monto_pagado": str(total_pagado),
            "saldo_pendiente": str(total_pendiente),
            "facturas": gestiones
        }
        
    except Exception as e:
        logger.error(f"Error en análisis por proveedor: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/resumen/pendientes")
def obtener_resumen_pendientes():
    """
    Obtener resumen de facturas pendientes
    
    Retorna información consolidada de facturas no pagadas completamente.
    Incluye totales por prioridad y montos pendientes.
    """
    try:
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        filter_pendientes = FacturacionGestionFilter(
            estado_pago_neto=EstadoPagoNeto.PENDIENTE
        )
        pendientes = gestion_service.get_all_gestiones(filter_pendientes, 1, 1000)
        
        filter_parcial = FacturacionGestionFilter(
            estado_pago_neto=EstadoPagoNeto.PAGADO_PARCIAL
        )
        parciales = gestion_service.get_all_gestiones(filter_parcial, 1, 1000)
        
        total_pendiente = sum(item["saldo_pendiente"] for item in pendientes["items"])
        total_parcial = sum(item["saldo_pendiente"] for item in parciales["items"])
        
        return {
            "total_pendientes": len(pendientes["items"]),
            "total_parciales": len(parciales["items"]),
            "monto_total_pendiente": str(total_pendiente + total_parcial),
            "detalle_pendientes": pendientes["items"],
            "detalle_parciales": parciales["items"]
        }
        
    except Exception as e:
        logger.error(f"Error al obtener resumen pendientes: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/dashboard/analytics/advanced")
def obtener_analytics_avanzadas(timeframe: str = "month"):
    """
    Obtiene analíticas avanzadas con gráficos y KPIs
    
    Parámetros:
    - timeframe: "day", "week", "month", "year" (default: "month")
    """
    try:
        # Validar timeframe
        valid_timeframes = ["day", "week", "month", "year"]
        if timeframe not in valid_timeframes:
            raise HTTPException(
                status_code=400, 
                detail=f"Timeframe inválido. Valores permitidos: {', '.join(valid_timeframes)}"
            )
        
        db = get_database()
        gestion_service = FacturacionGestionService(db)
        
        analytics = gestion_service.get_advanced_analytics(timeframe=timeframe)
        
        if not analytics:
            return {
                "kpis": {},
                "graficos": {},
                "message": "No hay datos disponibles"
            }
        
        return analytics
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener analíticas avanzadas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
