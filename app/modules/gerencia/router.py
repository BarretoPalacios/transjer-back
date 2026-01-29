from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
import logging
from app.core.database import get_database
from app.modules.gerencia.servicio import GerenciaService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/gerencia", tags=["Gerencia"])

def get_gerencia_service(db = Depends(get_database)):
    """Dependency injection para el servicio de gerencia"""
    return GerenciaService(db)

@router.get("/total-valorizado")
def obtener_total_valorizado(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),
    nombre_cliente: Optional[str] = Query(None, description="Nombre del cliente (opcional)"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD) (opcional)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD) (opcional)")
):
    """
    Obtiene el total valorizado de fletes.
    
    Parámetros opcionales:
    - Sin parámetros: Retorna total de TODOS los fletes valorizados
    - Solo cliente: Retorna total para ese cliente (todas las fechas)
    - Solo fechas: Retorna total para TODOS clientes en ese rango de fechas
    - Cliente y fechas: Retorna total para ese cliente en ese rango de fechas
    
    Ejemplos:
    - /gerencia/total-valorizado (todos los fletes valorizados)
    - /gerencia/total-valorizado?nombre_cliente=OECHSLE (todos los de OECHSLE)
    - /gerencia/total-valorizado?fecha_inicio=2026-01-01&fecha_fin=2026-01-31 (todos en enero 2026)
    - /gerencia/total-valorizado?nombre_cliente=OECHSLE&fecha_inicio=2026-01-01&fecha_fin=2026-01-31
    """
    try:
        # Convertir fechas de string a datetime (si se proporcionan)
        fecha_inicio_dt = None
        fecha_fin_dt = None
        
        if fecha_inicio:
            try:
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de fecha_inicio inválido. Use YYYY-MM-DD")
        
        if fecha_fin:
            try:
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
                # Ajustar fecha_fin al final del día
                fecha_fin_dt = datetime.combine(fecha_fin_dt.date(), datetime.max.time())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de fecha_fin inválido. Use YYYY-MM-DD")
        
        # Validar que si se pasa fecha_inicio o fecha_fin, se pasen ambas
        if (fecha_inicio and not fecha_fin) or (fecha_fin and not fecha_inicio):
            logger.warning("Se proporcionó solo una fecha. Filtrando con la fecha proporcionada.")
            # Esto es permitido: podemos filtrar solo por fecha_inicio o solo por fecha_fin
        
        # Usar el servicio con parámetros opcionales
        resultado = gerencia_service.get_total_valorizado(
            nombre_cliente=nombre_cliente,
            fecha_inicio=fecha_inicio_dt,
            fecha_fin=fecha_fin_dt
        )
        
        # Agregar información de filtros aplicados
        resultado["filtros_aplicados"] = {
            "cliente": nombre_cliente if nombre_cliente else "NINGUNO (TODOS)",
            "fecha_inicio": fecha_inicio if fecha_inicio else "NINGUNA",
            "fecha_fin": fecha_fin if fecha_fin else "NINGUNA"
        }
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener totales de gerencia: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/dashboard/resumen")
def obtener_resumen_dashboard(
    gerencia_service: GerenciaService = Depends(get_gerencia_service)
):
    """
    Endpoint para dashboard que muestra múltiples métricas.
    """
    try:
        # Obtener total general
        total_general = gerencia_service.get_total_valorizado()
        
        # Obtener total del mes actual
        hoy = datetime.now()
        primer_dia_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if hoy.month == 12:
            ultimo_dia_mes = hoy.replace(year=hoy.year + 1, month=1, day=1)
        else:
            ultimo_dia_mes = hoy.replace(month=hoy.month + 1, day=1)
        
        ultimo_dia_mes = ultimo_dia_mes.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        total_mes_actual = gerencia_service.get_total_valorizado(
            fecha_inicio=primer_dia_mes,
            fecha_fin=ultimo_dia_mes
        )
        
        return {
            "resumen": {
                "total_general": total_general,
                "total_mes_actual": total_mes_actual,
                "fecha_consulta": datetime.now().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error al obtener resumen del dashboard: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    
@router.get("/kpis-completos")
def obtener_kpis_completos(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),
    nombre_cliente: Optional[str] = Query(None, description="Nombre del cliente (case-insensitive, opcional)"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD) (opcional)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD) (opcional)"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(100, ge=1, le=500, description="Tamaño de página")
):
    """
    Obtiene todos los KPIs financieros con filtros opcionales.
    
    KPIs incluidos:
    - Total Vendido (fletes valorizados)
    - Total Facturado
    - Total Pagado
    - Total Pendiente
    - Detracciones (totales, pagadas y pendientes)
    
    Filtros:
    - Cliente (case-insensitive): "calera" encontrará "CALERA", "Calera", etc.
    - Rango de fechas (opcional)
    """
    try:
        # Convertir fechas de string a datetime (si se proporcionan)
        fecha_inicio_dt = None
        fecha_fin_dt = None
        
        if fecha_inicio:
            try:
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de fecha_inicio inválido. Use YYYY-MM-DD")
        
        if fecha_fin:
            try:
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
                # Ajustar fecha_fin al final del día
                fecha_fin_dt = datetime.combine(fecha_fin_dt.date(), datetime.max.time())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de fecha_fin inválido. Use YYYY-MM-DD")
        
        # Validar rango de fechas
        if fecha_inicio_dt and fecha_fin_dt and fecha_inicio_dt > fecha_fin_dt:
            raise HTTPException(
                status_code=400, 
                detail="La fecha de inicio no puede ser mayor a la fecha de fin"
            )
        
        # Usar el servicio para obtener todos los KPIs
        resultado = gerencia_service.get_kpis_completos(
            nombre_cliente=nombre_cliente,
            fecha_inicio=fecha_inicio_dt,
            fecha_fin=fecha_fin_dt,
            page=page,
            page_size=page_size
        )
        
        # Agregar información de la consulta
        resultado["consulta"] = {
            "filtros": {
                "cliente": nombre_cliente if nombre_cliente else "NINGUNO",
                "fecha_inicio": fecha_inicio if fecha_inicio else "NINGUNA",
                "fecha_fin": fecha_fin if fecha_fin else "NINGUNA",
                "pagina": page,
                "tamano_pagina": page_size
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener KPIs completos: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    
@router.get("/resumen-por-placa")
def obtener_resumen_por_placa(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),
    placa: Optional[str] = Query(None, description="Placa del vehículo (case-insensitive, opcional)"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio del servicio (YYYY-MM-DD) (opcional)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin del servicio (YYYY-MM-DD) (opcional)")
):

    try:
        # Convertir fechas de string a datetime (si se proporcionan)
        fecha_inicio_dt = None
        fecha_fin_dt = None
        
        if fecha_inicio:
            try:
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Formato de fecha_inicio inválido. Use YYYY-MM-DD"
                )
        
        if fecha_fin:
            try:
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
                # Ajustar fecha_fin al final del día
                fecha_fin_dt = datetime.combine(fecha_fin_dt.date(), datetime.max.time())
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Formato de fecha_fin inválido. Use YYYY-MM-DD"
                )
        
        # Validar rango de fechas
        if fecha_inicio_dt and fecha_fin_dt and fecha_inicio_dt > fecha_fin_dt:
            raise HTTPException(
                status_code=400, 
                detail="La fecha de inicio no puede ser mayor a la fecha de fin"
            )
        
        # Llamar al servicio
        resultado = gerencia_service.get_resumen_por_placa(
            placa=placa,
            fecha_inicio=fecha_inicio_dt,
            fecha_fin=fecha_fin_dt
        )
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener resumen por placa: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor") 


@router.get("/resumen-por-proveedor")
def obtener_resumen_por_proveedor(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),
    nombre_proveedor: Optional[str] = Query(None, description="Nombre del proveedor (case-insensitive, opcional)"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio del servicio (YYYY-MM-DD) (opcional)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin del servicio (YYYY-MM-DD) (opcional)")
):

    try:
        # Convertir fechas de string a datetime (si se proporcionan)
        fecha_inicio_dt = None
        fecha_fin_dt = None
        
        if fecha_inicio:
            try:
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Formato de fecha_inicio inválido. Use YYYY-MM-DD"
                )
        
        if fecha_fin:
            try:
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
                # Ajustar fecha_fin al final del día
                fecha_fin_dt = datetime.combine(fecha_fin_dt.date(), datetime.max.time())
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Formato de fecha_fin inválido. Use YYYY-MM-DD"
                )
        
        # Validar rango de fechas
        if fecha_inicio_dt and fecha_fin_dt and fecha_inicio_dt > fecha_fin_dt:
            raise HTTPException(
                status_code=400, 
                detail="La fecha de inicio no puede ser mayor a la fecha de fin"
            )
        
        # Llamar al servicio
        resultado = gerencia_service.get_resumen_por_proveedor(
            nombre_proveedor=nombre_proveedor,
            fecha_inicio=fecha_inicio_dt,
            fecha_fin=fecha_fin_dt
        )
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener resumen por proveedor: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")
 
@router.get("/resumen-por-cliente")
def obtener_resumen_por_cliente(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),
    nombre_cliente: Optional[str] = Query(None, description="Nombre del cliente (case-insensitive, opcional)"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio del servicio (YYYY-MM-DD) (opcional)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin del servicio (YYYY-MM-DD) (opcional)")
):
    """
    Obtiene un ranking de ventas agrupado por cliente, permitiendo filtrar por nombre y fechas.
    """
    try:
        # Convertir fechas de string a datetime
        fecha_inicio_dt = None
        fecha_fin_dt = None
        
        if fecha_inicio:
            try:
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Formato de fecha_inicio inválido. Use YYYY-MM-DD"
                )
        
        if fecha_fin:
            try:
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
                # Ajustar fecha_fin al final del día (23:59:59)
                fecha_fin_dt = datetime.combine(fecha_fin_dt.date(), datetime.max.time())
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Formato de fecha_fin inválido. Use YYYY-MM-DD"
                )
        
        # Validar rango de fechas
        if fecha_inicio_dt and fecha_fin_dt and fecha_inicio_dt > fecha_fin_dt:
            raise HTTPException(
                status_code=400, 
                detail="La fecha de inicio no puede ser mayor a la fecha de fin"
            )
        
        # Llamar al servicio que creamos anteriormente
        resultado = gerencia_service.get_resumen_por_cliente(
            nombre_cliente=nombre_cliente,
            fecha_inicio=fecha_inicio_dt,
            fecha_fin=fecha_fin_dt
        )
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener resumen por cliente: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/kpis-ranking")
def obtener_kpis_completos(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),
    # nombre_cliente: Optional[str] = Query(None, description="Nombre del cliente (case-insensitive, opcional)"),
    # fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD) (opcional)"),
    # fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD) (opcional)"),
    # page: int = Query(1, ge=1, description="Número de página"),
    # page_size: int = Query(100, ge=1, le=500, description="Tamaño de página")
):

    try:
        # Convertir fechas de string a datetime (si se proporcionan)
        # fecha_inicio_dt = None
        # fecha_fin_dt = None
        
        # if fecha_inicio:
        #     try:
        #         fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
        #     except ValueError:
        #         raise HTTPException(status_code=400, detail="Formato de fecha_inicio inválido. Use YYYY-MM-DD")
        
        # if fecha_fin:
        #     try:
        #         fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
        #         # Ajustar fecha_fin al final del día
        #         fecha_fin_dt = datetime.combine(fecha_fin_dt.date(), datetime.max.time())
        #     except ValueError:
        #         raise HTTPException(status_code=400, detail="Formato de fecha_fin inválido. Use YYYY-MM-DD")
        
        # # Validar rango de fechas
        # if fecha_inicio_dt and fecha_fin_dt and fecha_inicio_dt > fecha_fin_dt:
        #     raise HTTPException(
        #         status_code=400, 
        #         detail="La fecha de inicio no puede ser mayor a la fecha de fin"
        #     )
        
        # Usar el servicio para obtener todos los KPIs
        resultado = gerencia_service.get_analytics_kpis(
            # nombre_cliente=nombre_cliente,
            # fecha_inicio=fecha_inicio_dt,
            # fecha_fin=fecha_fin_dt,
            # page=page,
            # page_size=page_size
        )
        
        # # Agregar información de la consulta
        # resultado["consulta"] = {
        #     "filtros": {
        #         "cliente": nombre_cliente if nombre_cliente else "NINGUNO",
        #         "fecha_inicio": fecha_inicio if fecha_inicio else "NINGUNA",
        #         "fecha_fin": fecha_fin if fecha_fin else "NINGUNA",
        #         "pagina": page,
        #         "tamano_pagina": page_size
        #     },
        #     "timestamp": datetime.now().isoformat()
        # }
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener KPIs completos: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.get("/fletes_indicador")
def obtener_kpis_completos(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),

):

    try:
        resultado = gerencia_service.get_resumen_fletes_completo()

        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener KPIs completos: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")

# get_kpis_financieros_especificos

@router.get("/get_kpis_financieros_especificos")
def get_kpis_financieros_especificos(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),

):

    try:
        resultado = gerencia_service.get_kpis_financieros_especificos()

        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener KPIs completos: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")