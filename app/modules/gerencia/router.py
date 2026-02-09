from datetime import datetime,timedelta
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

@router.get("/test-total-valorizado")
def test_total_valorizado(
    nombre_cliente: Optional[str] = Query(None),
    fecha_inicio: Optional[str] = Query(None, description="Formato YYYY-MM-DD"),
    fecha_fin: Optional[str] = Query(None, description="Formato YYYY-MM-DD"),
    mes: Optional[int] = Query(None, ge=1, le=12),
    anio: Optional[int] = Query(None, ge=2000),
    gerencia_service: GerenciaService = Depends(get_gerencia_service)
):
    """
    Endpoint de prueba para verificar el cálculo del total valorizado.
    """
    try:
        f_inicio_dt = None
        f_fin_dt = None

        # 1. Prioridad: Si hay mes y año, calculamos el rango
        if mes and anio:
            # Restamos 5 horas al inicio para atrapar lo que se registró al final del mes anterior en UTC
            f_inicio_dt = datetime(anio, mes, 1) - timedelta(hours=5)
            
            # Sumamos 5 horas al final
            if mes == 12:
                f_fin_dt = datetime(anio + 1, 1, 1) + timedelta(hours=5)
            else:
                f_fin_dt = datetime(anio, mes + 1, 1) + timedelta(hours=5)
        
        # 2. Si no hubo mes/año, intentamos con las fechas manuales
        else:
            if fecha_inicio:
                f_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            if fecha_fin:
                f_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
                f_fin_dt = datetime.combine(f_fin_dt.date(), datetime.max.time())

        # 3. Llamada directa a tu función
        resultado = gerencia_service.get_total_valorizado(
            nombre_cliente=nombre_cliente,
            fecha_inicio=f_inicio_dt,
            fecha_fin=f_fin_dt
        )

        return {
            "status": "success",
            "filtros_aplicados": {
                "cliente": nombre_cliente,
                "rango_calculado": {
                    "desde": f_inicio_dt.isoformat() if f_inicio_dt else None,
                    "hasta": f_fin_dt.isoformat() if f_fin_dt else None
                }
            },
            "data": resultado
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Error en formato de datos: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

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

@router.get("/resumen-financiero-por-cliente")
def obtener_resumen_financiero_por_cliente(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),
    nombre_cliente: Optional[str] = Query(None, description="Nombre del cliente (case-insensitive, opcional)"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD)"),
    mes: Optional[int] = Query(None, ge=1, le=12, description="Número de mes (1-12)"),
    anio: Optional[int] = Query(None, ge=2000, description="Año (ej. 2026)")
):
    """
    Obtiene un ranking de ventas agrupado por cliente. 
    Puede filtrar por nombre, rango exacto de fechas, o por un mes/año específico.
    """
    try:
        fecha_inicio_dt = None
        fecha_fin_dt = None
        
        # 1. Validación de formato de fechas tradicionales
        if fecha_inicio:
            try:
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de fecha_inicio inválido. Use YYYY-MM-DD")
        
        if fecha_fin:
            try:
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
                fecha_fin_dt = datetime.combine(fecha_fin_dt.date(), datetime.max.time())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de fecha_fin inválido. Use YYYY-MM-DD")
        
        # 2. Validación cruzada de fechas
        if fecha_inicio_dt and fecha_fin_dt and fecha_inicio_dt > fecha_fin_dt:
            raise HTTPException(status_code=400, detail="La fecha de inicio no puede ser mayor a la fecha de fin")
        
        # 3. Validación de lógica mes/año (si envías uno, deberías enviar el otro)
        if (mes and not anio) or (anio and not mes):
            raise HTTPException(status_code=400, detail="Para filtrar por mes, debes proporcionar tanto 'mes' como 'anio'")

        # Llamar al servicio con los nuevos parámetros
        resultado = gerencia_service.get_resumen_financiero_cliente(
            nombre_cliente=nombre_cliente,
            fecha_inicio=fecha_inicio_dt,
            fecha_fin=fecha_fin_dt,
            mes=mes,
            anio=anio
        )
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener resumen por cliente: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/get_kpis_financieros_especificos")
def get_kpis_financieros_especificos(
    gerencia_service: GerenciaService = Depends(get_gerencia_service),
nombre_cliente: Optional[str] = None,
        fecha_inicio: Optional[datetime] = None,
        fecha_fin: Optional[datetime] = None,
        mes: Optional[int] = None,
        anio: Optional[int] = None,
):

    try:
        resultado = gerencia_service.get_kpis_financieros_especificos(
            nombre_cliente=nombre_cliente,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            mes=mes,
            anio=anio
        )
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener KPIs completos: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno del servidor")