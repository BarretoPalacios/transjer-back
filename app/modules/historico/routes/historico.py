# routers/historico_router.py
from fastapi import APIRouter, HTTPException, Depends, status, Query
from typing import Optional, List
from datetime import date

from app.modules.historico.schemas.historico import (
    HistoricoResponse,
    HistoricoConServicioResponse,
    HistoricoFilter,
    HistoricoListResponse,
    HistoricoEstadisticas
)
from app.modules.historico.services.historico import HistoricoService
from app.core.database import get_database

router = APIRouter(
    prefix="/historicos",
    tags=["Históricos"]
)


def get_historico_service(db=Depends(get_database)) -> HistoricoService:
    """Dependency para obtener el servicio de históricos"""
    return HistoricoService(db)


# ============ OBTENER TODOS LOS HISTÓRICOS CON FILTROS ============
@router.get(
    "/",
    response_model=HistoricoListResponse,
    summary="Obtener históricos con filtros",
    description="Obtiene lista paginada de históricos con filtros opcionales"
)
async def get_historicos(
    # Filtros de texto exacto
    tipo: Optional[str] = Query(None, description="Filtrar por tipo (completado/cancelado)"),
    periodo: Optional[str] = Query(None, description="Filtrar por periodo (YYYY-MM)"),
    estado_final: Optional[str] = Query(None, description="Filtrar por estado final"),
    servicio_id: Optional[str] = Query(None, description="Filtrar por ID de servicio"),
    
    # Filtros de texto con regex
    codigo_servicio: Optional[str] = Query(None, description="Buscar por código de servicio"),
    usuario: Optional[str] = Query(None, description="Buscar por usuario"),
    
    # Filtros de fecha
    fecha_inicio: Optional[date] = Query(None, description="Fecha de registro desde"),
    fecha_fin: Optional[date] = Query(None, description="Fecha de registro hasta"),
    
    # Filtros de periodo (rango)
    periodo_inicio: Optional[str] = Query(None, description="Periodo desde (YYYY-MM)"),
    periodo_fin: Optional[str] = Query(None, description="Periodo hasta (YYYY-MM)"),
    
    # Paginación
    skip: int = Query(0, ge=0, description="Número de registros a saltar"),
    limit: int = Query(100, ge=1, le=1000, description="Número máximo de registros"),
    
    # Ordenamiento
    sort_by: str = Query("fecha_registro", description="Campo para ordenar"),
    sort_order: int = Query(-1, description="Orden: 1 ascendente, -1 descendente"),
    
    service: HistoricoService = Depends(get_historico_service)
):
    """
    Obtener históricos con filtros opcionales y paginación.
    
    **Filtros disponibles:**
    - Por tipo (completado/cancelado)
    - Por periodo específico o rango de periodos
    - Por código de servicio
    - Por usuario que registró
    - Por rango de fechas
    
    **Ejemplos de uso:**
    - `/historicos?tipo=completado&periodo=2024-12`
    - `/historicos?codigo_servicio=SRV-2024&skip=0&limit=50`
    - `/historicos?fecha_inicio=2024-12-01&fecha_fin=2024-12-31`
    """
    try:
        filter_params = HistoricoFilter(
            tipo=tipo,
            periodo=periodo,
            estado_final=estado_final,
            servicio_id=servicio_id,
            codigo_servicio=codigo_servicio,
            usuario=usuario,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            periodo_inicio=periodo_inicio,
            periodo_fin=periodo_fin,
            skip=skip,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        result = service.get_all_historicos(filter_params)
        return result
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al obtener históricos: {str(e)}"
        )


# ============ OBTENER HISTÓRICO POR ID ============
@router.get(
    "/{historico_id}",
    response_model=HistoricoResponse,
    summary="Obtener histórico por ID",
    description="Obtiene un registro histórico específico por su ID"
)
async def get_historico(
    historico_id: str,
    service: HistoricoService = Depends(get_historico_service)
):
    """
    Obtener un registro histórico específico.
    
    **Parámetros:**
    - `historico_id`: ID del registro histórico
    """
    try:
        historico = service.get_historico_by_id(historico_id)
        
        if not historico:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Histórico no encontrado"
            )
        
        return historico
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al obtener histórico: {str(e)}"
        )


# ============ OBTENER HISTÓRICO CON SERVICIO COMPLETO ============
@router.get(
    "/{historico_id}/servicio",
    response_model=HistoricoConServicioResponse,
    summary="Obtener histórico con servicio completo",
    description="Obtiene un registro histórico junto con todos los datos del servicio relacionado"
)
async def get_historico_con_servicio(
    historico_id: str,
    service: HistoricoService = Depends(get_historico_service)
):
    """
    Obtener histórico con los datos completos del servicio.
    
    **Útil para:**
    - Ver detalles completos del servicio que fue cerrado
    - Auditoría de servicios históricos
    - Reportes detallados
    """
    try:
        historico = service.get_historico_con_servicio(historico_id)
        
        if not historico:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Histórico no encontrado"
            )
        
        return historico
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al obtener histórico con servicio: {str(e)}"
        )


# ============ OBTENER HISTÓRICOS DE UN SERVICIO ============
@router.get(
    "/servicio/{servicio_id}",
    response_model=List[HistoricoResponse],
    summary="Obtener históricos de un servicio",
    description="Obtiene todos los registros históricos de un servicio específico"
)
async def get_historicos_by_servicio(
    servicio_id: str,
    service: HistoricoService = Depends(get_historico_service)
):
    """
    Obtener todos los históricos de un servicio específico.
    
    **Útil para:**
    - Ver el historial completo de cierres de un servicio
    - Auditoría de cambios de estado
    - Tracking de servicios que fueron cerrados múltiples veces
    """
    try:
        historicos = service.get_historicos_by_servicio(servicio_id)
        return historicos
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al obtener históricos del servicio: {str(e)}"
        )


# ============ OBTENER ESTADÍSTICAS ============
@router.get(
    "/estadisticas/resumen",
    response_model=HistoricoEstadisticas,
    summary="Obtener estadísticas de históricos",
    description="Obtiene estadísticas agregadas de los registros históricos"
)
async def get_estadisticas(
    fecha_inicio: Optional[date] = Query(None, description="Fecha desde"),
    fecha_fin: Optional[date] = Query(None, description="Fecha hasta"),
    service: HistoricoService = Depends(get_historico_service)
):
    """
    Obtener estadísticas de los históricos.
    
    **Incluye:**
    - Total de registros (completados y cancelados)
    - Distribución por periodo
    - Distribución por usuario
    
    **Ejemplos:**
    - `/historicos/estadisticas/resumen` (todas las estadísticas)
    - `/historicos/estadisticas/resumen?fecha_inicio=2024-01-01&fecha_fin=2024-12-31`
    """
    try:
        estadisticas = service.get_estadisticas(fecha_inicio, fecha_fin)
        return estadisticas
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al obtener estadísticas: {str(e)}"
        )


# ============ OBTENER HISTÓRICOS POR PERIODO ============
@router.get(
    "/periodo/{periodo}",
    response_model=HistoricoListResponse,
    summary="Obtener históricos por periodo",
    description="Obtiene todos los históricos de un periodo específico (YYYY-MM)"
)
async def get_historicos_by_periodo(
    periodo: str,
    tipo: Optional[str] = Query(None, description="Filtrar por tipo"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: HistoricoService = Depends(get_historico_service)
):
    """
    Obtener históricos de un periodo específico.
    
    **Formato del periodo:** YYYY-MM (ejemplo: 2024-12)
    
    **Ejemplos:**
    - `/historicos/periodo/2024-12`
    - `/historicos/periodo/2024-12?tipo=completado`
    """
    try:
        filter_params = HistoricoFilter(
            periodo=periodo,
            tipo=tipo,
            skip=skip,
            limit=limit
        )
        
        result = service.get_all_historicos(filter_params)
        return result
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al obtener históricos del periodo: {str(e)}"
        )


# ============ OBTENER ÚLTIMOS HISTÓRICOS ============
@router.get(
    "/recientes/ultimos",
    response_model=List[HistoricoResponse],
    summary="Obtener últimos históricos",
    description="Obtiene los históricos más recientes"
)
async def get_ultimos_historicos(
    limit: int = Query(10, ge=1, le=100, description="Número de registros"),
    tipo: Optional[str] = Query(None, description="Filtrar por tipo"),
    service: HistoricoService = Depends(get_historico_service)
):
    """
    Obtener los históricos más recientes.
    
    **Útil para:**
    - Dashboard de actividad reciente
    - Monitor de cierres de servicios
    - Vista rápida de últimas acciones
    """
    try:
        filter_params = HistoricoFilter(
            tipo=tipo,
            skip=0,
            limit=limit,
            sort_by="fecha_registro",
            sort_order=-1
        )
        
        result = service.get_all_historicos(filter_params)
        return result["data"]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al obtener últimos históricos: {str(e)}"
        )


# # ============ ELIMINAR HISTÓRICO (ADMIN) ============
# @router.delete(
#     "/{historico_id}",
#     status_code=status.HTTP_204_NO_CONTENT,
#     summary="Eliminar histórico",
#     description="Elimina un registro histórico (usar con precaución)"
# )
# async def delete_historico(
#     historico_id: str,
#     service: HistoricoService = Depends(get_historico_service)
#     # TODO: Agregar dependencia de autenticación y verificar rol de admin
# ):
#     """
#     Eliminar un registro histórico.
    
#     ⚠️ **ADVERTENCIA:** Esta operación es irreversible.
    
#     Solo debe usarse en casos excepcionales como:
#     - Corrección de errores en registros
#     - Limpieza de datos de prueba
#     - Cumplimiento de políticas de privacidad (GDPR)
#     """
#     try:
#         deleted = service.delete_historico(historico_id)
        
#         if not deleted:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail="Histórico no encontrado"
#             )
        
#         return None
        
#     except ValueError as e:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=str(e)
#         )
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error al eliminar histórico: {str(e)}"
#         )
    
