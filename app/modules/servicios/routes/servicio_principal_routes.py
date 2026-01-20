from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query, status
from typing import List, Optional
from datetime import date
from app.core.database import get_database
from app.modules.servicios.services.servicio_principal_service import ServicioPrincipalService 
from app.modules.servicios.schemas.servicio_principal_schema import (
    ServicioPrincipalCreate, 
    ServicioPrincipalUpdate, 
    ServicioPrincipalResponse,
    ServicioPrincipalConPermisos,
    ServicioPrincipalFilter, 
    ServicioPrincipalExcelImportResponse,
    CambioEstadoRequest,
    CierreServicioRequest,
    HistorialServicioResponse,
    EstadoServicio
)
import logging
from datetime import datetime 

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/servicios-principales", tags=["Servicios Principales"])

 
@router.post("/", response_model=ServicioPrincipalResponse, status_code=status.HTTP_201_CREATED)
def crear_servicio_principal(servicio: ServicioPrincipalCreate):
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        created_servicio = servicio_service.create_servicio(servicio.model_dump())
        return created_servicio
        
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear servicio: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.get("/", response_model=dict)
def listar_servicios_principales(
    codigo_servicio_principal: Optional[str] = Query(None),
    mes: Optional[str] = Query(None),
    tipo_servicio: Optional[str] = Query(None),
    modalidad_servicio: Optional[str] = Query(None),
    zona: Optional[str] = Query(None),
    estado: Optional[EstadoServicio] = Query(None),
    solicitud: Optional[str] = Query(None),
    periodo: Optional[str] = Query(None),
    fecha_servicio: Optional[date] = Query(None),
    fecha_inicio: Optional[date] = Query(None),
    fecha_fin: Optional[date] = Query(None),
    servicio_cerrado: Optional[bool] = Query(None),
    es_editable: Optional[bool] = Query(None),
    pertenece_a_factura: Optional[bool] = Query(None),
    cliente_nombre: Optional[str] = Query(None),
    proveedor_nombre: Optional[str] = Query(None),
    cuenta_nombre: Optional[str] = Query(None),
    flota_placa: Optional[str] = Query(None),
    conductor_nombre: Optional[str] = Query(None),
    origen: Optional[str] = Query(None),
    destino: Optional[str] = Query(None),
    responsable: Optional[str] = Query(None),
    gia_rr: Optional[str] = Query(None),
    gia_rt: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100)
):
    try:
        if periodo and periodo not in ['hoy', 'semana', 'mes', 'año']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Período inválido. Use: hoy, semana, mes, año"
            )
        
        if zona and zona not in ['Lima', 'Provincia', 'Extranjero']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Zona inválida. Use: Lima, Provincia, Extranjero"
            )
        
        if fecha_inicio and fecha_fin and fecha_inicio > fecha_fin:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="La fecha de inicio no puede ser mayor a la fecha fin"
            )
        
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        filter_params = ServicioPrincipalFilter(
            codigo_servicio_principal=codigo_servicio_principal,
            mes=mes,
            tipo_servicio=tipo_servicio,
            modalidad_servicio=modalidad_servicio,
            zona=zona,
            estado=estado,
            solicitud=solicitud,
            periodo=periodo,
            fecha_servicio=fecha_servicio,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            servicio_cerrado=servicio_cerrado,
            es_editable=es_editable,
            pertenece_a_factura=pertenece_a_factura,
            cliente_nombre=cliente_nombre,
            proveedor_nombre=proveedor_nombre,
            cuenta_nombre=cuenta_nombre,
            flota_placa=flota_placa,
            conductor_nombre=conductor_nombre,
            origen=origen,
            destino=destino,
            responsable=responsable,
            gia_rr=gia_rr,
            gia_rt=gia_rt
        )
        
        result = servicio_service.get_all_servicios(filter_params, page, page_size)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al listar servicios: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.get("/{servicio_id}", response_model=ServicioPrincipalResponse)
def obtener_servicio_principal(servicio_id: str):
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        servicio = servicio_service.get_servicio_by_id(servicio_id)
        if not servicio:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Servicio no encontrado")
        
        return servicio
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener servicio: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")

@router.get("/codigo/{codigo_servicio}", response_model=ServicioPrincipalResponse)
def obtener_servicio_principal_por_codigo(codigo_servicio: str):
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        servicio = servicio_service.get_servicio_by_codigo_principal(codigo_servicio)
        
        if not servicio:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="Servicio no encontrado con el código proporcionado"
            )
        
        return servicio
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener servicio: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Error interno del servidor"
        )

@router.put("/{servicio_id}", response_model=ServicioPrincipalResponse)
def actualizar_servicio_principal(servicio_id: str, servicio_update: ServicioPrincipalUpdate):
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        servicio = servicio_service.update_servicio(
            servicio_id, 
            servicio_update.model_dump(exclude_unset=True)
        )
        
        if not servicio:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Servicio no encontrado")
        
        return servicio
        
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar servicio: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.patch("/{servicio_id}/cambiar-estado")
def cambiar_estado_servicio(servicio_id: str, cambio: CambioEstadoRequest):
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        resultado = servicio_service.cambiar_estado_servicio(servicio_id, cambio)
        
        return {
            "message": f"Estado cambiado exitosamente a {cambio.nuevo_estado}",
            "resultado": resultado,
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Cambio de estado no permitido", "mensaje": str(e)}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al cambiar estado: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.get("/{servicio_id}/historial", response_model=HistorialServicioResponse)
def obtener_historial_servicio(servicio_id: str):
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        historial = servicio_service.get_historial_servicio(servicio_id)
        return historial
        
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Error al obtener historial: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.get("/{servicio_id}/permisos")
def verificar_permisos_servicio(servicio_id: str):
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        permisos = servicio_service.verificar_permisos_servicio(servicio_id)
        return permisos
        
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Error al verificar permisos: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.post("/{servicio_id}/cerrar")
def cerrar_servicio(servicio_id: str, cierre: CierreServicioRequest):
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        resultado = servicio_service.cerrar_servicio(servicio_id, cierre)
        return resultado
        
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error al cerrar servicio: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.get("/export/excel")
def exportar_servicios_principales_excel(
    mes: Optional[str] = Query(None),
    tipo_servicio: Optional[str] = Query(None),
    modalidad_servicio: Optional[str] = Query(None),
    zona: Optional[str] = Query(None),
    estado: Optional[EstadoServicio] = Query(None),
    solicitud: Optional[str] = Query(None),
    periodo: Optional[str] = Query(None),
    fecha_servicio: Optional[date] = Query(None),
    fecha_inicio: Optional[date] = Query(None),
    fecha_fin: Optional[date] = Query(None),
    servicio_cerrado: Optional[bool] = Query(None),
    pertenece_a_factura: Optional[bool] = Query(None),
    cliente_nombre: Optional[str] = Query(None),
    proveedor_nombre: Optional[str] = Query(None),
    cuenta_nombre: Optional[str] = Query(None),
    flota_placa: Optional[str] = Query(None),
    conductor_nombre: Optional[str] = Query(None),
    origen: Optional[str] = Query(None),
    destino: Optional[str] = Query(None),
    responsable: Optional[str] = Query(None),
    gia_rr: Optional[str] = Query(None),
    gia_rt: Optional[str] = Query(None)
):
    try:
        if periodo and periodo not in ['hoy', 'semana', 'mes', 'año']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Período inválido. Use: hoy, semana, mes, año"
            )
        
        if fecha_inicio and fecha_fin and fecha_inicio > fecha_fin:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="La fecha de inicio no puede ser mayor a la fecha fin"
            )
        
        db = get_database()
        servicio_service = ServicioPrincipalService(db)

        filter_params = ServicioPrincipalFilter(
            mes=mes,
            tipo_servicio=tipo_servicio,
            modalidad_servicio=modalidad_servicio,
            zona=zona,
            estado=estado,
            solicitud=solicitud,
            periodo=periodo,
            fecha_servicio=fecha_servicio,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            servicio_cerrado=servicio_cerrado,
            pertenece_a_factura=pertenece_a_factura,
            cliente_nombre=cliente_nombre,
            proveedor_nombre=proveedor_nombre,
            cuenta_nombre=cuenta_nombre,
            flota_placa=flota_placa,
            conductor_nombre=conductor_nombre,
            origen=origen,
            destino=destino,
            responsable=responsable,
            gia_rr=gia_rr,
            gia_rt=gia_rt
        )

        excel_file = servicio_service.export_to_excel(filter_params)
        excel_file.seek(0)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=servicios_principales.xlsx"}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al exportar servicios a Excel: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.post("/import/excel", response_model=ServicioPrincipalExcelImportResponse)
def importar_servicios_principales_excel(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Formato de archivo no válido. Use archivos .xlsx o .xls"
            )
        
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        content = file.file.read()
        result = servicio_service.import_from_excel(content)
        
        status_message = "Importación completada exitosamente"
        if result["has_errors"]:
            status_message = f"Importación completada con {len(result['errors'])} errores"
        
        return {"message": status_message, "result": result}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al importar servicios desde Excel: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error al procesar el archivo: {str(e)}")


@router.get("/stats/estadisticas")
def obtener_estadisticas_servicios_principales():
    try:
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        stats = servicio_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor")


@router.post("/cargar-excel-historico")
async def cargar_excel_historico(
    archivo: UploadFile = File(..., description="Archivo Excel con servicios históricos")
):
    """
    Carga Excel histórico - VERSIÓN CORREGIDA
    """
    try:
        # Validar archivo
        if not archivo.filename.lower().endswith(('.xlsx', '.xls', '.xlsm')):
            raise HTTPException(
                status_code=400,
                detail="Formato no soportado. Use .xlsx, .xls o .xlsm"
            )
        
        # Leer contenido
        contenido = await archivo.read()
        if len(contenido) == 0:
            raise HTTPException(status_code=400, detail="Archivo vacío")
        
        logger.info(f"📤 Recibido archivo: {archivo.filename} ({len(contenido)} bytes)")
        
        # Procesar
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        # Primero debug
        debug_info = servicio_service.debug_excel(contenido)
        logger.info(f"🔍 Debug info: {debug_info.get('success', False)}")
        
        # Luego importar
        resultado = servicio_service.importar_excel_servicios_historicos(contenido)
        
        # Preparar respuesta - AQUÍ ESTABA EL ERROR
        respuesta = {
            "success": True,
            "archivo": archivo.filename,
            "tamaño_bytes": len(contenido),
            "procesado_en": datetime.now().isoformat(),  # ← NECESITA datetime IMPORTADO
            "resultado": resultado,
            "debug_info": debug_info if debug_info.get('success') else None
        }
        
        logger.info(f"✅ Procesado: {resultado.get('servicios_creados', 0)} servicios creados")
        
        return respuesta
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error en endpoint: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error interno: {str(e)[:200]}"
        )

@router.post("/debug-excel")
async def debug_excel_endpoint(
    archivo: UploadFile = File(...)
):
    """
    Solo analiza el Excel sin insertar
    """
    try:
        contenido = await archivo.read()
        
        db = get_database()
        servicio_service = ServicioPrincipalService(db)
        
        resultado = servicio_service.debug_excel(contenido)
        
        return {
            "success": True,
            "archivo": archivo.filename,
            "debug_info": resultado
        }
        
    except Exception as e:
        logger.error(f"Error en debug: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))