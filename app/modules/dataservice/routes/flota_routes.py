from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from typing import List, Optional
from app.core.database import get_database 
from app.modules.dataservice.services.flota_service import FlotaService 
from app.modules.dataservice.schemas.flota_schema import (
    FlotaCreate, FlotaUpdate, FlotaResponse, 
    FlotaFilter, ExcelImportResponse, PaginationParams, PaginatedResponse
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flota", tags=["Flota"])

@router.post("/", response_model=FlotaResponse)
def crear_flota(flota: FlotaCreate):
    """Crear un nuevo vehículo"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        created_flota = flota_service.create_flota(flota.model_dump())
        return created_flota
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear vehículo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=PaginatedResponse[FlotaResponse])
def listar_flota(
    codigo_flota: Optional[str] = Query(None, description="Filtrar por código de flota"),
    placa: Optional[str] = Query(None, description="Filtrar por placa"),
    marca: Optional[str] = Query(None, description="Filtrar por marca"),
    modelo: Optional[str] = Query(None, description="Filtrar por modelo"),
    anio: Optional[int] = Query(None, description="Filtrar por año"),
    tipo_vehiculo: Optional[str] = Query(None, description="Filtrar por tipo de vehículo"),
    tipo_combustible: Optional[str] = Query(None, description="Filtrar por tipo de combustible"),
    nombre_conductor: Optional[str] = Query(None, description="Filtrar por nombre del conductor"),
    numero_licencia: Optional[str] = Query(None, description="Filtrar por número de licencia"),
    mtc_numero: Optional[str] = Query(None, description="Filtrar por número MTC"),
    activo: Optional[bool] = Query(None, description="Filtrar por estado activo/inactivo"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(10, ge=1, le=100, description="Elementos por página")
):
    """Listar todos los vehículos con paginación y filtros"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        
        filter_params = FlotaFilter(
            codigo_flota=codigo_flota,
            placa=placa,
            marca=marca,
            modelo=modelo,
            anio=anio,
            tipo_vehiculo=tipo_vehiculo,
            tipo_combustible=tipo_combustible,
            nombre_conductor=nombre_conductor,
            numero_licencia=numero_licencia,
            mtc_numero=mtc_numero,
            activo=activo
        )
        
        result = flota_service.get_all_flotas(filter_params, page, page_size)
        return result
    except Exception as e:
        logger.error(f"Error al listar vehículos: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{flota_id}", response_model=FlotaResponse)
def obtener_flota(flota_id: str):
    """Obtener un vehículo por ID"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        flota = flota_service.get_flota_by_id(flota_id)
        if not flota:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado")
        return flota
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener vehículo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{codigo_flota}", response_model=FlotaResponse)
def obtener_flota_por_codigo(codigo_flota: str):
    """Obtener un vehículo por código de flota"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        flota = flota_service.get_flota_by_codigo(codigo_flota)
        if not flota:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado")
        return flota
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener vehículo por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/placa/{placa}", response_model=FlotaResponse)
def obtener_flota_por_placa(placa: str):
    """Obtener un vehículo por placa"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        flota = flota_service.get_flota_by_placa(placa)
        if not flota:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado")
        return flota
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener vehículo por placa: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/licencia/{numero_licencia}", response_model=FlotaResponse)
def obtener_flota_por_licencia(numero_licencia: str):
    """Obtener un vehículo por número de licencia del conductor"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        flota = flota_service.get_flota_by_licencia(numero_licencia)
        if not flota:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado con esta licencia")
        return flota
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener vehículo por licencia: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{flota_id}", response_model=FlotaResponse)
def actualizar_flota(flota_id: str, flota_update: FlotaUpdate):
    """Actualizar un vehículo"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        flota = flota_service.update_flota(flota_id, flota_update.model_dump(exclude_unset=True))
        if not flota:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado")
        return flota
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar vehículo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{flota_id}")
def eliminar_flota(flota_id: str):
    """Eliminar un vehículo"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        success = flota_service.delete_flota(flota_id)
        if not success:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado")
        return {"message": "Vehículo eliminado correctamente"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar vehículo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_flota_excel(
    codigo_flota: Optional[str] = Query(None, description="Filtrar por código de flota"),
    placa: Optional[str] = Query(None, description="Filtrar por placa"),
    marca: Optional[str] = Query(None, description="Filtrar por marca"),
    modelo: Optional[str] = Query(None, description="Filtrar por modelo"),
    anio: Optional[int] = Query(None, description="Filtrar por año"),
    tipo_vehiculo: Optional[str] = Query(None, description="Filtrar por tipo de vehículo"),
    tipo_combustible: Optional[str] = Query(None, description="Filtrar por tipo de combustible"),
    nombre_conductor: Optional[str] = Query(None, description="Filtrar por nombre del conductor"),
    numero_licencia: Optional[str] = Query(None, description="Filtrar por número de licencia"),
    mtc_numero: Optional[str] = Query(None, description="Filtrar por número MTC"),
    activo: Optional[bool] = Query(None, description="Filtrar por estado activo/inactivo")
):
    """Exportar vehículos a Excel con filtros opcionales"""
    try:
        db = get_database()
        flota_service = FlotaService(db)

        filter_params = FlotaFilter(
            codigo_flota=codigo_flota,
            placa=placa,
            marca=marca,
            modelo=modelo,
            anio=anio,
            tipo_vehiculo=tipo_vehiculo,
            tipo_combustible=tipo_combustible,
            nombre_conductor=nombre_conductor,
            numero_licencia=numero_licencia,
            mtc_numero=mtc_numero,
            activo=activo
        )

        excel_file = flota_service.export_to_excel(filter_params)
        excel_file.seek(0)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=flota_export.xlsx"
            }
        )
    except Exception as e:
        logger.error(f"Error al exportar vehículos a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.post("/import/excel", response_model=ExcelImportResponse)
def importar_flota_excel(file: UploadFile = File(...)):
    """Importar vehículos desde archivo Excel"""
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, 
                detail="Formato de archivo no válido. Use .xlsx o .xls"
            )
        
        db = get_database()
        flota_service = FlotaService(db)
        content = file.file.read()
        result = flota_service.import_from_excel(content)
        
        message = f"Importación completada. {result['created']} vehículos creados, {result['updated']} actualizados"
        if result['skipped'] > 0:
            message += f", {result['skipped']} omitidos"
        
        return {
            "message": message,
            "result": result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al importar vehículos desde Excel: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al importar: {str(e)}")

@router.get("/template/excel")
def descargar_plantilla_excel():
    """Descargar plantilla de Excel para importación de vehículos"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        template_file = flota_service.generate_excel_template()
        template_file.seek(0)
        
        return StreamingResponse(
            template_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=plantilla_flota.xlsx"
            }
        )
    except Exception as e:
        logger.error(f"Error al generar plantilla Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/stats/general")
def obtener_estadisticas():
    """Obtener estadísticas generales de la flota"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        stats = flota_service.get_stats()
        return stats
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/stats/alertas")
def obtener_alertas(
    dias_anticipacion: int = Query(30, ge=1, le=365, description="Días de anticipación para alertas")
):
    """Obtener vehículos con documentos por vencer o vencidos"""
    try:
        db = get_database()
        flota_service = FlotaService(db)
        alertas = flota_service.get_flotas_con_documentos_vencidos(dias_anticipacion)
        
        return {
            "dias_anticipacion": dias_anticipacion,
            "alertas": alertas,
            "total_alertas": (
                len(alertas["revision_tecnica"]) + 
                len(alertas["soat"]) + 
                len(alertas["extintor"])
            )
        }
    except Exception as e:
        logger.error(f"Error al obtener alertas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")