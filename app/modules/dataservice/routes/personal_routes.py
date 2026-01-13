from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from typing import List, Optional
from datetime import date
from app.core.database import get_database
from app.modules.dataservice.services.personal_service import PersonalService 
from app.modules.dataservice.schemas.personal_schema import (
    PersonalCreate, PersonalUpdate, PersonalResponse, 
    PersonalFilter, ExcelImportResponse, PersonalPaginatedResponse,
    PersonalStatsResponse, BulkStatusUpdate
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/personal", tags=["Personal"])

@router.post("/", response_model=PersonalResponse)
def crear_personal(personal: PersonalCreate):
    """
    Crear un nuevo registro de personal
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        created_personal = personal_service.create_personal(personal.model_dump())
        return created_personal
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear personal: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=PersonalPaginatedResponse, name="listar_personal_paginado")
def listar_personal(
    # Parámetros de paginación
    page: int = Query(default=1, ge=1, description="Número de página"),
    page_size: int = Query(default=10, ge=1, le=100, description="Elementos por página"),
    sort_by: str = Query(default="fecha_registro", description="Campo por el que ordenar"),
    sort_order: str = Query(default="desc", description="Orden: asc o desc"),
    # Parámetros de filtrado
    dni: Optional[str] = Query(None, min_length=8, max_length=15, description="Filtrar por DNI"),
    nombres_completos: Optional[str] = Query(None, description="Filtrar por nombres completos"),
    tipo: Optional[str] = Query(None, description="Filtrar por tipo de personal"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    licencia_conducir: Optional[str] = Query(None, description="Filtrar por número de licencia"),
    categoria_licencia: Optional[str] = Query(None, description="Filtrar por categoría de licencia"),
    turno: Optional[str] = Query(None, description="Filtrar por turno de trabajo"),
    fecha_ingreso_desde: Optional[date] = Query(None, description="Fecha de ingreso desde"),
    fecha_ingreso_hasta: Optional[date] = Query(None, description="Fecha de ingreso hasta"),
    salario_min: Optional[float] = Query(None, ge=0, description="Salario mínimo"),
    salario_max: Optional[float] = Query(None, ge=0, description="Salario máximo"),
    banco: Optional[str] = Query(None, description="Filtrar por banco"),
    telefono: Optional[str] = Query(None, description="Filtrar por teléfono"),
    email: Optional[str] = Query(None, description="Filtrar por email"),
    contacto_emergencia: Optional[str] = Query(None, description="Filtrar por contacto de emergencia")
):
    """
    Obtener todo el personal con filtros opcionales y paginación
    
    - **page**: Número de página (default: 1)
    - **page_size**: Cantidad de elementos por página (default: 10, max: 100)
    - **sort_by**: Campo por el que ordenar (default: fecha_registro)
    - **sort_order**: Orden ascendente (asc) o descendente (desc) (default: desc)
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        filter_params = PersonalFilter(
            dni=dni,
            nombres_completos=nombres_completos,
            tipo=tipo,
            estado=estado,
            licencia_conducir=licencia_conducir,
            categoria_licencia=categoria_licencia,
            turno=turno,
            fecha_ingreso_desde=fecha_ingreso_desde,
            fecha_ingreso_hasta=fecha_ingreso_hasta,
            salario_min=salario_min,
            salario_max=salario_max,
            banco=banco,
            telefono=telefono,
            email=email,
            contacto_emergencia=contacto_emergencia
        )
        
        result = personal_service.get_all_personal(
            filter_params, 
            page, 
            page_size,
            sort_by,
            sort_order
        )
        return result
        
    except Exception as e:
        logger.error(f"Error al listar personal: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{personal_id}", response_model=PersonalResponse)
def obtener_personal(personal_id: str):
    """
    Obtener un registro de personal por ID
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        personal = personal_service.get_personal_by_id(personal_id)
        if not personal:
            raise HTTPException(status_code=404, detail="Personal no encontrado")
        
        return personal
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener personal: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{codigo_personal}", response_model=PersonalResponse)
def obtener_personal_por_codigo(codigo_personal: str):
    """
    Obtener un registro de personal por código
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        personal = personal_service.get_personal_by_codigo(codigo_personal)
        if not personal:
            raise HTTPException(status_code=404, detail="Personal no encontrado")
        
        return personal
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener personal por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/dni/{dni}", response_model=PersonalResponse)
def obtener_personal_por_dni(dni: str):
    """
    Obtener un registro de personal por DNI
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        personal = personal_service.get_personal_by_dni(dni)
        if not personal:
            raise HTTPException(status_code=404, detail="Personal no encontrado")
        
        return personal
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener personal por DNI: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{personal_id}", response_model=PersonalResponse)
def actualizar_personal(personal_id: str, personal_update: PersonalUpdate):
    """
    Actualizar un registro de personal existente
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        personal = personal_service.update_personal(personal_id, personal_update.model_dump(exclude_unset=True))
        if not personal:
            raise HTTPException(status_code=404, detail="Personal no encontrado")
        
        return personal
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar personal: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{personal_id}")
def eliminar_personal(personal_id: str):
    """
    Eliminar un registro de personal
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        success = personal_service.delete_personal(personal_id)
        if not success:
            raise HTTPException(status_code=404, detail="Personal no encontrado")
        
        return {"message": "Personal eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar personal: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{personal_id}/export/excel")
def exportar_personal_excel(personal_id: str):
    """
    Exportar un registro específico de personal a Excel
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        personal = personal_service.get_personal_by_id(personal_id)
        if not personal:
            raise HTTPException(status_code=404, detail="Personal no encontrado")
        
        filter_params = PersonalFilter(dni=personal["dni"])
        excel_file = personal_service.export_to_excel(filter_params)
        
        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=personal_{personal['dni']}.xlsx"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al exportar personal a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_personal_todo_excel(
    dni: Optional[str] = Query(None, description="Filtrar por DNI"),
    nombres_completos: Optional[str] = Query(None, description="Filtrar por nombres completos"),
    tipo: Optional[str] = Query(None, description="Filtrar por tipo de personal"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    licencia_conducir: Optional[str] = Query(None, description="Filtrar por número de licencia"),
    categoria_licencia: Optional[str] = Query(None, description="Filtrar por categoría de licencia"),
    turno: Optional[str] = Query(None, description="Filtrar por turno de trabajo"),
    fecha_ingreso_desde: Optional[date] = Query(None, description="Fecha de ingreso desde"),
    fecha_ingreso_hasta: Optional[date] = Query(None, description="Fecha de ingreso hasta")
):
    """
    Exportar personal a Excel con filtros opcionales
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)

        filter_params = PersonalFilter(
            dni=dni,
            nombres_completos=nombres_completos,
            tipo=tipo,
            estado=estado,
            licencia_conducir=licencia_conducir,
            categoria_licencia=categoria_licencia,
            turno=turno,
            fecha_ingreso_desde=fecha_ingreso_desde,
            fecha_ingreso_hasta=fecha_ingreso_hasta
        )

        excel_file = personal_service.export_to_excel(filter_params)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=personal.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"Error al exportar personal a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/template/excel", name="descargar_plantilla_importacion_personal")
async def descargar_plantilla_importacion_personal():
    """
    Descargar plantilla de Excel vacía para importación de personal
    
    La plantilla incluye:
    - Columnas con formato correcto
    - Una fila de ejemplo
    - Hoja de instrucciones con descripción de cada campo
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        template_file = personal_service.generate_excel_template()
        
        return StreamingResponse(
            template_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=plantilla_importacion_personal.xlsx"
            }
        )
        
    except Exception as e:
        logger.error(f"Error al generar plantilla: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al generar plantilla")

@router.post("/import/excel", response_model=ExcelImportResponse)
async def importar_personal_excel(file: UploadFile = File(...)):
    """
    Importar personal desde archivo Excel
    
    El archivo debe contener las siguientes columnas:
    - **Obligatorias**: DNI, Nombres Completos, Tipo
    - **Opcionales**: Estado, Fecha Ingreso, Fecha Nacimiento, Teléfono, 
      Email, Dirección, Licencia Conducir, Categoría Licencia, 
      Fecha Venc. Licencia, Turno, Salario, Banco, Número Cuenta, 
      Contacto Emergencia, Teléfono Emergencia, Observaciones
    
    Formatos aceptados: .xlsx, .xls
    
    **Validaciones:**
    - DNI debe contener solo números (8-15 dígitos)
    - Tipo debe ser uno de: Conductor, Auxiliar, Operario, Administrativo, 
      Supervisor, Mecánico, Almacenero
    """
    try:
        # Validar extensión del archivo
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, 
                detail="Formato de archivo no válido. Use archivos .xlsx o .xls"
            )
        
        # Validar tamaño del archivo (10MB máximo)
        content = await file.read()
        if len(content) > 10 * 1024 * 1024:  # 10MB
            raise HTTPException(
                status_code=400,
                detail="El archivo es demasiado grande. Tamaño máximo: 10MB"
            )
        
        db = get_database()
        personal_service = PersonalService(db)
        
        result = personal_service.import_from_excel(content)
        
        # Mensaje personalizado según resultados
        if result["has_errors"]:
            if result["created"] > 0 or result["updated"] > 0:
                message = f"Importación completada con advertencias. Procesados: {result['created'] + result['updated']}, Errores: {len(result['errors'])}"
            else:
                message = "Importación completada con errores. Revise el detalle."
        else:
            message = f"Importación exitosa. {result['created']} registros creados, {result['updated']} actualizados"
        
        return {
            "message": message,
            "result": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al importar personal desde Excel: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error al procesar el archivo: {str(e)}"
        )

@router.get("/stats/estadisticas", response_model=PersonalStatsResponse)
def obtener_estadisticas_personal():
    """
    Obtener estadísticas del personal
    
    Incluye:
    - Total de personal
    - Personal activo/inactivo
    - Distribución por tipo, estado y turno
    - Promedio de salario
    - Licencias por vencer
    - Personal reciente
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        stats = personal_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/licencias/por-vencer")
def obtener_licencias_por_vencer(dias_restantes: int = Query(default=30, ge=1, description="Días restantes para considerar como 'por vencer'")):
    """
    Obtener personal con licencias próximas a vencer
    
    - **dias_restantes**: Días restantes para considerar como 'por vencer' (default: 30)
    """
    try:
        db = get_database()
        personal_service = PersonalService(db)
        
        # Calcular fechas para el filtro
        hoy = date.today()
        fecha_limite = hoy + timedelta(days=dias_restantes)
        
        filter_params = PersonalFilter(
            fecha_venc_licencia_desde=hoy,
            fecha_venc_licencia_hasta=fecha_limite
        )
        
        # Obtener personal sin paginación para este reporte especial
        personal_list = personal_service.get_all_personal_sin_paginacion(filter_params)
        
        # Filtrar solo los que tienen licencia
        personal_con_licencia = [p for p in personal_list if p.get("licencia_conducir")]
        
        return {
            "total": len(personal_con_licencia),
            "dias_restantes": dias_restantes,
            "personal": personal_con_licencia
        }
        
    except Exception as e:
        logger.error(f"Error al obtener licencias por vencer: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
