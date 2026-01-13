from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from typing import List, Optional
from app.core.database import get_database
from app.modules.dataservice.services.proveedor_service import ProveedorService 
from app.modules.dataservice.schemas.proveedor_schema import (
    ProveedorCreate, ProveedorUpdate, ProveedorResponse, 
    ProveedorFilter, ExcelImportResponseProveedor, PaginatedResponse
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/proveedores", tags=["Proveedores"])

@router.post("/", response_model=ProveedorResponse)
def crear_proveedor(proveedor: ProveedorCreate):
    """
    Crear un nuevo proveedor
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        created_proveedor = proveedor_service.create_proveedor(proveedor.model_dump())
        return created_proveedor
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear proveedor: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=PaginatedResponse[ProveedorResponse], name="listar_proveedores_paginados")
def listar_proveedores(
    # Parámetros de paginación
    page: int = Query(default=1, ge=1, description="Número de página"),
    page_size: int = Query(default=10, ge=1, le=100, description="Elementos por página"),
    # Parámetros de filtrado
    codigo_proveedor: Optional[str] = Query(None, description="Filtrar por código de proveedor"),
    tipo_documento: Optional[str] = Query(None, description="Filtrar por tipo de documento"),
    numero_documento: Optional[str] = Query(None, description="Filtrar por número de documento"),
    razon_social: Optional[str] = Query(None, description="Filtrar por razón social"),
    rubro_proveedor: Optional[str] = Query(None, description="Filtrar por rubro del proveedor"),
    contacto_principal: Optional[str] = Query(None, description="Filtrar por contacto principal"),
    telefono: Optional[str] = Query(None, description="Filtrar por teléfono"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    servicio: Optional[str] = Query(None, description="Filtrar por servicio específico")
):
    """
    Obtener todos los proveedores con filtros opcionales y paginación
    
    - **page**: Número de página (default: 1)
    - **page_size**: Cantidad de elementos por página (default: 10, max: 100)
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        filter_params = ProveedorFilter(
            codigo_proveedor=codigo_proveedor,
            tipo_documento=tipo_documento,
            numero_documento=numero_documento,
            razon_social=razon_social,
            rubro_proveedor=rubro_proveedor,
            contacto_principal=contacto_principal,
            telefono=telefono,
            estado=estado,
            servicio=servicio
        )
        
        result = proveedor_service.get_all_proveedores(filter_params, page, page_size)
        return result
        
    except Exception as e:
        logger.error(f"Error al listar proveedores: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{proveedor_id}", response_model=ProveedorResponse)
def obtener_proveedor(proveedor_id: str):
    """
    Obtener un proveedor por ID
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        proveedor = proveedor_service.get_proveedor_by_id(proveedor_id)
        if not proveedor:
            raise HTTPException(status_code=404, detail="Proveedor no encontrado")
        
        return proveedor
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener proveedor: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{codigo_proveedor}", response_model=ProveedorResponse)
def obtener_proveedor_por_codigo(codigo_proveedor: str):
    """
    Obtener un proveedor por código
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        proveedor = proveedor_service.get_proveedor_by_codigo(codigo_proveedor)
        if not proveedor:
            raise HTTPException(status_code=404, detail="Proveedor no encontrado")
        
        return proveedor
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener proveedor por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/documento/{tipo_documento}/{numero_documento}", response_model=ProveedorResponse)
def obtener_proveedor_por_documento(tipo_documento: str, numero_documento: str):
    """
    Obtener un proveedor por tipo y número de documento
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        proveedor = proveedor_service.get_proveedor_by_documento(tipo_documento, numero_documento)
        if not proveedor:
            raise HTTPException(status_code=404, detail="Proveedor no encontrado")
        
        return proveedor
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener proveedor por documento: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{proveedor_id}", response_model=ProveedorResponse)
def actualizar_proveedor(proveedor_id: str, proveedor_update: ProveedorUpdate):
    """
    Actualizar un proveedor existente
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        proveedor = proveedor_service.update_proveedor(proveedor_id, proveedor_update.model_dump(exclude_unset=True))
        if not proveedor:
            raise HTTPException(status_code=404, detail="Proveedor no encontrado")
        
        return proveedor
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar proveedor: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{proveedor_id}")
def eliminar_proveedor(proveedor_id: str):
    """
    Eliminar un proveedor
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        success = proveedor_service.delete_proveedor(proveedor_id)
        if not success:
            raise HTTPException(status_code=404, detail="Proveedor no encontrado")
        
        return {"message": "Proveedor eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar proveedor: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{proveedor_id}/export/excel")
def exportar_proveedor_excel(proveedor_id: str):
    """
    Exportar un proveedor específico a Excel
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        proveedor = proveedor_service.get_proveedor_by_id(proveedor_id)
        if not proveedor:
            raise HTTPException(status_code=404, detail="Proveedor no encontrado")
        
        filter_params = ProveedorFilter(codigo_proveedor=proveedor["codigo_proveedor"])
        excel_file = proveedor_service.export_to_excel(filter_params)
        
        return {
            "filename": f"proveedor_{proveedor['codigo_proveedor']}.xlsx",
            "content": excel_file.getvalue()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al exportar proveedor a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel", name="exportar_proveedores_excel")
async def exportar_proveedores_excel(
    codigo_proveedor: Optional[str] = Query(None, description="Filtrar por código de proveedor"),
    tipo_documento: Optional[str] = Query(None, description="Filtrar por tipo de documento"),
    numero_documento: Optional[str] = Query(None, description="Filtrar por número de documento"),
    razon_social: Optional[str] = Query(None, description="Filtrar por razón social"),
    rubro_proveedor: Optional[str] = Query(None, description="Filtrar por rubro del proveedor"),
    contacto_principal: Optional[str] = Query(None, description="Filtrar por contacto principal"),
    telefono: Optional[str] = Query(None, description="Filtrar por teléfono"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    servicio: Optional[str] = Query(None, description="Filtrar por servicio específico")
):
    """
    Exportar proveedores a Excel con datos actuales
    
    Aplica los mismos filtros disponibles en el listado de proveedores.
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        filter_params = ProveedorFilter(
            codigo_proveedor=codigo_proveedor,
            tipo_documento=tipo_documento,
            numero_documento=numero_documento,
            razon_social=razon_social,
            rubro_proveedor=rubro_proveedor,
            contacto_principal=contacto_principal,
            telefono=telefono,
            estado=estado,
            servicio=servicio
        )
        
        excel_file = proveedor_service.export_to_excel(filter_params)
        
        from datetime import datetime
        filename = f"proveedores_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except Exception as e:
        logger.error(f"Error al exportar proveedores a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al exportar proveedores")

@router.get("/template/excel", name="descargar_plantilla_importacion_proveedores")
async def descargar_plantilla_importacion():
    """
    Descargar plantilla de Excel vacía para importación de proveedores
    
    La plantilla incluye:
    - Columnas con formato correcto
    - Una fila de ejemplo
    - Hoja de instrucciones con descripción de cada campo
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        template_file = proveedor_service.generate_excel_template()
        
        return StreamingResponse(
            template_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=plantilla_importacion_proveedores.xlsx"
            }
        )
        
    except Exception as e:
        logger.error(f"Error al generar plantilla: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al generar plantilla")

@router.post("/import/excel", response_model=ExcelImportResponseProveedor)
async def importar_proveedores_excel(file: UploadFile = File(...)):
    """
    Importar proveedores desde archivo Excel
    
    **Pasos para importar:**
    1. Descargue la plantilla desde `/proveedores/template/excel`
    2. Complete los datos de sus proveedores
    3. Suba el archivo completado aquí
    
    **Campos obligatorios:**
    - Tipo Documento
    - Número Documento
    - Razón Social
    
    **Formatos aceptados:** .xlsx, .xls
    **Tamaño máximo:** 10MB
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
        proveedor_service = ProveedorService(db)
        
        result = proveedor_service.import_from_excel(content)
        
        # Mensaje personalizado según resultados
        if result["has_errors"]:
            if result["created"] > 0 or result["updated"] > 0:
                message = f"Importación completada con advertencias. Procesados: {result['created'] + result['updated']}, Errores: {len(result['errors'])}"
            else:
                message = "Importación completada con errores. Revise el detalle."
        else:
            message = f"Importación exitosa. {result['created']} proveedores creados, {result['updated']} actualizados"
        
        return {
            "message": message,
            "result": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al importar proveedores desde Excel: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error al procesar el archivo: {str(e)}"
        )

@router.get("/stats/estadisticas")
def obtener_estadisticas():
    """
    Obtener estadísticas de proveedores
    """
    try:
        db = get_database()
        proveedor_service = ProveedorService(db)
        
        stats = proveedor_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")