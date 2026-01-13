from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from typing import List, Optional
from app.core.database import get_database
from app.modules.dataservice.services.lugar_service import LugarService 
from app.modules.dataservice.schemas.lugar_schema import (
    LugarCreate, LugarUpdate, LugarResponse, 
    LugarFilter, ExcelImportResponse
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lugares", tags=["Lugares"])

@router.post("/", response_model=LugarResponse)
def crear_lugar(lugar: LugarCreate):
    """
    Crear un nuevo lugar 
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        created_lugar = lugar_service.create_lugar(lugar.model_dump())
        return created_lugar
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear lugar: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=List[LugarResponse], name="listar_lugares")
def listar_lugares(
    codigo_lugar: Optional[str] = Query(None, description="Filtrar por código de lugar"),
    nombre: Optional[str] = Query(None, description="Filtrar por nombre"),
    tipo_lugar: Optional[str] = Query(None, description="Filtrar por tipo de lugar (origen, destino, almacen, taller, oficina)"),
    distrito: Optional[str] = Query(None, description="Filtrar por distrito"),
    provincia: Optional[str] = Query(None, description="Filtrar por provincia"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    es_principal: Optional[bool] = Query(None, description="Filtrar por lugar principal")
):
    """
    Obtener todos los lugares con filtros opcionales
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        filter_params = LugarFilter(
            codigo_lugar=codigo_lugar,
            nombre=nombre,
            tipo_lugar=tipo_lugar,
            distrito=distrito,
            provincia=provincia,
            estado=estado,
            es_principal=es_principal
        )
        
        lugares = lugar_service.get_all_lugares(filter_params)
        return lugares
        
    except Exception as e:
        logger.error(f"Error al listar lugares: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{lugar_id}", response_model=LugarResponse)
def obtener_lugar(lugar_id: str):
    """
    Obtener un lugar por ID
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        lugar = lugar_service.get_lugar_by_id(lugar_id)
        if not lugar:
            raise HTTPException(status_code=404, detail="Lugar no encontrado")
        
        return lugar
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener lugar: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{codigo_lugar}", response_model=LugarResponse)
def obtener_lugar_por_codigo(codigo_lugar: str):
    """
    Obtener un lugar por código
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        lugar = lugar_service.get_lugar_by_codigo(codigo_lugar)
        if not lugar:
            raise HTTPException(status_code=404, detail="Lugar no encontrado")
        
        return lugar
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener lugar por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/tipo/{tipo_lugar}", response_model=List[LugarResponse])
def obtener_lugares_por_tipo(tipo_lugar: str):
    """
    Obtener lugares por tipo
    """
    try:
        # Validar tipo de lugar
        tipos_validos = ["origen", "destino", "almacen", "taller", "oficina"]
        if tipo_lugar not in tipos_validos:
            raise HTTPException(
                status_code=400, 
                detail=f"Tipo de lugar inválido. Debe ser: {', '.join(tipos_validos)}"
            )
        
        db = get_database()
        lugar_service = LugarService(db)
        
        lugares = lugar_service.get_lugares_by_tipo(tipo_lugar)
        return lugares
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener lugares por tipo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/tipo/{tipo_lugar}/principal", response_model=LugarResponse)
def obtener_lugar_principal_por_tipo(tipo_lugar: str):
    """
    Obtener el lugar principal por tipo
    """
    try:
        # Validar tipo de lugar
        tipos_validos = ["origen", "destino", "almacen", "taller", "oficina"]
        if tipo_lugar not in tipos_validos:
            raise HTTPException(
                status_code=400, 
                detail=f"Tipo de lugar inválido. Debe ser: {', '.join(tipos_validos)}"
            )
        
        db = get_database()
        lugar_service = LugarService(db)
        
        lugar = lugar_service.get_lugar_principal_by_tipo(tipo_lugar)
        if not lugar:
            raise HTTPException(
                status_code=404, 
                detail=f"No se encontró un lugar principal para el tipo: {tipo_lugar}"
            )
        
        return lugar
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener lugar principal por tipo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{lugar_id}", response_model=LugarResponse)
def actualizar_lugar(lugar_id: str, lugar_update: LugarUpdate):
    """
    Actualizar un lugar existente
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        lugar = lugar_service.update_lugar(lugar_id, lugar_update.model_dump(exclude_unset=True))
        if not lugar:
            raise HTTPException(status_code=404, detail="Lugar no encontrado")
        
        return lugar
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar lugar: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{lugar_id}")
def eliminar_lugar(lugar_id: str):
    """
    Eliminar un lugar
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        success = lugar_service.delete_lugar(lugar_id)
        if not success:
            raise HTTPException(status_code=404, detail="Lugar no encontrado")
        
        return {"message": "Lugar eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar lugar: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{lugar_id}/export/excel")
def exportar_lugar_excel(lugar_id: str):
    """
    Exportar un lugar específico a Excel
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        lugar = lugar_service.get_lugar_by_id(lugar_id)
        if not lugar:
            raise HTTPException(status_code=404, detail="Lugar no encontrado")
        
        filter_params = LugarFilter(codigo_lugar=lugar["codigo_lugar"])
        excel_file = lugar_service.export_to_excel(filter_params)
        
        return {
            "filename": f"lugar_{lugar['codigo_lugar']}.xlsx",
            "content": excel_file.getvalue()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al exportar lugar a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_lugares_excel(
    codigo_lugar: Optional[str] = Query(None),
    nombre: Optional[str] = Query(None),
    tipo_lugar: Optional[str] = Query(None),
    distrito: Optional[str] = Query(None),
    provincia: Optional[str] = Query(None),
    estado: Optional[str] = Query(None),
    es_principal: Optional[bool] = Query(None)
):
    """
    Exportar lugares a Excel con filtros opcionales
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)

        filter_params = LugarFilter(
            codigo_lugar=codigo_lugar,
            nombre=nombre,
            tipo_lugar=tipo_lugar,
            distrito=distrito,
            provincia=provincia,
            estado=estado,
            es_principal=es_principal
        )

        excel_file = lugar_service.export_to_excel(filter_params)
        excel_file.seek(0)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=lugares.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"Error al exportar lugares a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.post("/import/excel", response_model=ExcelImportResponse)
def importar_lugares_excel(file: UploadFile = File(...)):
    """
    Importar lugares desde archivo Excel
    """
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="Formato de archivo no válido. Use .xlsx o .xls")
        
        db = get_database()
        lugar_service = LugarService(db)
        
        content = file.file.read()
        result = lugar_service.import_from_excel(content)
        
        return {
            "message": "Importación completada",
            "result": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al importar lugares desde Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/stats/estadisticas")
def obtener_estadisticas():
    """
    Obtener estadísticas de lugares
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        stats = lugar_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/departamento/{departamento}", response_model=List[LugarResponse])
def obtener_lugares_por_departamento(departamento: str):
    """
    Obtener lugares por departamento
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        # Usamos el filtro para obtener por departamento
        filter_params = LugarFilter(departamento=departamento)
        lugares = lugar_service.get_all_lugares(filter_params)
        return lugares
        
    except Exception as e:
        logger.error(f"Error al obtener lugares por departamento: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/provincia/{provincia}", response_model=List[LugarResponse])
def obtener_lugares_por_provincia(provincia: str):
    """
    Obtener lugares por provincia
    """
    try:
        db = get_database()
        lugar_service = LugarService(db)
        
        filter_params = LugarFilter(provincia=provincia)
        lugares = lugar_service.get_all_lugares(filter_params)
        return lugares
        
    except Exception as e:
        logger.error(f"Error al obtener lugares por provincia: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")