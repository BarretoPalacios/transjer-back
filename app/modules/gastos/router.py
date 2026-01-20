from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from typing import List, Optional
from app.core.database import get_database
from app.modules.gastos.service import GastoService 
from app.modules.gastos.schema import (
    GastoCreate, GastoUpdate, GastoResponse, 
    GastoFilter, ExcelImportResponse, PaginatedResponse
)
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/gastos", tags=["Gastos"])

@router.post("/", response_model=GastoResponse)
def crear_gasto(gasto: GastoCreate):
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        created_gasto = gasto_service.create_gasto(gasto.model_dump())
        return created_gasto
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear gasto: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=PaginatedResponse[GastoResponse], name="listar_gastos_paginados")
def listar_gastos(
    page: int = Query(default=1, ge=1, description="Número de página"),
    page_size: int = Query(default=10, ge=1, le=100, description="Elementos por página"),
    id_gasto: Optional[str] = Query(None, description="Filtrar por ID de gasto"),
    placa: Optional[str] = Query(None, description="Filtrar por placa"),
    ambito: Optional[str] = Query(None, description="Filtrar por ámbito (local/nacional)"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    tipo_gasto: Optional[str] = Query(None, description="Filtrar por tipo de gasto"),
    fecha_gasto_desde: Optional[datetime] = Query(None, description="Filtrar por fecha desde"),
    fecha_gasto_hasta: Optional[datetime] = Query(None, description="Filtrar por fecha hasta"),
    valor_minimo: Optional[float] = Query(None, description="Filtrar por valor mínimo"),
    valor_maximo: Optional[float] = Query(None, description="Filtrar por valor máximo")
):
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        filter_params = GastoFilter(
            id_gasto=id_gasto,
            placa=placa,
            ambito=ambito,
            estado=estado,
            tipo_gasto=tipo_gasto,
            fecha_gasto_desde=fecha_gasto_desde,
            fecha_gasto_hasta=fecha_gasto_hasta,
            valor_minimo=valor_minimo,
            valor_maximo=valor_maximo
        )
        
        result = gasto_service.get_all_gastos(filter_params, page, page_size)
        return result
        
    except Exception as e:
        logger.error(f"Error al listar gastos: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{gasto_id}", response_model=GastoResponse)
def obtener_gasto(gasto_id: str):
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        gasto = gasto_service.get_gasto_by_id(gasto_id)
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto no encontrado")
        
        return gasto
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener gasto: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{id_gasto}", response_model=GastoResponse)
def obtener_gasto_por_codigo(id_gasto: str):
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        gasto = gasto_service.get_gasto_by_codigo(id_gasto)
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto no encontrado")
        
        return gasto
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener gasto por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/placa/{placa}", response_model=List[GastoResponse])
def obtener_gastos_por_placa(placa: str):
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        gastos = gasto_service.get_gastos_by_placa(placa)
        return gastos
        
    except Exception as e:
        logger.error(f"Error al obtener gastos por placa: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{gasto_id}", response_model=GastoResponse)
def actualizar_gasto(gasto_id: str, gasto_update: GastoUpdate):
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        gasto = gasto_service.update_gasto(gasto_id, gasto_update.model_dump(exclude_unset=True))
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto no encontrado")
        
        return gasto
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar gasto: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{gasto_id}")
def eliminar_gasto(gasto_id: str):
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        success = gasto_service.delete_gasto(gasto_id)
        if not success:
            raise HTTPException(status_code=404, detail="Gasto no encontrado")
        
        return {"message": "Gasto eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar gasto: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{gasto_id}/export/excel")
def exportar_gasto_excel(gasto_id: str):
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        gasto = gasto_service.get_gasto_by_id(gasto_id)
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto no encontrado")
        
        filter_params = GastoFilter(id_gasto=gasto["id_gasto"])
        excel_file = gasto_service.export_to_excel(filter_params)
        
        return {
            "filename": f"gasto_{gasto['id_gasto']}.xlsx",
            "content": excel_file.getvalue()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al exportar gasto a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_gastos_excel(
    id_gasto: Optional[str] = Query(None, description="Filtrar por ID de gasto"),
    placa: Optional[str] = Query(None, description="Filtrar por placa"),
    ambito: Optional[str] = Query(None, description="Filtrar por ámbito"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    tipo_gasto: Optional[str] = Query(None, description="Filtrar por tipo de gasto"),
    fecha_gasto_desde: Optional[datetime] = Query(None, description="Filtrar por fecha desde"),
    fecha_gasto_hasta: Optional[datetime] = Query(None, description="Filtrar por fecha hasta")
):
    try:
        db = get_database()
        gasto_service = GastoService(db)

        filter_params = GastoFilter(
            id_gasto=id_gasto,
            placa=placa,
            ambito=ambito,
            estado=estado,
            tipo_gasto=tipo_gasto,
            fecha_gasto_desde=fecha_gasto_desde,
            fecha_gasto_hasta=fecha_gasto_hasta
        )

        excel_file = gasto_service.export_to_excel(filter_params)
        excel_file.seek(0)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=gastos.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"Error al exportar gastos a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/template/excel", name="descargar_plantilla_importacion_gastos")
async def descargar_plantilla_importacion():
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        template_file = gasto_service.generate_excel_template()
        
        return StreamingResponse(
            template_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=plantilla_importacion_gastos.xlsx"
            }
        )
        
    except Exception as e:
        logger.error(f"Error al generar plantilla: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al generar plantilla")

@router.post("/import/excel", response_model=ExcelImportResponse)
async def importar_gastos_excel(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, 
                detail="Formato de archivo no válido. Use archivos .xlsx o .xls"
            )
        
        content = await file.read()
        if len(content) > 10 * 1024 * 1024:
            raise HTTPException(
                status_code=400,
                detail="El archivo es demasiado grande. Tamaño máximo: 10MB"
            )
        
        db = get_database()
        gasto_service = GastoService(db)
        
        result = gasto_service.import_from_excel(content)
        
        if result["has_errors"]:
            if result["created"] > 0 or result["updated"] > 0:
                message = f"Importación completada con advertencias. Procesados: {result['created'] + result['updated']}, Errores: {len(result['errors'])}"
            else:
                message = "Importación completada con errores. Revise el detalle."
        else:
            message = f"Importación exitosa. {result['created']} gastos creados, {result['updated']} actualizados"
        
        return {
            "message": message,
            "result": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al importar gastos desde Excel: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error al procesar el archivo: {str(e)}"
        )

@router.get("/stats/estadisticas")
def obtener_estadisticas():
    try:
        db = get_database()
        gasto_service = GastoService(db)
        
        stats = gasto_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")