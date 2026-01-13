from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from typing import List, Optional
from app.core.database import get_database
from app.modules.dataservice.services.cuenta_service import CuentaService 
from app.modules.dataservice.schemas.cuenta_schema import (
    CuentaCreate, CuentaUpdate, CuentaResponse, 
    CuentaFilter, ExcelImportResponse
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cuentas", tags=["Cuentas"])

@router.post("/", response_model=CuentaResponse)
def crear_cuenta(cuenta: CuentaCreate):
    """
    Crear una nueva cuenta
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        created_cuenta = cuenta_service.create_cuenta(cuenta.model_dump())
        return created_cuenta
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear cuenta: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=List[CuentaResponse], name="listar_cuentas_por_id")
def listar_cuentas(
    codigo_cuenta: Optional[str] = Query(None, description="Filtrar por código de cuenta"),
    codigo_cliente: Optional[str] = Query(None, description="Filtrar por código de cliente"),
    nombre: Optional[str] = Query(None, description="Filtrar por nombre"),
    ruc: Optional[str] = Query(None, description="Filtrar por RUC"),
    contacto: Optional[str] = Query(None, description="Filtrar por contacto"),
    tipo_cliente: Optional[str] = Query(None, description="Filtrar por tipo de cliente"),
    estado: Optional[str] = Query(None, description="Filtrar por estado")
):
    """
    Obtener todas las cuentas con filtros opcionales
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        filter_params = CuentaFilter(
            codigo_cuenta=codigo_cuenta,
            codigo_cliente=codigo_cliente,
            nombre=nombre,
            ruc=ruc,
            contacto=contacto,
            tipo_cliente=tipo_cliente,
            estado=estado
        )
        
        cuentas = cuenta_service.get_all_cuentas(filter_params)
        return cuentas
        
    except Exception as e:
        logger.error(f"Error al listar cuentas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{cuenta_id}", response_model=CuentaResponse)
def obtener_cuenta(cuenta_id: str):
    """
    Obtener una cuenta por ID
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        cuenta = cuenta_service.get_cuenta_by_id(cuenta_id)
        if not cuenta:
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        
        return cuenta
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener cuenta: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{codigo_cuenta}", response_model=CuentaResponse)
def obtener_cuenta_por_codigo(codigo_cuenta: str):
    """
    Obtener una cuenta por código
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        cuenta = cuenta_service.get_cuenta_by_codigo(codigo_cuenta)
        if not cuenta:
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        
        return cuenta
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener cuenta por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/ruc/{ruc}", response_model=CuentaResponse)
def obtener_cuenta_por_ruc(ruc: str):
    """
    Obtener una cuenta por RUC
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        cuenta = cuenta_service.get_cuenta_by_ruc(ruc)
        if not cuenta:
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        
        return cuenta
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener cuenta por RUC: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/cliente/{codigo_cliente}", response_model=List[CuentaResponse])
def obtener_cuentas_por_cliente(codigo_cliente: str):
    """
    Obtener todas las cuentas de un cliente específico
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        cuentas = cuenta_service.get_cuentas_by_cliente(codigo_cliente)
        return cuentas
        
    except Exception as e:
        logger.error(f"Error al obtener cuentas por cliente: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{cuenta_id}", response_model=CuentaResponse)
def actualizar_cuenta(cuenta_id: str, cuenta_update: CuentaUpdate):
    """
    Actualizar una cuenta existente
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        cuenta = cuenta_service.update_cuenta(cuenta_id, cuenta_update.model_dump(exclude_unset=True))
        if not cuenta:
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        
        return cuenta
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar cuenta: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{cuenta_id}")
def eliminar_cuenta(cuenta_id: str):
    """
    Eliminar una cuenta
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        success = cuenta_service.delete_cuenta(cuenta_id)
        if not success:
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        
        return {"message": "Cuenta eliminada correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar cuenta: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{cuenta_id}/export/excel")
def exportar_cuenta_excel(cuenta_id: str):
    """
    Exportar una cuenta específica a Excel
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        cuenta = cuenta_service.get_cuenta_by_id(cuenta_id)
        if not cuenta:
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        
        filter_params = CuentaFilter(codigo_cuenta=cuenta["codigo_cuenta"])
        excel_file = cuenta_service.export_to_excel(filter_params)
        
        return {
            "filename": f"cuenta_{cuenta['codigo_cuenta']}.xlsx",
            "content": excel_file.getvalue()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al exportar cuenta a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_cuentas_excel(
    codigo_cuenta: Optional[str] = Query(None, description="Filtrar por código de cuenta"),
    codigo_cliente: Optional[str] = Query(None, description="Filtrar por código de cliente"),
    nombre: Optional[str] = Query(None, description="Filtrar por nombre"),
    tipo_cliente: Optional[str] = Query(None, description="Filtrar por tipo de cliente"),
    estado: Optional[str] = Query(None, description="Filtrar por estado")
):
    """
    Exportar cuentas a Excel con filtros opcionales
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)

        filter_params = CuentaFilter(
            codigo_cuenta=codigo_cuenta,
            codigo_cliente=codigo_cliente,
            nombre=nombre,
            tipo_cliente=tipo_cliente,
            estado=estado
        )

        excel_file = cuenta_service.export_to_excel(filter_params)
        excel_file.seek(0)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=cuentas.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"Error al exportar cuentas a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.post("/import/excel", response_model=ExcelImportResponse)
def importar_cuentas_excel(file: UploadFile = File(...)):
    """
    Importar cuentas desde archivo Excel
    """
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="Formato de archivo no válido. Use .xlsx o .xls")
        
        db = get_database()
        cuenta_service = CuentaService(db)
        
        content = file.file.read()
        result = cuenta_service.import_from_excel(content)
        
        return {
            "message": "Importación completada",
            "result": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al importar cuentas desde Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/stats/estadisticas")
def obtener_estadisticas():
    """
    Obtener estadísticas de cuentas
    """
    try:
        db = get_database()
        cuenta_service = CuentaService(db)
        
        stats = cuenta_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")