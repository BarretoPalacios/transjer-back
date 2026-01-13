from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from typing import List, Optional
from app.core.database import get_database
from app.modules.dataservice.services.cliente_service import ClienteService 
from app.modules.dataservice.schemas.cliente_schema import (
    ClienteCreate, ClienteUpdate, ClienteResponse, 
    ClienteFilter, ExcelImportResponse, PaginatedResponse
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/clientes", tags=["Clientes"])

@router.post("/", response_model=ClienteResponse)
def crear_cliente(cliente: ClienteCreate):
    """
    Crear un nuevo cliente
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        created_cliente = cliente_service.create_cliente(cliente.model_dump())
        return created_cliente
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al crear cliente: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/", response_model=PaginatedResponse[ClienteResponse], name="listar_clientes_paginados")
def listar_clientes(
    # Parámetros de paginación
    page: int = Query(default=1, ge=1, description="Número de página"),
    page_size: int = Query(default=10, ge=1, le=100, description="Elementos por página"),
    # Parámetros de filtrado
    codigo_cliente: Optional[str] = Query(None, description="Filtrar por código de cliente"),
    tipo_documento: Optional[str] = Query(None, description="Filtrar por tipo de documento"),
    numero_documento: Optional[str] = Query(None, description="Filtrar por número de documento"),
    razon_social: Optional[str] = Query(None, description="Filtrar por razón social"),
    tipo_cliente: Optional[str] = Query(None, description="Filtrar por tipo de cliente (rubro comercial)"),
    tipo_pago: Optional[str] = Query(None, description="Filtrar por tipo de pago"),
    dias_credito: Optional[int] = Query(None, description="Filtrar por días de crédito"),
    contacto_principal: Optional[str] = Query(None, description="Filtrar por contacto principal"),
    telefono: Optional[str] = Query(None, description="Filtrar por teléfono"),
    periodo_facturacion: Optional[str] = Query(None, description="Filtrar por período de facturación"),
    estado: Optional[str] = Query(None, description="Filtrar por estado")
):
    """
    Obtener todos los clientes con filtros opcionales y paginación
    
    - **page**: Número de página (default: 1)
    - **page_size**: Cantidad de elementos por página (default: 10, max: 100)
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        filter_params = ClienteFilter(
            codigo_cliente=codigo_cliente,
            tipo_documento=tipo_documento,
            numero_documento=numero_documento,
            razon_social=razon_social,
            tipo_cliente=tipo_cliente,
            tipo_pago=tipo_pago,
            dias_credito=dias_credito,
            contacto_principal=contacto_principal,
            telefono=telefono,
            periodo_facturacion=periodo_facturacion,
            estado=estado
        )
        
        result = cliente_service.get_all_clientes(filter_params, page, page_size)
        return result
        
    except Exception as e:
        logger.error(f"Error al listar clientes: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{cliente_id}", response_model=ClienteResponse)
def obtener_cliente(cliente_id: str):
    """
    Obtener un cliente por ID
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        cliente = cliente_service.get_cliente_by_id(cliente_id)
        if not cliente:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        return cliente
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener cliente: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/codigo/{codigo_cliente}", response_model=ClienteResponse)
def obtener_cliente_por_codigo(codigo_cliente: str):
    """
    Obtener un cliente por código
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        cliente = cliente_service.get_cliente_by_codigo(codigo_cliente)
        if not cliente:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        return cliente
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener cliente por código: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/documento/{tipo_documento}/{numero_documento}", response_model=ClienteResponse)
def obtener_cliente_por_documento(tipo_documento: str, numero_documento: str):
    """
    Obtener un cliente por tipo y número de documento
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        cliente = cliente_service.get_cliente_by_documento(tipo_documento, numero_documento)
        if not cliente:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        return cliente
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener cliente por documento: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.put("/{cliente_id}", response_model=ClienteResponse)
def actualizar_cliente(cliente_id: str, cliente_update: ClienteUpdate):
    """
    Actualizar un cliente existente
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        cliente = cliente_service.update_cliente(cliente_id, cliente_update.model_dump(exclude_unset=True))
        if not cliente:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        return cliente
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar cliente: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete("/{cliente_id}")
def eliminar_cliente(cliente_id: str):
    """
    Eliminar un cliente
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        success = cliente_service.delete_cliente(cliente_id)
        if not success:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        return {"message": "Cliente eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar cliente: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/{cliente_id}/export/excel")
def exportar_cliente_excel(cliente_id: str):
    """
    Exportar un cliente específico a Excel
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        cliente = cliente_service.get_cliente_by_id(cliente_id)
        if not cliente:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        filter_params = ClienteFilter(codigo_cliente=cliente["codigo_cliente"])
        excel_file = cliente_service.export_to_excel(filter_params)
        
        return {
            "filename": f"cliente_{cliente['codigo_cliente']}.xlsx",
            "content": excel_file.getvalue()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al exportar cliente a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/export/excel")
def exportar_clientes_excel(
    codigo_cliente: Optional[str] = Query(None, description="Filtrar por código de cliente"),
    razon_social: Optional[str] = Query(None, description="Filtrar por razón social"),
    tipo_cliente: Optional[str] = Query(None, description="Filtrar por tipo de cliente (rubro comercial)"),
    tipo_pago: Optional[str] = Query(None, description="Filtrar por tipo de pago"),
    dias_credito: Optional[int] = Query(None, description="Filtrar por días de crédito"),
    periodo_facturacion: Optional[str] = Query(None, description="Filtrar por período de facturación"),
    estado: Optional[str] = Query(None, description="Filtrar por estado")
):
    """
    Exportar clientes a Excel con filtros opcionales
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)

        filter_params = ClienteFilter(
            codigo_cliente=codigo_cliente,
            razon_social=razon_social,
            tipo_cliente=tipo_cliente,
            tipo_pago=tipo_pago,
            dias_credito=dias_credito,
            periodo_facturacion=periodo_facturacion,
            estado=estado
        )

        excel_file = cliente_service.export_to_excel(filter_params)
        excel_file.seek(0)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=clientes.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"Error al exportar clientes a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get("/template/excel", name="descargar_plantilla_importacion")
async def descargar_plantilla_importacion():
    """
    Descargar plantilla de Excel vacía para importación de clientes
    
    La plantilla incluye:
    - Columnas con formato correcto
    - Una fila de ejemplo
    - Hoja de instrucciones con descripción de cada campo
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        template_file = cliente_service.generate_excel_template()
        
        return StreamingResponse(
            template_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=plantilla_importacion_clientes.xlsx"
            }
        )
        
    except Exception as e:
        logger.error(f"Error al generar plantilla: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al generar plantilla")

@router.post("/import/excel", response_model=ExcelImportResponse)
async def importar_clientes_excel(file: UploadFile = File(...)):
    """
    Importar clientes desde archivo Excel
    
    El archivo debe contener las siguientes columnas:
    - **Obligatorias**: Tipo Documento, Número Documento, Razón Social
    - **Opcionales**: Código Cliente, Tipo Cliente, Período Facturación, 
      Período Facturación Días, Tipo Pago, Días Crédito, Contacto Principal, 
      Teléfono, Email, Dirección, Website, Estado, Observaciones
    
    Formatos aceptados: .xlsx, .xls
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
        cliente_service = ClienteService(db)
        
        result = cliente_service.import_from_excel(content)
        
        # Mensaje personalizado según resultados
        if result["has_errors"]:
            if result["created"] > 0 or result["updated"] > 0:
                message = f"Importación completada con advertencias. Procesados: {result['created'] + result['updated']}, Errores: {len(result['errors'])}"
            else:
                message = "Importación completada con errores. Revise el detalle."
        else:
            message = f"Importación exitosa. {result['created']} clientes creados, {result['updated']} actualizados"
        
        return {
            "message": message,
            "result": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al importar clientes desde Excel: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error al procesar el archivo: {str(e)}"
        )

@router.get("/stats/estadisticas")
def obtener_estadisticas():
    """
    Obtener estadísticas de clientes
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        stats = cliente_service.get_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.get("/{cliente_id}/cuentas", response_model=dict)
def obtener_cuentas_cliente(cliente_id: str):
    """
    Obtener cuentas asociadas a un cliente
    """
    try:
        db = get_database()
        cliente_service = ClienteService(db)
        
        cuentas = cliente_service.get_cuentas_by_cliente_id(cliente_id)
        return cuentas
        
    except Exception as e:
        logger.error(f"Error al obtener cuentas del cliente: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.post("/{cliente_id}/cuentas", response_model=dict) 
def agregar_cuenta_a_cliente(cliente_id: str, cuenta_data: dict):
    """
    Agregar una cuenta a un cliente
    """
    try:
        print(cuenta_data)
        db = get_database()
        cliente_service = ClienteService(db)
        
        cuenta = cliente_service.add_cuenta_to_cliente(cliente_id, cuenta_data)
        return cuenta_data
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al agregar cuenta al cliente: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")