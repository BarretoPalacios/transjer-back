
from fastapi import APIRouter, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect, Query
from typing import Dict, Any, Optional
import time
import asyncio
import uuid
import os
import tempfile
from fastapi.responses import StreamingResponse
from app.modules.servicios_historicos.models import (
    ResultadoCarga,
    ActualizarEstadoRequest,
    FiltrosServiciosRequest,
    ErrorDetalle
)
from app.modules.servicios_historicos.excel_processor import ExcelProcessor
from app.modules.servicios_historicos.repository import ServicioRepository


# Router principal
router = APIRouter(
    prefix="/servicios-historicos",
    tags=["Servicios Historicos"]
)


# Diccionario para almacenar estados de carga en progreso
cargas_en_progreso: Dict[str, Dict[str, Any]] = {}


# ========== WEBSOCKET PARA PROGRESO EN TIEMPO REAL ==========

@router.websocket("/ws/progreso/{carga_id}")
async def websocket_progreso(websocket: WebSocket, carga_id: str):
    """
    WebSocket para enviar progreso en tiempo real durante la carga.
    El frontend se conecta a este endpoint antes de iniciar la carga.
    
    Args:
        carga_id: ID único de la carga generado por el cliente
    """
    await websocket.accept()
    
    try:
        while True:
            # Verificar si existe progreso para esta carga
            if carga_id in cargas_en_progreso:
                progreso = cargas_en_progreso[carga_id]
                
                # Enviar progreso al frontend
                await websocket.send_json({
                    "progreso": progreso.get("progreso", 0),
                    "mensaje": progreso.get("mensaje", "Procesando..."),
                    "registros_procesados": progreso.get("registros_procesados", 0),
                    "total_registros": progreso.get("total_registros", 0),
                    "completado": progreso.get("completado", False),
                    "error": progreso.get("error", None)
                })
                
                # Si se completó, cerrar conexión
                if progreso.get("completado"):
                    break
            
            # Esperar 200ms antes de verificar nuevamente
            await asyncio.sleep(0.2)
            
    except WebSocketDisconnect:
        print(f"✂️ Cliente desconectado del WebSocket: {carga_id}")
    except Exception as e:
        print(f"❌ Error en WebSocket: {str(e)}")
    finally:
        # Limpiar datos de progreso después de un tiempo
        await asyncio.sleep(5)
        if carga_id in cargas_en_progreso:
            del cargas_en_progreso[carga_id]


# ========== ENDPOINTS HTTP ==========

@router.post("/cargar-excel", response_model=ResultadoCarga)
def cargar_excel(
    archivo: UploadFile = File(...),
    usuario: str = "sistema"
):
    """
    Carga un archivo Excel con servicios de transporte.
    
    **IMPORTANTE:** Se insertarán TODOS los registros, incluso aquellos con datos faltantes.
    Solo se generarán advertencias para campos vacíos o inconsistentes.
    
    **Proceso:**
    1. Validación del archivo
    2. Procesamiento y limpieza de datos
    3. Generación de advertencias (NO errores críticos)
    4. Inserción de TODOS los registros en MongoDB
    5. Reporte detallado de resultados
    
    **Uso con WebSocket:**
    Conectar al WebSocket `/servicios/ws/progreso/{carga_id}` antes de llamar
    a este endpoint para recibir actualizaciones en tiempo real.
    
    Args:
        archivo: Archivo Excel (.xlsx o .xls)
        usuario: Usuario que realiza la carga
        
    Returns:
        ResultadoCarga con estadísticas y detalles de advertencias
    """
    inicio = time.time()
    
    # Validar tipo de archivo
    if not archivo.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="El archivo debe ser Excel (.xlsx o .xls)"
        )
    
    # Generar ID único para esta carga
    carga_id = str(uuid.uuid4())
    
    # Inicializar estado de progreso
    cargas_en_progreso[carga_id] = {
        "progreso": 0,
        "mensaje": "Iniciando carga...",
        "registros_procesados": 0,
        "total_registros": 0,
        "completado": False,
        "error": None
    }
    
    try:
        # Crear archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            contenido = archivo.file.read()
            tmp_file.write(contenido)
            tmp_path = tmp_file.name
        
        try:
            # Actualizar progreso
            cargas_en_progreso[carga_id].update({
                "progreso": 10,
                "mensaje": "Procesando Excel..."
            })
            
            # Procesar Excel
            processor = ExcelProcessor(
                archivo_nombre=archivo.filename,
                usuario_carga=usuario
            )
            
            # Leer archivo desde ruta temporal
            with open(tmp_path, 'rb') as f:
                contenido_bytes = f.read()
            
            # IMPORTANTE: Ahora todos los registros se insertan
            registros_limpios, errores, advertencias = processor.procesar_excel(contenido_bytes)
            
            total_registros = len(registros_limpios)
            
            # Actualizar progreso
            cargas_en_progreso[carga_id].update({
                "progreso": 30,
                "mensaje": f"Procesados {len(registros_limpios)} registros",
                "total_registros": total_registros,
                "advertencias": len(advertencias)
            })
            
            # Si no hay registros, retornar error
            if not registros_limpios:
                resultado = ResultadoCarga(
                    total_registros=0,
                    insertados=0,
                    errores=0,
                    advertencias=len(advertencias),
                    tiempo_procesamiento=time.time() - inicio,
                    detalles_errores=[],
                    detalles_advertencias=advertencias
                )
                
                cargas_en_progreso[carga_id].update({
                    "progreso": 100,
                    "mensaje": "No se procesaron registros",
                    "completado": True
                })
                
                return resultado
            
            # Insertar en base de datos por lotes
            repository = ServicioRepository()
            
            def callback_progreso(progreso, registros_procesados, total_registros):
                """Callback para actualizar progreso durante inserción"""
                progreso_real = 30 + (progreso * 0.7)  # 30-100%
                
                cargas_en_progreso[carga_id].update({
                    "progreso": progreso_real,
                    "mensaje": f"Insertando registros en base de datos... {int(progreso)}%",
                    "registros_procesados": registros_procesados,
                    "total_registros": total_registros
                })
            
            # IMPORTANTE: Se insertan TODOS los registros
            resultado_insercion = repository.insertar_por_lotes(
                registros_limpios,
                tamaño_lote=500,
                callback_progreso=callback_progreso
            )
            
            # Preparar respuesta - ahora "errores" son realmente errores de inserción
            tiempo_procesamiento = time.time() - inicio
            
            resultado = ResultadoCarga(
                total_registros=total_registros,
                insertados=resultado_insercion['insertados'],
                errores=resultado_insercion.get('errores_insercion', 0),
                advertencias=len(advertencias),
                tiempo_procesamiento=tiempo_procesamiento,
                detalles_errores=[ErrorDetalle(
                    fila=0,
                    mensaje=f"Errores de inserción: {resultado_insercion.get('errores_insercion', 0)}",
                    tipo="ERROR"
                )] if resultado_insercion.get('errores_insercion', 0) > 0 else [],
                detalles_advertencias=advertencias[:100]  # Limitar a 100 para no sobrecargar
            )
            
            # Marcar como completado
            cargas_en_progreso[carga_id].update({
                "progreso": 100,
                "mensaje": f"✅ Carga completada: {resultado_insercion['insertados']} registros insertados ({len(advertencias)} advertencias)",
                "completado": True,
                "duplicados": resultado_insercion.get('duplicados', 0)
            })
            
            return resultado
            
        finally:
            # Limpiar archivo temporal
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
    except ValueError as e:
        cargas_en_progreso[carga_id].update({
            "error": str(e),
            "completado": True
        })
        raise HTTPException(status_code=400, detail=str(e))
        
    except Exception as e:
        cargas_en_progreso[carga_id].update({
            "error": f"Error inesperado: {str(e)}",
            "completado": True
        })
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@router.get("/buscar")
def buscar_servicios(
    cliente: Optional[str] = None,
    estado_factura: Optional[str] = None,
    estado_servicio: Optional[str] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    servicio: Optional[str] = None,
    grte: Optional[str] = None,
    cliente_destino: Optional[str] = None,
    proveedor: Optional[str] = None,
    conductor: Optional[str] = None,
    placa: Optional[str] = None,
    busqueda_general: Optional[str] = None,  # Nuevo parámetro para búsqueda general
    skip: int = 0,
    limit: int = 100,
    ordenar_por: str = "fecha_servicio",
    orden: str = "desc"
):
    """
    Busca servicios con filtros opcionales.
    
    **Nuevo parámetro: busqueda_general**
    Busca en múltiples campos: cliente, conductor, placa, factura, servicio, etc.
    
    Args:
        busqueda_general: Texto para búsqueda en múltiples campos
        ... (otros parámetros)
    """
    try:
        from datetime import datetime
        
        # Construir filtros asegurando que sean strings válidos
        filtros = {}
        
        # Función helper para limpiar valores
        def limpiar_valor(valor):
            if valor is None:
                return None
            valor_str = str(valor).strip()
            return valor_str if valor_str else None
        
        # Asignar filtros con validación
        if cliente:
            cliente_limpio = limpiar_valor(cliente)
            if cliente_limpio:
                filtros["cliente"] = cliente_limpio
        
        if estado_factura:
            estado_limpio = limpiar_valor(estado_factura)
            if estado_limpio:
                filtros["estado_factura"] = estado_limpio
        
        if estado_servicio:
            estado_servicio_limpio = limpiar_valor(estado_servicio)
            if estado_servicio_limpio:
                filtros["estado_servicio"] = estado_servicio_limpio
        
        # Nuevos filtros
        if servicio:
            servicio_limpio = limpiar_valor(servicio)
            if servicio_limpio:
                filtros["servicio"] = servicio_limpio
        
        if grte:
            grte_limpio = limpiar_valor(grte)
            if grte_limpio:
                filtros["grte"] = grte_limpio
        
        if cliente_destino:
            cliente_destino_limpio = limpiar_valor(cliente_destino)
            if cliente_destino_limpio:
                filtros["cliente_destino"] = cliente_destino_limpio
        
        if proveedor:
            proveedor_limpio = limpiar_valor(proveedor)
            if proveedor_limpio:
                filtros["proveedor"] = proveedor_limpio
        
        if conductor:
            conductor_limpio = limpiar_valor(conductor)
            if conductor_limpio:
                filtros["conductor"] = conductor_limpio
        
        if placa:
            placa_limpio = limpiar_valor(placa)
            if placa_limpio:
                filtros["placa"] = placa_limpio
        
        # Búsqueda general
        if busqueda_general:
            busqueda_limpio = limpiar_valor(busqueda_general)
            if busqueda_limpio:
                filtros["busqueda_general"] = busqueda_limpio
        
        # Filtros de fecha
        if fecha_desde:
            fecha_desde_limpio = limpiar_valor(fecha_desde)
            if fecha_desde_limpio:
                filtros["fecha_desde"] = fecha_desde_limpio
        
        if fecha_hasta:
            fecha_hasta_limpio = limpiar_valor(fecha_hasta)
            if fecha_hasta_limpio:
                filtros["fecha_hasta"] = fecha_hasta_limpio
        
        # Validar parámetros de ordenación
        campos_validos = [
            "fecha_servicio", "fecha_salida", "cliente", "servicio", 
            "proveedor", "factura.numero", "estado_servicio", "created_at"
        ]
        
        if ordenar_por not in campos_validos:
            ordenar_por = "fecha_servicio"
        
        orden_num = -1 if orden.lower() == "desc" else 1
        
        repository = ServicioRepository()
        servicios = repository.buscar_servicios(
            filtros=filtros,
            skip=skip,
            limit=limit,
            ordenar_por=ordenar_por,
            orden=orden_num
        )
        
        # Contar total para paginación
        total = repository.contar_servicios(filtros)
        
        return {
            "servicios": servicios,
            "paginacion": {
                "skip": skip,
                "limit": limit,
                "total": total,
                "has_more": skip + len(servicios) < total
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en buscar_servicios: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/estadisticas")
def obtener_estadisticas():
    """
    Obtiene estadísticas generales de servicios.
    
    **Ahora incluye:**
    - Servicios por estado de factura
    - Servicios por estado de servicio
    - Top 10 clientes con más servicios
    - Servicios por mes
    - Top 10 proveedores
    - Top 10 servicios más frecuentes
    - Total de servicios
    - Servicios pendientes de facturación
    
    Returns:
        Dict con múltiples estadísticas agrupadas
    """
    try:
        repository = ServicioRepository()
        estadisticas = repository.obtener_estadisticas()
        return estadisticas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pendientes-facturacion")
def obtener_pendientes_facturacion(
    skip: int = 0,
    limit: int = 100
):
    """
    Obtiene servicios completados pero pendientes de facturación.
    
    Útil para identificar servicios que ya se realizaron pero aún no
    tienen factura asociada.
    
    Args:
        skip: Número de registros a saltar
        limit: Límite de registros a retornar
        
    Returns:
        Lista de servicios pendientes de facturación
    """
    try:
        repository = ServicioRepository()
        servicios = repository.obtener_servicios_pendientes_facturacion(
            skip=skip,
            limit=limit
        )
        return servicios
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recientes")
def obtener_servicios_recientes(
    dias: int = 7,
    limit: int = 50
):
    """
    Obtiene servicios recientes.
    
    Args:
        dias: Número de días hacia atrás (default: 7)
        limit: Límite de resultados (default: 50)
        
    Returns:
        Lista de servicios recientes
    """
    try:
        repository = ServicioRepository()
        servicios = repository.obtener_servicios_recientes(
            dias=dias,
            limit=limit
        )
        return servicios
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{servicio_id}")
def obtener_servicio(servicio_id: str):
    """
    Obtiene un servicio específico por ID.
    
    **Ahora incluye:** Todos los nuevos campos (servicio, grte, cliente_destino)
    
    Args:
        servicio_id: ID del servicio
        
    Returns:
        Datos completos del servicio incluyendo historial de cambios
    """
    try:
        repository = ServicioRepository()
        servicio = repository.buscar_por_id(servicio_id)
        
        if not servicio:
            raise HTTPException(status_code=404, detail="Servicio no encontrado")
        
        return servicio
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{servicio_id}")
def actualizar_servicio(
    servicio_id: str,
    datos: Dict[str, Any],
    usuario: Optional[str] = None
):
    try:
        repository = ServicioRepository()
        actualizado = repository.actualizar_servicio(
            servicio_id=servicio_id,
            datos=datos,
            usuario=usuario
        )
        
        if not actualizado:
            raise HTTPException(status_code=404, detail="Servicio no encontrado")
        
        return {
            "mensaje": "Servicio actualizado correctamente",
            "servicio_id": servicio_id,
            "actualizado": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{servicio_id}")
def eliminar_servicio(servicio_id: str):
    """
    Elimina un servicio por ID.
    
    ⚠️ **Advertencia:** Esta acción es permanente y no se puede deshacer.
    
    Args:
        servicio_id: ID del servicio a eliminar
        
    Returns:
        Confirmación de eliminación
    """
    try:
        repository = ServicioRepository()
        eliminado = repository.eliminar_servicio(servicio_id)
        
        if not eliminado:
            raise HTTPException(status_code=404, detail="Servicio no encontrado")
        
        return {
            "mensaje": "Servicio eliminado correctamente",
            "servicio_id": servicio_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/busqueda/texto")
def buscar_por_texto(
    q: str,
    limit: int = 50
):
    """
    Busca servicios por texto en múltiples campos.
    
    **Ahora busca en:** cliente, conductor, placa, número de factura,
    servicio, origen, destino, grte
    
    Args:
        q: Texto a buscar
        limit: Límite de resultados (máximo 100)
        
    Returns:
        Lista de servicios que coinciden con la búsqueda
    """
    try:
        if not q or len(q) < 2:
            raise HTTPException(
                status_code=400,
                detail="La búsqueda debe tener al menos 2 caracteres"
            )
        
        if limit > 100:
            limit = 100
        
        repository = ServicioRepository()
        servicios = repository.buscar_por_texto(q, limit=limit)
        
        return servicios
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/exportar/excel")
def exportar_servicios_excel(
    cliente: Optional[str] = Query(None, description="Filtrar por cliente"),
    estado_factura: Optional[str] = Query(None, description="Filtrar por estado de factura"),
    estado_servicio: Optional[str] = Query(None, description="Filtrar por estado de servicio"),
    fecha_desde: Optional[str] = Query(None, description="Fecha desde (YYYY-MM-DD)"),
    fecha_hasta: Optional[str] = Query(None, description="Fecha hasta (YYYY-MM-DD)"),
    servicio: Optional[str] = Query(None, description="Filtrar por servicio"),
    grte: Optional[str] = Query(None, description="Filtrar por GRTE"),
    cliente_destino: Optional[str] = Query(None, description="Filtrar por cliente destino"),
    proveedor: Optional[str] = Query(None, description="Filtrar por proveedor"),
    conductor: Optional[str] = Query(None, description="Filtrar por conductor"),
    placa: Optional[str] = Query(None, description="Filtrar por placa"),
    busqueda_general: Optional[str] = Query(None, description="Búsqueda general en múltiples campos")
):
    """
    Exportar servicios a Excel con filtros opcionales
    """
    try:
        def limpiar_valor(valor):
            if valor is None:
                return None
            valor_str = str(valor).strip()
            return valor_str if valor_str else None
        
        filtros = {}
        
        if cliente:
            cliente_limpio = limpiar_valor(cliente)
            if cliente_limpio:
                filtros["cliente"] = cliente_limpio
        
        if estado_factura:
            estado_limpio = limpiar_valor(estado_factura)
            if estado_limpio:
                filtros["estado_factura"] = estado_limpio
        
        if estado_servicio:
            estado_servicio_limpio = limpiar_valor(estado_servicio)
            if estado_servicio_limpio:
                filtros["estado_servicio"] = estado_servicio_limpio
        
        if servicio:
            servicio_limpio = limpiar_valor(servicio)
            if servicio_limpio:
                filtros["servicio"] = servicio_limpio
        
        if grte:
            grte_limpio = limpiar_valor(grte)
            if grte_limpio:
                filtros["grte"] = grte_limpio
        
        if cliente_destino:
            cliente_destino_limpio = limpiar_valor(cliente_destino)
            if cliente_destino_limpio:
                filtros["cliente_destino"] = cliente_destino_limpio
        
        if proveedor:
            proveedor_limpio = limpiar_valor(proveedor)
            if proveedor_limpio:
                filtros["proveedor"] = proveedor_limpio
        
        if conductor:
            conductor_limpio = limpiar_valor(conductor)
            if conductor_limpio:
                filtros["conductor"] = conductor_limpio
        
        if placa:
            placa_limpio = limpiar_valor(placa)
            if placa_limpio:
                filtros["placa"] = placa_limpio
        
        if busqueda_general:
            busqueda_limpio = limpiar_valor(busqueda_general)
            if busqueda_limpio:
                filtros["busqueda_general"] = busqueda_limpio
        
        if fecha_desde:
            fecha_desde_limpio = limpiar_valor(fecha_desde)
            if fecha_desde_limpio:
                filtros["fecha_desde"] = fecha_desde_limpio
        
        if fecha_hasta:
            fecha_hasta_limpio = limpiar_valor(fecha_hasta)
            if fecha_hasta_limpio:
                filtros["fecha_hasta"] = fecha_hasta_limpio
        
        repository = ServicioRepository()
        excel_file = repository.exportar_servicios_excel(filtros)
        excel_file.seek(0)
        
        from datetime import datetime
        fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=servicios_historicos_{fecha_actual}.xlsx"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error al exportar servicios a Excel: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al exportar: {str(e)}")

