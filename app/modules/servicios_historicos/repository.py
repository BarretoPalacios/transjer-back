
from io import BytesIO
from typing import List, Dict, Any, Optional
from datetime import datetime
from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, TEXT

from app.core.database import get_database


class ServicioRepository:
    
    def __init__(self):
        db = get_database()
        self.collection = db["servicios"]
    
    def crear_indices(self):
        """Crea índices para optimizar consultas"""
        
        # Índices compuestos para queries frecuentes
        self.collection.create_index([
            ("factura.estado", ASCENDING),
            ("factura.fecha_emision", DESCENDING)
        ], name="idx_factura_estado_fecha")
        
        self.collection.create_index([
            ("fecha_servicio", DESCENDING)
        ], name="idx_fecha_servicio")
        
        self.collection.create_index([
            ("cliente", ASCENDING),
            ("mes", ASCENDING)
        ], name="idx_cliente_mes")
        
        self.collection.create_index([
            ("factura.numero", ASCENDING)
        ], name="idx_factura_numero", unique=False, sparse=True)  # Cambiado unique=False
        
        self.collection.create_index([
            ("estado_servicio", ASCENDING)
        ], name="idx_estado_servicio")
        
        self.collection.create_index([
            ("placa", ASCENDING)
        ], name="idx_placa")
        
        # Índice para los nuevos campos
        self.collection.create_index([
            ("servicio", ASCENDING)
        ], name="idx_servicio", sparse=True)
        
        self.collection.create_index([
            ("grte", ASCENDING)
        ], name="idx_grte", sparse=True)
        
        self.collection.create_index([
            ("cliente_destino", ASCENDING)
        ], name="idx_cliente_destino", sparse=True)
        
        self.collection.create_index([
            ("proveedor", ASCENDING)
        ], name="idx_proveedor")
        
        # Índice de texto para búsquedas
        self.collection.create_index([
            ("cliente", TEXT),
            ("conductor", TEXT),
            ("placa", TEXT),
            ("factura.numero", TEXT),
            ("servicio", TEXT),  # Nuevo campo
            ("origen", TEXT),    # Nuevo campo
            ("destino", TEXT)    # Nuevo campo
        ], name="idx_text_search")
        
        print("✅ Índices de servicios creados exitosamente")
    
    def insertar_por_lotes(
        self, 
        registros: List[Dict[str, Any]], 
        tamaño_lote: int = 500,
        callback_progreso = None
    ) -> Dict[str, Any]:
        """
        Inserta registros por lotes con reporte de progreso
        
        Args:
            registros: Lista de registros a insertar
            tamaño_lote: Tamaño de cada lote (default 500)
            callback_progreso: Función callback para reportar progreso
            
        Returns:
            Dict con resultado de la inserción
        """
        total_registros = len(registros)
        insertados = 0
        errores_insercion = []
        duplicados = 0
        
        # Pre-procesar registros para limpiar datos
        registros_procesados = []
        for registro in registros:
            try:
                # Asegurar que los nuevos campos existan
                registro.setdefault("servicio", None)
                registro.setdefault("cliente_destino", None)
                registro.setdefault("grte", None)
                registro.setdefault("servicio_descripcion", None)
                registro.setdefault("cliente_grte", None)
                
                # Asegurar que campos críticos tengan valores por defecto
                if not registro.get("cliente"):
                    registro["cliente"] = "DESCONOCIDO"
                
                if not registro.get("fecha_servicio"):
                    registro["fecha_servicio"] = datetime.utcnow()
                
                # Asegurar estructura de factura
                if "factura" not in registro or not isinstance(registro["factura"], dict):
                    registro["factura"] = {
                        "numero": None,
                        "fecha_emision": None,
                        "estado": "PENDIENTE",
                        "monto": None,
                        "moneda": "PEN"
                    }
                
                registros_procesados.append(registro)
            except Exception as e:
                errores_insercion.append({
                    'registro': registro,
                    'error': f"Error en pre-procesamiento: {str(e)}"
                })
        
        # Usar registros procesados
        registros = registros_procesados
        
        # Dividir en lotes
        lotes = [
            registros[i:i + tamaño_lote] 
            for i in range(0, total_registros, tamaño_lote)
        ]
        
        for idx, lote in enumerate(lotes):
            try:
                # Insertar lote con continue_on_error=True
                resultado = self.collection.insert_many(lote, ordered=False)
                insertados += len(resultado.inserted_ids)
                
                # Reportar progreso
                if callback_progreso:
                    progreso = ((idx + 1) / len(lotes)) * 100
                    callback_progreso(
                        progreso=progreso,
                        registros_procesados=min((idx + 1) * tamaño_lote, total_registros),
                        total_registros=total_registros
                    )
                
            except Exception as e:
                # Si hay error de duplicado, intentar insertar uno por uno
                if "duplicate key error" in str(e).lower():
                    for registro in lote:
                        try:
                            # Intentar insertar individualmente
                            self.collection.insert_one(registro)
                            insertados += 1
                        except Exception as e2:
                            if "duplicate key error" in str(e2).lower():
                                duplicados += 1
                            else:
                                errores_insercion.append({
                                    'lote': idx,
                                    'error': f"Error individual: {str(e2)}",
                                    'registro': registro
                                })
                else:
                    errores_insercion.append({
                        'lote': idx,
                        'error': str(e),
                        'registros_afectados': len(lote)
                    })
        
        return {
            'total_registros': total_registros,
            'insertados': insertados,
            'duplicados': duplicados,
            'errores_insercion': len(errores_insercion)
        }
    
    def insertar_servicio_unico(self, registro: Dict[str, Any]) -> Optional[str]:
        """
        Inserta un solo servicio
        
        Args:
            registro: Datos del servicio a insertar
            
        Returns:
            ID del servicio insertado o None si hubo error
        """
        try:
            # Procesar registro similar a insertar_por_lotes
            registro.setdefault("servicio", None)
            registro.setdefault("cliente_destino", None)
            registro.setdefault("grte", None)
            registro.setdefault("servicio_descripcion", None)
            registro.setdefault("cliente_grte", None)
            
            # Asegurar campos críticos
            if not registro.get("cliente"):
                registro["cliente"] = "DESCONOCIDO"
            
            if not registro.get("fecha_servicio"):
                registro["fecha_servicio"] = datetime.utcnow()
            
            # Asegurar estructura de factura
            if "factura" not in registro or not isinstance(registro["factura"], dict):
                registro["factura"] = {
                    "numero": None,
                    "fecha_emision": None,
                    "estado": "PENDIENTE",
                    "monto": None,
                    "moneda": "PEN"
                }
            
            resultado = self.collection.insert_one(registro)
            return str(resultado.inserted_id)
            
        except Exception as e:
            print(f"Error al insertar servicio único: {str(e)}")
            return None
    
    def buscar_por_id(self, servicio_id: str) -> Optional[Dict[str, Any]]:
        """Busca un servicio por ID"""
        try:
            servicio = self.collection.find_one({"_id": ObjectId(servicio_id)})
            if servicio:
                servicio["_id"] = str(servicio["_id"])
            return servicio
        except Exception:
            return None
    
    def buscar_servicios(
            self,
            filtros: Dict[str, Any] = None,
            skip: int = 0,
            limit: int = 100,
            ordenar_por: str = "fecha_servicio",
            orden: int = -1
        ) -> List[Dict[str, Any]]:
            """
            Busca servicios con filtros y paginación
            """
            query = self._construir_query(filtros) if filtros else {}
            
            try:
                # Validar que el campo de ordenación exista
                campos_validos = [
                    "fecha_servicio", "fecha_salida", "cliente", "servicio", 
                    "proveedor", "factura.numero", "estado_servicio", 
                    "created_at", "updated_at", "factura.fecha_emision"
                ]
                
                if ordenar_por not in campos_validos:
                    ordenar_por = "fecha_servicio"
                
                cursor = self.collection.find(query).sort(ordenar_por, orden).skip(skip).limit(limit)
                servicios = list(cursor)
                
                # Convertir ObjectId a string y asegurar campos
                for servicio in servicios:
                    servicio["_id"] = str(servicio["_id"])
                    
                    # Asegurar que todos los campos existan
                    campos_requeridos = [
                        "servicio", "cliente_destino", "grte", "servicio_descripcion",
                        "cliente_grte", "conductor", "auxiliar", "origen", "destino",
                        "placa", "tipo_camion", "capacidad_m3", "capacidad_tn"
                    ]
                    
                    for campo in campos_requeridos:
                        servicio.setdefault(campo, None)
                
                return servicios
                
            except Exception as e:
                print(f"❌ Error en buscar_servicios: {str(e)}")
                return []
    
    def _construir_query(self, filtros: Dict[str, Any]) -> Dict[str, Any]:
            """
            Construye query MongoDB a partir de filtros del usuario
            Corrige el error "$regex has to be a string"
            """
            query = {}
            
            # Helper function para validar y limpiar valores para regex
            def _validar_valor_regex(valor):
                if valor is None:
                    return None
                valor_str = str(valor).strip()
                return valor_str if valor_str else None
            
            # Filtros básicos
            if filtros.get("cliente"):
                cliente_valor = _validar_valor_regex(filtros["cliente"])
                if cliente_valor:
                    query["cliente"] = {"$regex": cliente_valor, "$options": "i"}
            
            if filtros.get("estado_factura"):
                estado_valor = str(filtros["estado_factura"]).strip()
                if estado_valor:
                    query["factura.estado"] = estado_valor
            
            if filtros.get("estado_servicio"):
                estado_servicio_valor = str(filtros["estado_servicio"]).strip()
                if estado_servicio_valor:
                    query["estado_servicio"] = estado_servicio_valor
            
            # Filtros para nuevos campos con validación
            if filtros.get("servicio"):
                servicio_valor = _validar_valor_regex(filtros["servicio"])
                if servicio_valor:
                    query["servicio"] = {"$regex": servicio_valor, "$options": "i"}
            
            if filtros.get("grte"):
                grte_valor = _validar_valor_regex(filtros["grte"])
                if grte_valor:
                    query["grte"] = {"$regex": grte_valor, "$options": "i"}
            
            if filtros.get("cliente_destino"):
                cliente_destino_valor = _validar_valor_regex(filtros["cliente_destino"])
                if cliente_destino_valor:
                    query["cliente_destino"] = {"$regex": cliente_destino_valor, "$options": "i"}
            
            if filtros.get("proveedor"):
                proveedor_valor = _validar_valor_regex(filtros["proveedor"])
                if proveedor_valor:
                    query["proveedor"] = {"$regex": proveedor_valor, "$options": "i"}
            
            if filtros.get("conductor"):
                conductor_valor = _validar_valor_regex(filtros["conductor"])
                if conductor_valor:
                    query["conductor"] = {"$regex": conductor_valor, "$options": "i"}
            
            if filtros.get("placa"):
                placa_valor = _validar_valor_regex(filtros["placa"])
                if placa_valor:
                    query["placa"] = {"$regex": placa_valor, "$options": "i"}
            
            # Filtros de fecha
            if filtros.get("fecha_desde") or filtros.get("fecha_hasta"):
                query["fecha_servicio"] = {}
                
                if filtros.get("fecha_desde"):
                    try:
                        fecha_desde = datetime.strptime(str(filtros["fecha_desde"]).strip(), "%Y-%m-%d")
                        query["fecha_servicio"]["$gte"] = fecha_desde
                    except (ValueError, TypeError) as e:
                        print(f"⚠️  Error parseando fecha_desde: {e}")
                
                if filtros.get("fecha_hasta"):
                    try:
                        fecha_hasta = datetime.strptime(str(filtros["fecha_hasta"]).strip(), "%Y-%m-%d")
                        query["fecha_servicio"]["$lte"] = fecha_hasta
                    except (ValueError, TypeError) as e:
                        print(f"⚠️  Error parseando fecha_hasta: {e}")
                
                # Si no se pudo parsear ninguna fecha, eliminar el campo
                if not query["fecha_servicio"]:
                    del query["fecha_servicio"]
            
            # Filtro para búsqueda general de texto
            if filtros.get("busqueda_general"):
                busqueda_valor = _validar_valor_regex(filtros["busqueda_general"])
                if busqueda_valor:
                    query["$or"] = [
                        {"cliente": {"$regex": busqueda_valor, "$options": "i"}},
                        {"cliente_destino": {"$regex": busqueda_valor, "$options": "i"}},
                        {"cuenta": {"$regex": busqueda_valor, "$options": "i"}},
                        {"conductor": {"$regex": busqueda_valor, "$options": "i"}},
                        {"placa": {"$regex": busqueda_valor, "$options": "i"}},
                        {"factura.numero": {"$regex": busqueda_valor, "$options": "i"}},
                        {"servicio": {"$regex": busqueda_valor, "$options": "i"}},
                        {"origen": {"$regex": busqueda_valor, "$options": "i"}},
                        {"destino": {"$regex": busqueda_valor, "$options": "i"}},
                        {"grte": {"$regex": busqueda_valor, "$options": "i"}},
                        {"proveedor": {"$regex": busqueda_valor, "$options": "i"}},
                        {"cliente_destino": {"$regex": busqueda_valor, "$options": "i"}}
                    ]
            
            # print(f"🔍 Query construida: {query}")
            return query
    
    def contar_servicios(self, filtros: Dict[str, Any] = None) -> int:
        """Cuenta servicios que cumplan con los filtros"""
        query = self._construir_query(filtros) if filtros else {}
        return self.collection.count_documents(query)
    
    def actualizar_servicio(
        self, 
        servicio_id: str, 
        datos: Dict[str, Any],
        usuario: str = None
    ) -> bool:
        try:
            servicio_anterior = self.collection.find_one({"_id": ObjectId(servicio_id)})
            
            if not servicio_anterior:
                return False
            
            estado_servicio_actual = str(servicio_anterior.get("estado_servicio", "")).strip().upper()
            
            if "nuevo_estado" in datos:
                nuevo_estado_factura = str(datos["nuevo_estado"]).strip().upper()
                
                datos_actualizacion = {
                    "factura.estado": nuevo_estado_factura,
                    "factura.numero": datos.get("numero_factura"),
                    "factura.monto": datos.get("monto"),
                    "factura.moneda": datos.get("moneda", "PEN"),
                    "factura.fecha_emision": datos.get("fecha_emision")
                }
                
                if nuevo_estado_factura == "FACTURADO" and estado_servicio_actual != "COMPLETADO":
                    datos_actualizacion["estado_servicio"] = "COMPLETADO"
                    
                    if not servicio_anterior.get("fecha_completado"):
                        datos_actualizacion["fecha_completado"] = datetime.utcnow()
                
                datos_actualizacion["updated_at"] = datetime.utcnow()
                
                cambios = []
                for campo, valor_nuevo in datos_actualizacion.items():
                    valor_anterior = self._obtener_valor_anterior(servicio_anterior, campo)
                    
                    if isinstance(valor_anterior, (datetime, ObjectId)):
                        valor_anterior_str = str(valor_anterior)
                    else:
                        valor_anterior_str = valor_anterior
                    
                    if isinstance(valor_nuevo, (datetime, ObjectId)):
                        valor_nuevo_str = str(valor_nuevo)
                    else:
                        valor_nuevo_str = valor_nuevo
                    
                    if valor_anterior_str != valor_nuevo_str:
                        cambios.append({
                            "campo": campo,
                            "valor_anterior": str(valor_anterior) if valor_anterior is not None else None,
                            "valor_nuevo": str(valor_nuevo) if valor_nuevo is not None else None,
                            "fecha": datetime.utcnow(),
                            "usuario": usuario or "sistema"
                        })
                
                update_data = {"$set": datos_actualizacion}
                
                if cambios:
                    update_data["$push"] = {"historial_cambios": {"$each": cambios}}
                
                resultado = self.collection.update_one(
                    {"_id": ObjectId(servicio_id)},
                    update_data
                )
                
                return resultado.modified_count > 0
            
            return False
            
        except Exception as e:
            print(f"Error al actualizar servicio: {str(e)}")
            return False

    def _obtener_valor_anterior(self, servicio: Dict[str, Any], campo: str) -> Any:
        """Obtiene el valor anterior de un campo (soporta campos anidados)"""
        if '.' in campo:
            partes = campo.split('.')
            valor = servicio.get(partes[0], {})
            for parte in partes[1:]:
                if isinstance(valor, dict):
                    valor = valor.get(parte)
                else:
                    return None
            return valor
        else:
            return servicio.get(campo)

    def eliminar_servicio(self, servicio_id: str) -> bool:
        """Elimina un servicio por ID"""
        resultado = self.collection.delete_one({"_id": ObjectId(servicio_id)})
        return resultado.deleted_count > 0
    
    def eliminar_todos(self) -> bool:
        """Elimina todos los servicios (usar con precaución)"""
        resultado = self.collection.delete_many({})
        return resultado.deleted_count > 0
    
    def obtener_estadisticas(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas generales de servicios
        
        Returns:
            Dict con estadísticas agrupadas por diferentes criterios
        """
        pipeline = [
            {
                "$facet": {
                    "por_estado_factura": [
                        {
                            "$group": {
                                "_id": "$factura.estado",
                                "cantidad": {"$sum": 1},
                                "monto_total": {"$sum": "$factura.monto"}
                            }
                        },
                        {"$sort": {"cantidad": -1}}
                    ],
                    "por_estado_servicio": [
                        {
                            "$group": {
                                "_id": "$estado_servicio",
                                "cantidad": {"$sum": 1}
                            }
                        },
                        {"$sort": {"cantidad": -1}}
                    ],
                    "por_cliente": [
                        {
                            "$group": {
                                "_id": "$cliente",
                                "cantidad": {"$sum": 1}
                            }
                        },
                        {"$sort": {"cantidad": -1}},
                        {"$limit": 10}
                    ],
                    "por_mes": [
                        {
                            "$group": {
                                "_id": "$mes",
                                "cantidad": {"$sum": 1}
                            }
                        },
                        {"$sort": {"cantidad": -1}}
                    ],
                    "por_proveedor": [
                        {
                            "$group": {
                                "_id": "$proveedor",
                                "cantidad": {"$sum": 1}
                            }
                        },
                        {"$sort": {"cantidad": -1}},
                        {"$limit": 10}
                    ],
                    "por_servicio": [
                        {
                            "$group": {
                                "_id": "$servicio",
                                "cantidad": {"$sum": 1}
                            }
                        },
                        {"$sort": {"cantidad": -1}},
                        {"$limit": 10}
                    ],
                    "total": [
                        {"$count": "total"}
                    ],
                    "pendientes_facturacion": [
                        {
                            "$match": {
                                "factura.estado": "PENDIENTE",
                                "estado_servicio": {"$in": ["COMPLETADO", "PENDIENTE_FACTURACION"]}
                            }
                        },
                        {"$count": "total"}
                    ]
                }
            }
        ]
        
        try:
            cursor = self.collection.aggregate(pipeline)
            resultado = list(cursor)
            
            if resultado:
                return resultado[0]
        except Exception as e:
            print(f"Error en estadísticas: {str(e)}")
        
        return {
            "por_estado_factura": [],
            "por_estado_servicio": [],
            "por_cliente": [],
            "por_mes": [],
            "por_proveedor": [],
            "por_servicio": [],
            "total": [{"total": 0}],
            "pendientes_facturacion": [{"total": 0}]
        }
    
    def buscar_por_texto(self, texto: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Busca servicios por texto en múltiples campos
        
        Args:
            texto: Texto a buscar
            limit: Límite de resultados
            
        Returns:
            Lista de servicios que coinciden con la búsqueda
        """
        try:
            # Verificar si existe índice de texto
            indices = list(self.collection.list_indexes())
            tiene_indice_texto = any('text' in str(idx) for idx in indices)
            
            if tiene_indice_texto:
                cursor = self.collection.find({"$text": {"$search": texto}}).limit(limit)
            else:
                # Búsqueda manual si no hay índice
                query = {
                    "$or": [
                        {"cliente": {"$regex": texto, "$options": "i"}},
                        {"conductor": {"$regex": texto, "$options": "i"}},
                        {"placa": {"$regex": texto, "$options": "i"}},
                        {"factura.numero": {"$regex": texto, "$options": "i"}},
                        {"servicio": {"$regex": texto, "$options": "i"}},
                        {"origen": {"$regex": texto, "$options": "i"}},
                        {"destino": {"$regex": texto, "$options": "i"}},
                        {"grte": {"$regex": texto, "$options": "i"}}
                    ]
                }
                cursor = self.collection.find(query).limit(limit)
            
            servicios = list(cursor)
            
            for servicio in servicios:
                servicio["_id"] = str(servicio["_id"])
                
                # Asegurar que los nuevos campos existan
                servicio.setdefault("servicio", None)
                servicio.setdefault("cliente_destino", None)
                servicio.setdefault("grte", None)
            
            return servicios
            
        except Exception as e:
            print(f"Error en búsqueda por texto: {str(e)}")
            return []
    
    def obtener_servicios_pendientes_facturacion(
        self,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Obtiene servicios completados pero sin factura"""
        filtros = {
            "factura.estado": "PENDIENTE",
            "estado_servicio": {"$in": ["COMPLETADO", "PENDIENTE_FACTURACION"]}
        }
        
        return self.buscar_servicios(
            filtros=filtros,
            skip=skip,
            limit=limit
        )
    
    def exportar_servicios_excel(
        self,
        filtros: Dict[str, Any] = None
    ) -> BytesIO:
        """
        Exporta servicios a Excel con filtros opcionales (sin límite de registros)
        
        Args:
            filtros: Filtros para la búsqueda de servicios
            
        Returns:
            BytesIO con el archivo Excel generado
        """
        try:
            import pandas as pd
            from io import BytesIO
            
            query = self._construir_query(filtros) if filtros else {}
            
            cursor = self.collection.find(query).sort("fecha_servicio", -1)
            servicios = list(cursor)
            
            for servicio in servicios:
                servicio["_id"] = str(servicio["_id"])
            
            if not servicios:
                df = pd.DataFrame(columns=[
                    "ID", "Cliente", "Servicio","Tipo", "Proveedor", "Cliente Destino",
                    "GRTE", "Conductor", "Auxiliar", "Placa", "Tipo Camión",
                    "Capacidad M3", "Capacidad TN", "Origen", "Destino",
                    "Fecha Servicio", "Fecha Salida", "Mes", "Estado Servicio",
                    "Número Factura", "Fecha Emisión Factura", "Estado Factura",
                    "Monto Factura", "Moneda", "Cuenta", "Observaciones"
                ])
            else:
                excel_data = []
                for servicio in servicios:
                    factura = servicio.get("factura", {})
                    
                    def formatear_fecha(fecha):
                        if not fecha:
                            return ""
                        if isinstance(fecha, datetime):
                            return fecha.strftime("%Y-%m-%d")
                        if isinstance(fecha, str):
                            return fecha
                        return ""
                    
                    excel_data.append({
                        "ID": servicio.get("_id", ""),
                        "Cliente": servicio.get("cliente", ""),
                        "Servicio": servicio.get("servicio", ""),
                        "Tipo":servicio.get("tipo_servicio", ""),
                        "Proveedor": servicio.get("proveedor", ""),
                        "Cliente Destino": servicio.get("cliente_destino", ""),
                        "GRTE": servicio.get("grte", ""),
                        "Conductor": servicio.get("conductor", ""),
                        "Auxiliar": servicio.get("auxiliar", ""),
                        "Placa": servicio.get("placa", ""),
                        "Tipo Camión": servicio.get("tipo_camion", ""),
                        "Capacidad M3": servicio.get("capacidad_m3", ""),
                        "Capacidad TN": servicio.get("capacidad_tn", ""),
                        "Origen": servicio.get("origen", ""),
                        "Destino": servicio.get("destino", ""),
                        "Fecha Servicio": formatear_fecha(servicio.get("fecha_servicio")),
                        "Fecha Salida": formatear_fecha(servicio.get("fecha_salida")),
                        "Mes": servicio.get("mes", ""),
                        "Estado Servicio": servicio.get("estado_servicio", ""),
                        "Número Factura": factura.get("numero", "") if isinstance(factura, dict) else "",
                        "Fecha Emisión Factura": formatear_fecha(factura.get("fecha_emision")) if isinstance(factura, dict) else "",
                        "Estado Factura": factura.get("estado", "") if isinstance(factura, dict) else "",
                        "Monto Factura": factura.get("monto", "") if isinstance(factura, dict) else "",
                        "Moneda": factura.get("moneda", "PEN") if isinstance(factura, dict) else "PEN",
                        "Cuenta": servicio.get("cuenta", ""),
                        "Observaciones": servicio.get("observaciones", "")
                    })
                
                df = pd.DataFrame(excel_data)
            
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Servicios')
                
                worksheet = writer.sheets['Servicios']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(cell.value)
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            output.seek(0)
            return output
            
        except Exception as e:
            print(f"Error al exportar servicios a Excel: {str(e)}")
            raise 

    def estadisticas(self):
        """Método de ejemplo para obtener estadísticas"""
        pipeline = [
            {
                "$group": {
                    "_id": "$estado_servicio",
                    "total": {"$sum": 1}
                }
            }
        ]
        
        try:
            cursor = self.collection.aggregate(pipeline)
            resultado = list(cursor)
            return resultado
        except Exception as e:
            print(f"Error en método estadisticas: {str(e)}")
            return []            