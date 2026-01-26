import logging
from datetime import datetime, date, timedelta , time
from typing import Dict, Any, Optional, List
from pymongo.collection import Collection
from math import ceil

logger = logging.getLogger(__name__)

class GerenciaService:
    def __init__(self, db):
        self.db = db
        self.fletes_collection = db["fletes"]
        self.servicio_principal_collection = db["servicio_principal"]
        self.collection = db["facturacion_gestion"]  # Colección de gestiones
        self.facturas_collection = db["facturacion"]  # Colección de facturas
    
    def get_total_valorizado(
        self,
        nombre_cliente: Optional[str] = None,
        fecha_inicio: Optional[datetime] = None,
        fecha_fin: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Calcula el total de fletes VALORIZADOS (case-insensitive).
        """
        try:
            # 1. Construir pipeline base
            pipeline = []
            
            # 2. Condiciones del filtro base
            base_match = {
                "estado_flete": "VALORIZADO",
                "monto_flete": {"$gt": 0}
            }
            
            # 3. Determinar si necesitamos hacer lookup
            necesita_lookup = (nombre_cliente is not None or 
                              fecha_inicio is not None or 
                              fecha_fin is not None)
            
            if not necesita_lookup:
                # Caso 1: Solo contar todos los fletes valorizados
                pipeline.extend([
                    {"$match": base_match},
                    {"$group": {
                        "_id": None,
                        "total_general": {"$sum": {"$toDouble": "$monto_flete"}},
                        "total_fletes": {"$sum": 1}
                    }}
                ])
                
                resultado = list(self.fletes_collection.aggregate(pipeline))
                
                if not resultado:
                    return {
                        "total_general": 0.0,
                        "total_fletes": 0,
                        "cliente": "TODOS LOS CLIENTES",
                        "estado_calculado": "VALORIZADO",
                        "tipo_consulta": "GENERAL"
                    }
                
                return {
                    "total_general": float(resultado[0]["total_general"]),
                    "total_fletes": resultado[0]["total_fletes"],
                    "cliente": "TODOS LOS CLIENTES",
                    "estado_calculado": "VALORIZADO",
                    "tipo_consulta": "GENERAL"
                }
            
            # 4. Caso 2: Necesitamos filtrar (con lookup)
            # Agregar filtro base
            pipeline.append({"$match": base_match})
            
            # Agregar lookup
            pipeline.append({
                "$lookup": {
                    "from": "servicio_principal",
                    "let": {"servicio_id_str": "$servicio_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        {"$toString": "$_id"},
                                        "$$servicio_id_str"
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "info_servicio"
                }
            })
            
            # Desempaquetar
            pipeline.append({
                "$unwind": {
                    "path": "$info_servicio",
                    "preserveNullAndEmptyArrays": False
                }
            })
            
            # 5. Construir filtros dinámicos para servicio_principal (case-insensitive)
            match_filters = {}
            
            if nombre_cliente:
                # Búsqueda case-insensitive usando regex
                match_filters["info_servicio.cliente.nombre"] = {
                    "$regex": f"^{nombre_cliente}$",
                    "$options": "i"  # 'i' para case-insensitive
                }
            
            if fecha_inicio and fecha_fin:
                match_filters["info_servicio.fecha_servicio"] = {
                    "$gte": fecha_inicio,
                    "$lte": fecha_fin
                }
            elif fecha_inicio:  # Solo fecha inicio
                match_filters["info_servicio.fecha_servicio"] = {
                    "$gte": fecha_inicio
                }
            elif fecha_fin:  # Solo fecha fin
                match_filters["info_servicio.fecha_servicio"] = {
                    "$lte": fecha_fin
                }
            
            # Agregar filtro si existe
            if match_filters:
                pipeline.append({"$match": match_filters})
            
            # 6. Determinar la agrupación
            if nombre_cliente:
                # Agrupar por cliente específico
                pipeline.append({
                    "$group": {
                        "_id": "$info_servicio.cliente.nombre",
                        "monto_total": {"$sum": {"$toDouble": "$monto_flete"}},
                        "conteo": {"$sum": 1}
                    }
                })
            else:
                # Agrupar todos los clientes que cumplen los filtros de fecha
                pipeline.append({
                    "$group": {
                        "_id": None,
                        "monto_total": {"$sum": {"$toDouble": "$monto_flete"}},
                        "conteo": {"$sum": 1},
                        "clientes_distintos": {"$addToSet": "$info_servicio.cliente.nombre"}
                    }
                })
            
            # 7. Ejecutar pipeline
            resultado = list(self.fletes_collection.aggregate(pipeline))
            
            # 8. Formatear respuesta
            if not resultado:
                response = {
                    "total_vendido": 0.0,
                    "cantidad_fletes": 0,
                    "estado_calculado": "VALORIZADO"
                }
                
                if nombre_cliente:
                    response["cliente"] = nombre_cliente
                    response["tipo_consulta"] = "POR_CLIENTE"
                else:
                    response["cliente"] = "TODOS LOS CLIENTES"
                    response["tipo_consulta"] = "POR_FECHA"
                
                return response
            
            # 9. Formatear resultado según el tipo de consulta
            if nombre_cliente:
                # Obtener el nombre real del cliente (con el case correcto)
                nombre_real = resultado[0]["_id"]
                
                # Consulta por cliente
                return {
                    "cliente": nombre_real,
                    "total_vendido": float(resultado[0]["monto_total"]),
                    "cantidad_fletes": resultado[0]["conteo"],
                    "estado_calculado": "VALORIZADO",
                    "tipo_consulta": "POR_CLIENTE",
                    "cliente_buscado": nombre_cliente,  # Lo que se buscó
                    "cliente_encontrado": nombre_real   # Lo que se encontró
                }
            else:
                # Consulta por fecha (todos los clientes)
                return {
                    "cliente": "TODOS LOS CLIENTES",
                    "total_vendido": float(resultado[0]["monto_total"]),
                    "cantidad_fletes": resultado[0]["conteo"],
                    "clientes_distintos": len(resultado[0].get("clientes_distintos", [])),
                    "estado_calculado": "VALORIZADO",
                    "tipo_consulta": "POR_FECHA"
                }

        except Exception as e:
            logger.error(f"Error en cálculo de total valorizado: {str(e)}", exc_info=True)
            raise

    def get_kpis_completos(
        self,
        nombre_cliente: Optional[str] = None,
        fecha_inicio: Optional[datetime] = None,
        fecha_fin: Optional[datetime] = None,
        page: int = 1,
        page_size: int = 100
    ) -> dict:
        """
        Obtiene todos los KPIs financieros: total vendido, facturado, pagado, pendiente, etc.
        """
        try:
            # NUEVO
            hoy = datetime.combine(datetime.now().date(), time.min)
            # 1. Construir el filtro para facturacion_gestion (case-insensitive)
            query = {}
            
            if nombre_cliente:
                # Para facturacion_gestion, el cliente está en datos_completos.fletes[].servicio.nombre_cliente
                query["datos_completos.fletes.servicio.nombre_cliente"] = {
                    "$regex": f"^{nombre_cliente}$",
                    "$options": "i"
                }
            
            if fecha_inicio and fecha_fin:
                query["datos_completos.fecha_emision"] = {
                    "$gte": fecha_inicio,
                    "$lte": fecha_fin
                }
            elif fecha_inicio:
                query["datos_completos.fecha_emision"] = {"$gte": fecha_inicio}
            elif fecha_fin:
                query["datos_completos.fecha_emision"] = {"$lte": fecha_fin}
            
            skip = (page - 1) * page_size
            
            logger.info(f"Query para facturacion_gestion: {query}")

            # 2. CÁLCULO: Total Vendido (usando nuestra nueva función)
            total_vendido_result = self.get_total_valorizado(
                nombre_cliente=nombre_cliente,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
            
            total_vendido = total_vendido_result.get("total_vendido", 0)
            if total_vendido == 0 and "total_general" in total_vendido_result:
                total_vendido = total_vendido_result.get("total_general", 0)

            # 3. Pipeline para KPIs financieros de facturacion_gestion
            pipeline = [
                {"$match": query},
                {
                    "$facet": {
                        # Rama 1: Cálculo de Totales (KPIs)
                        "totales": [
                            {
                                "$group": {
                                    "_id": None,
                                    "total_facturado": {"$sum": {"$toDouble": "$datos_completos.monto_total"}},
                                    "total_pagado_acumulado": {"$sum": {"$toDouble": "$monto_pagado_acumulado"}},
                                    
                                    # CALCULO DEL SALDO PENDIENTE (Monto Neto - Pagado Acumulado)
                                    "total_pendiente_neto": {
                                        "$sum": {
                                            "$subtract": [
                                                {"$toDouble": "$monto_neto"},
                                                {"$toDouble": "$monto_pagado_acumulado"}
                                            ]
                                        }
                                    },
                                    # NUEVO: Lógica de Vencidos (Monto)
                                    "total_pendiente_vencido": {
                                        "$sum": {
                                            "$cond": [
                                                { "$and": [
                                                    { "$lte": ["$datos_completos.fecha_vencimiento", hoy] },
                                                    { "$ne": ["$estado_pago_neto", "Pagado"] },
                                                    { "$ne": ["$estado_pago_neto", "Anulado"] }
                                                ]},
                                                { "$subtract": [
                                                    {"$toDouble": "$monto_neto"},
                                                    {"$toDouble": "$monto_pagado_acumulado"}
                                                ]},
                                                0
                                            ]
                                        }
                                    },
                                    # NUEVO: Lógica de Vencidos (Conteo)
                                    "count_vencidas": {
                                        "$sum": {
                                            "$cond": [
                                                { "$and": [
                                                    { "$lte": ["$datos_completos.fecha_vencimiento", hoy] },
                                                    { "$ne": ["$estado_pago_neto", "Pagado"] },
                                                    { "$ne": ["$estado_pago_neto", "Anulado"] }
                                                ]},
                                                1, 0
                                            ]
                                        }
                                    },
                                    
                                    "total_detracciones": {"$sum": {"$toDouble": "$monto_detraccion"}},
                                    
                                    # Lógica de detracciones pagadas vs pendientes
                                    "total_pagado_detracc": {
                                        "$sum": {
                                            "$cond": [
                                                {"$and": [
                                                    {"$ne": ["$fecha_pago_detraccion", None]},
                                                    {"$ne": ["$fecha_pago_detraccion", ""]}
                                                ]},
                                                {"$toDouble": "$monto_detraccion"},
                                                0
                                            ]
                                        }
                                    },
                                    "total_pendiente_detracc": {
                                        "$sum": {
                                            "$cond": [
                                                {"$or": [
                                                    {"$eq": ["$fecha_pago_detraccion", None]},
                                                    {"$eq": ["$fecha_pago_detraccion", ""]}
                                                ]},
                                                {"$toDouble": "$monto_detraccion"},
                                                0
                                            ]
                                        }
                                    },
                                    
                                    # Conteo de documentos
                                    "total_facturas": {"$sum": 1},
                                    "total_fletes_incluidos": {"$sum": {"$size": "$datos_completos.fletes"}}
                                }
                            }
                        ],
                        # Rama 2: Resultados Paginados
                        "resultados": [
                            {"$sort": {"_id": -1}},
                            {"$skip": skip},
                            {"$limit": page_size}
                        ],
                        # Rama 3: Conteo Total
                        "conteo": [{"$count": "total"}]
                    }
                }
            ]

            # 4. Ejecutar pipeline para KPIs
            result_kpis = list(self.collection.aggregate(pipeline))
            
            if not result_kpis:
                # Si no hay resultados, retornar valores por defecto
                totales_data = {}
                items_raw = []
                total_docs = 0
            else:
                result = result_kpis[0]
                totales_data = result["totales"][0] if result["totales"] else {}
                items_raw = result["resultados"]
                total_docs = result["conteo"][0]["total"] if result["conteo"] else 0
            
            # 5. Enriquecer cada gestión con datos adicionales
            items_enriquecidos = []
            for item in items_raw:
                item_enriquecido = self._format_gestion_response(item)
                
                # Intentar obtener información adicional del primer flete
                if item.get("datos_completos", {}).get("fletes") and len(item["datos_completos"]["fletes"]) > 0:
                    primer_flete = item["datos_completos"]["fletes"][0]
                    codigo_flete = primer_flete.get("codigo_flete")
                    
                    if codigo_flete:
                        # Buscar información del flete en la colección fletes
                        flete_info = self.fletes_collection.find_one({"codigo_flete": codigo_flete})
                        if flete_info:
                            item_enriquecido["flete_info"] = {
                                "estado_flete": flete_info.get("estado_flete"),
                                "servicio_id": flete_info.get("servicio_id"),
                                "fecha_creacion": flete_info.get("fecha_creacion"),
                                "fecha_actualizacion": flete_info.get("fecha_actualizacion")
                            }
                        
                        # Buscar información del servicio en servicio_principal
                        if flete_info and flete_info.get("servicio_id"):
                            try:
                                from bson import ObjectId
                                servicio_info = self.servicio_principal_collection.find_one(
                                    {"_id": ObjectId(flete_info["servicio_id"])}
                                )
                                if servicio_info:
                                    item_enriquecido["servicio_detallado"] = {
                                        "codigo_servicio_principal": servicio_info.get("codigo_servicio_principal"),
                                        "tipo_servicio": servicio_info.get("tipo_servicio"),
                                        "modalidad_servicio": servicio_info.get("modalidad_servicio"),
                                        "zona": servicio_info.get("zona"),
                                        "m3": servicio_info.get("m3"),
                                        "tn": servicio_info.get("tn"),
                                        "origen": servicio_info.get("origen"),
                                        "destino": servicio_info.get("destino"),
                                        "cliente_detalle": servicio_info.get("cliente"),
                                        "proveedor_detalle": servicio_info.get("proveedor"),
                                        "flota_detalle": servicio_info.get("flota"),
                                        "conductor_detalle": servicio_info.get("conductor", []),
                                        "auxiliar_detalle": servicio_info.get("auxiliar", [])
                                    }
                            except Exception as e:
                                logger.warning(f"No se pudo obtener servicio detallado: {str(e)}")
                
                items_enriquecidos.append(item_enriquecido)
            
            total_pages = ceil(total_docs / page_size) if page_size > 0 else 0

            # 6. Obtener información del cliente real (si se buscó por cliente)
            cliente_real = None
            if nombre_cliente and "cliente_encontrado" in total_vendido_result:
                cliente_real = total_vendido_result["cliente_encontrado"]
            elif nombre_cliente and "cliente" in total_vendido_result:
                cliente_real = total_vendido_result["cliente"]

            return {
                "summary": {
                    # Total Vendido (calculado por separado)
                    "total_vendido": float(total_vendido),
                    "cantidad_fletes_vendidos": total_vendido_result.get("cantidad_fletes", 0),
                    
                    # KPIs financieros (de facturacion_gestion)
                    "total_facturado": float(totales_data.get("total_facturado", 0)),
                    "total_pagado": float(totales_data.get("total_pagado_acumulado", 0)),
                    "total_pendiente": float(totales_data.get("total_pendiente_neto", 0)),
                    "total_detracciones": float(totales_data.get("total_detracciones", 0)),
                    "total_pagado_detracc": float(totales_data.get("total_pagado_detracc", 0)),
                    "total_pendiente_detracc": float(totales_data.get("total_pendiente_detracc", 0)),

                    # NUEVO: KPIs de Vencimiento
                    "total_pendiente_vencido": float(totales_data.get("total_pendiente_vencido", 0)),
                    "cantidad_vencidas": totales_data.get("count_vencidas", 0),
                    
                    # Métricas adicionales
                    "total_facturas": totales_data.get("total_facturas", 0),
                    "total_fletes_incluidos": totales_data.get("total_fletes_incluidos", 0),
                    
                    # Información del filtro
                    "cliente_buscado": nombre_cliente,
                    "cliente_encontrado": cliente_real if cliente_real else nombre_cliente,
                    "rango_fechas": {
                        "inicio": fecha_inicio.isoformat() if fecha_inicio else None,
                        "fin": fecha_fin.isoformat() if fecha_fin else None
                    }
                },
                "items": items_enriquecidos,
                "pagination": {
                    "total": total_docs,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1
                }
            }

        except Exception as e:
            logger.error(f"Error al obtener KPIs completos: {str(e)}", exc_info=True)
            raise


    def _format_gestion_response(self, gestion: dict) -> dict:
        """
        Formatea la respuesta de una gestión de facturación con datos enriquecidos.
        """
        try:
            datos_completos = gestion.get("datos_completos", {})
            fletes = datos_completos.get("fletes", [])
            
            # Obtener información del primer flete (si existe)
            primer_flete = fletes[0] if fletes else {}
            servicio_flete = primer_flete.get("servicio", {}) if primer_flete else {}
            
            # Extraer información de todos los fletes
            info_fletes = []
            for flete in fletes:
                info_fletes.append({
                    "codigo_flete": flete.get("codigo_flete"),
                    "monto_flete": flete.get("monto_flete"),
                    "cliente": flete.get("servicio", {}).get("nombre_cliente"),
                    "proveedor": flete.get("servicio", {}).get("nombre_proveedor"),
                    "conductor": flete.get("servicio", {}).get("nombre_conductor"),
                    "placa": flete.get("servicio", {}).get("placa_flota"),
                    "fecha_servicio": flete.get("servicio", {}).get("fecha_servicio"),
                    "tipo_servicio": flete.get("servicio", {}).get("tipo_servicio")
                })
            
            # Formatear respuesta completa
            return {
                # Información básica
                "id": str(gestion.get("_id", "")),
                "codigo_factura": gestion.get("codigo_factura", ""),
                "numero_factura": datos_completos.get("numero_factura", ""),
                
                # Datos financieros
                "financiero": {
                    "monto_total": datos_completos.get("monto_total", 0),
                    "monto_neto": gestion.get("monto_neto", 0),
                    "monto_pagado_acumulado": gestion.get("monto_pagado_acumulado", 0),
                    "saldo_pendiente": gestion.get("monto_neto", 0) - gestion.get("monto_pagado_acumulado", 0),
                    "tasa_detraccion": gestion.get("tasa_detraccion", 0),
                    "monto_detraccion": gestion.get("monto_detraccion", 0),
                    "monto_pagado_detraccion": gestion.get("fecha_pago_detraccion") is not None and gestion.get("fecha_pago_detraccion") != "" 
                                              and gestion.get("monto_detraccion") or 0,
                    "monto_pendiente_detraccion": gestion.get("fecha_pago_detraccion") is None or gestion.get("fecha_pago_detraccion") == ""
                                                 and gestion.get("monto_detraccion") or 0
                },
                
                # Estados
                "estados": {
                    "estado_detraccion": gestion.get("estado_detraccion", ""),
                    "estado_pago_neto": gestion.get("estado_pago_neto", ""),
                    "prioridad": gestion.get("prioridad", "")
                },
                
                # Información del servicio (del primer flete)
                "servicio": {
                    "cliente": servicio_flete.get("nombre_cliente", ""),
                    "cuenta": servicio_flete.get("nombre_cuenta", ""),
                    "proveedor": servicio_flete.get("nombre_proveedor", ""),
                    "conductor": servicio_flete.get("nombre_conductor", ""),
                    "auxiliar": servicio_flete.get("nombre_auxiliar", ""),
                    "placa_flota": servicio_flete.get("placa_flota", ""),
                    "tipo_servicio": servicio_flete.get("tipo_servicio", ""),
                    "modalidad": servicio_flete.get("modalidad", ""),
                    "zona": servicio_flete.get("zona", ""),
                    "m3": servicio_flete.get("m3", ""),
                    "tn": servicio_flete.get("tn", ""),
                    "origen": servicio_flete.get("origen", ""),
                    "destino": servicio_flete.get("destino", ""),
                    "gia_rr": servicio_flete.get("gia_rr", ""),
                    "gia_rt": servicio_flete.get("gia_rt", "")
                },
                
                # Información de todos los fletes incluidos
                "fletes_incluidos": {
                    "cantidad": len(fletes),
                    "detalles": info_fletes,
                    "codigos_fletes": [f.get("codigo_flete") for f in fletes if f.get("codigo_flete")]
                },
                
                # Fechas importantes
                "fechas": {
                    "emision": datos_completos.get("fecha_emision"),
                    "vencimiento": datos_completos.get("fecha_vencimiento"),
                    "pago_detraccion": gestion.get("fecha_pago_detraccion"),
                    "probable_pago": gestion.get("fecha_probable_pago"),
                    "ultima_actualizacion": gestion.get("ultima_actualizacion")
                },
                
                # Información adicional
                "informacion_adicional": {
                    "nro_constancia_detraccion": gestion.get("nro_constancia_detraccion"),
                    "banco_destino": gestion.get("banco_destino"),
                    "cuenta_bancaria_destino": gestion.get("cuenta_bancaria_destino"),
                    "nro_operacion_pago_neto": gestion.get("nro_operacion_pago_neto"),
                    "centro_costo": gestion.get("centro_costo"),
                    "responsable_gestion": gestion.get("responsable_gestion"),
                    "observaciones_admin": gestion.get("observaciones_admin")
                },
                
                # Datos originales (opcional, para depuración)
                "_original": {
                    "datos_completos_keys": list(datos_completos.keys()) if datos_completos else [],
                    "fletes_count": len(fletes)
                } if logger.isEnabledFor(logging.DEBUG) else None
            }
            
        except Exception as e:
            logger.error(f"Error al formatear gestión {gestion.get('_id', '')}: {str(e)}")
            # Retornar estructura básica en caso de error
            return {
                "id": str(gestion.get("_id", "")),
                "codigo_factura": gestion.get("codigo_factura", ""),
                "error": f"Error al formatear: {str(e)}",
                "raw_data": gestion if logger.isEnabledFor(logging.DEBUG) else None
            }

    # Mantener función original para compatibilidad
    def get_total_valorizado_cliente(
        self,
        nombre_cliente: str,
        fecha_inicio: datetime,
        fecha_fin: datetime
    ) -> Dict[str, Any]:
        """
        Versión anterior mantenida para compatibilidad.
        """
        return self.get_total_valorizado(
            nombre_cliente=nombre_cliente,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin
        )
        
    def get_resumen_por_placa(
        self,
        placa: Optional[str] = None,
        fecha_inicio: Optional[datetime] = None,
        fecha_fin: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Obtiene resumen de servicios agrupados por placa de flota.
        Filtra por placa (opcional) y rango de fechas (opcional).
        
        Retorna:
        - PLACA | TOTAL DE SERVICIOS | TOTAL VENDIDO
        """
        try:
            # Construir pipeline de agregación
            pipeline = []
            
            # 1. Match inicial en la colección de fletes (solo valorizados)
            match_fletes = {
                "estado_flete": "VALORIZADO",
                "monto_flete": {"$gt": 0}
            }
            
            pipeline.append({"$match": match_fletes})
            
            # 2. Lookup para obtener información del servicio
            pipeline.append({
                "$lookup": {
                    "from": "servicio_principal",
                    "let": {"servicio_id_str": "$servicio_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        {"$toString": "$_id"},
                                        "$$servicio_id_str"
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "info_servicio"
                }
            })
            
            # 3. Desempaquetar el servicio
            pipeline.append({
                "$unwind": {
                    "path": "$info_servicio",
                    "preserveNullAndEmptyArrays": False
                }
            })
            
            # 4. Construir filtros dinámicos
            match_filters = {}
            
            # Filtro por placa (case-insensitive)
            if placa:
                match_filters["info_servicio.flota.placa"] = {
                    "$regex": f"^{placa}$",
                    "$options": "i"
                }
            
            # Filtro por rango de fechas
            if fecha_inicio and fecha_fin:
                match_filters["info_servicio.fecha_servicio"] = {
                    "$gte": fecha_inicio,
                    "$lte": fecha_fin
                }
            elif fecha_inicio:
                match_filters["info_servicio.fecha_servicio"] = {
                    "$gte": fecha_inicio
                }
            elif fecha_fin:
                match_filters["info_servicio.fecha_servicio"] = {
                    "$lte": fecha_fin
                }
            
            # Aplicar filtros si existen
            if match_filters:
                pipeline.append({"$match": match_filters})
            
            # 5. Agrupar por placa
            pipeline.append({
                "$group": {
                    "_id": "$info_servicio.flota.placa",
                    "total_servicios": {"$sum": 1},
                    "total_vendido": {"$sum": {"$toDouble": "$monto_flete"}},
                    "codigos_servicio": {"$addToSet": "$info_servicio.codigo_servicio_principal"},
                    "codigos_flete": {"$addToSet": "$codigo_flete"}
                }
            })
            
            # 6. Ordenar por placa
            pipeline.append({
                "$sort": {"_id": 1}
            })
            
            # 7. Proyectar el formato final
            pipeline.append({
                "$project": {
                    "_id": 0,
                    "placa": "$_id",
                    "total_servicios": 1,
                    "total_vendido": 1,
                    "codigos_servicio": 1,
                    "codigos_flete": 1
                }
            })
            
            # 8. Ejecutar pipeline
            logger.info(f"Pipeline resumen por placa: {pipeline}")
            resultados = list(self.fletes_collection.aggregate(pipeline))
            
            # 9. Calcular totales generales
            total_general_servicios = sum(r["total_servicios"] for r in resultados)
            total_general_vendido = sum(r["total_vendido"] for r in resultados)
            total_placas = len(resultados)
            
            # 10. Formatear respuesta
            return {
                "resumen": {
                    "total_placas": total_placas,
                    "total_servicios": total_general_servicios,
                    "total_vendido": float(total_general_vendido)
                },
                "filtros_aplicados": {
                    "placa": placa,
                    "fecha_inicio": fecha_inicio.isoformat() if fecha_inicio else None,
                    "fecha_fin": fecha_fin.isoformat() if fecha_fin else None
                },
                "detalle_por_placa": [
                    {
                        "placa": r["placa"],
                        "total_servicios": r["total_servicios"],
                        "total_vendido": float(r["total_vendido"]),
                        "cantidad_servicios_distintos": len(r.get("codigos_servicio", [])),
                        "cantidad_fletes": len(r.get("codigos_flete", []))
                    }
                    for r in resultados
                ]
            }
            
        except Exception as e:
            logger.error(f"Error al obtener resumen por placa: {str(e)}", exc_info=True)
            raise

    def get_resumen_por_proveedor(
        self,
        nombre_proveedor: Optional[str] = None,
        fecha_inicio: Optional[datetime] = None,
        fecha_fin: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Obtiene resumen de servicios agrupados por proveedor.
        Filtra por proveedor (opcional) y rango de fechas (opcional).
        
        Retorna:
        - PROVEEDOR | TOTAL DE SERVICIOS | TOTAL VENDIDO
        """
        try:
            # Construir pipeline de agregación
            pipeline = []
            
            # 1. Match inicial en la colección de fletes (solo valorizados)
            match_fletes = {
                "estado_flete": "VALORIZADO",
                "monto_flete": {"$gt": 0}
            }
            
            pipeline.append({"$match": match_fletes})
            
            # 2. Lookup para obtener información del servicio
            pipeline.append({
                "$lookup": {
                    "from": "servicio_principal",
                    "let": {"servicio_id_str": "$servicio_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        {"$toString": "$_id"},
                                        "$$servicio_id_str"
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "info_servicio"
                }
            })
            
            # 3. Desempaquetar el servicio
            pipeline.append({
                "$unwind": {
                    "path": "$info_servicio",
                    "preserveNullAndEmptyArrays": False
                }
            })
            
            # 4. Construir filtros dinámicos
            match_filters = {}
            
            # Filtro por proveedor (case-insensitive)
            if nombre_proveedor:
                match_filters["info_servicio.proveedor.nombre"] = {
                    "$regex": f"^{nombre_proveedor}$",
                    "$options": "i"
                }
            
            # Filtro por rango de fechas
            if fecha_inicio and fecha_fin:
                match_filters["info_servicio.fecha_servicio"] = {
                    "$gte": fecha_inicio,
                    "$lte": fecha_fin
                }
            elif fecha_inicio:
                match_filters["info_servicio.fecha_servicio"] = {
                    "$gte": fecha_inicio
                }
            elif fecha_fin:
                match_filters["info_servicio.fecha_servicio"] = {
                    "$lte": fecha_fin
                }
            
            # Aplicar filtros si existen
            if match_filters:
                pipeline.append({"$match": match_filters})
            
            # 5. Agrupar por proveedor
            pipeline.append({
                "$group": {
                    "_id": "$info_servicio.proveedor.nombre",
                    "total_servicios": {"$sum": 1},
                    "total_vendido": {"$sum": {"$toDouble": "$monto_flete"}},
                    "codigos_servicio": {"$addToSet": "$info_servicio.codigo_servicio_principal"},
                    "codigos_flete": {"$addToSet": "$codigo_flete"},
                    # Información adicional del proveedor
                    "razon_social": {"$first": "$info_servicio.proveedor.razon_social"},
                    "ruc": {"$first": "$info_servicio.proveedor.ruc"}
                }
            })
            
            # 6. Ordenar por nombre de proveedor
            pipeline.append({
                "$sort": {"_id": 1}
            })
            
            # 7. Proyectar el formato final
            pipeline.append({
                "$project": {
                    "_id": 0,
                    "proveedor": "$_id",
                    "razon_social": 1,
                    "ruc": 1,
                    "total_servicios": 1,
                    "total_vendido": 1,
                    "codigos_servicio": 1,
                    "codigos_flete": 1
                }
            })
            
            # 8. Ejecutar pipeline
            logger.info(f"Pipeline resumen por proveedor: {pipeline}")
            resultados = list(self.fletes_collection.aggregate(pipeline))
            
            # 9. Calcular totales generales
            total_general_servicios = sum(r["total_servicios"] for r in resultados)
            total_general_vendido = sum(r["total_vendido"] for r in resultados)
            total_proveedores = len(resultados)
            
            # 10. Formatear respuesta
            return {
                "resumen": {
                    "total_proveedores": total_proveedores,
                    "total_servicios": total_general_servicios,
                    "total_vendido": float(total_general_vendido)
                },
                "filtros_aplicados": {
                    "proveedor": nombre_proveedor,
                    "fecha_inicio": fecha_inicio.isoformat() if fecha_inicio else None,
                    "fecha_fin": fecha_fin.isoformat() if fecha_fin else None
                },
                "detalle_por_proveedor": [
                    {
                        "proveedor": r["proveedor"],
                        "razon_social": r.get("razon_social", ""),
                        "ruc": r.get("ruc", ""),
                        "total_servicios": r["total_servicios"],
                        "total_vendido": float(r["total_vendido"]),
                        "cantidad_servicios_distintos": len(r.get("codigos_servicio", [])),
                        "cantidad_fletes": len(r.get("codigos_flete", []))
                    }
                    for r in resultados
                ]
            }
            
        except Exception as e:
            logger.error(f"Error al obtener resumen por proveedor: {str(e)}", exc_info=True)
            raise
        