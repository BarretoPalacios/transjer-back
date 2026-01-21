from typing import List, Optional, Dict, Any, Tuple
from enum import Enum
from bson import ObjectId
from decimal import Decimal
from datetime import datetime, date, timedelta , time
from app.core.database import get_database
from app.modules.seguimiento_facturas.model import FacturacionGestion
from app.modules.seguimiento_facturas.schema import (
    FacturacionGestionCreate,
    FacturacionGestionUpdate,
    FacturacionGestionFilter,
    FacturacionGestionResponse,
    EstadoPagoNeto,
    EstadoDetraccion,
    PrioridadPago
)
import pandas as pd
from io import BytesIO
import logging
# from math import ceil
from math import ceil
import re

logger = logging.getLogger(__name__)

class FacturacionGestionService:
    def __init__(self, db):
        self.db = db
        self.collection = db["facturacion_gestion"]
        self.facturas_collection = db["facturacion"]
        self.fletes_collection = db["fletes"]
        self.servicios_collection = db["servicio_principal"]
    
    def create_gestion(self, gestion_data: dict) -> dict:
        """Crear nueva gestión de facturación"""
        try:
            # 1️⃣ Verificar que la factura existe
            factura = self.facturas_collection.find_one(
                {"codigo_factura": gestion_data["codigo_factura"]}
            )
            if not factura:
                raise ValueError(
                    f"La factura {gestion_data['codigo_factura']} no existe"
                )
            
            # 2️⃣ Verificar que no existe gestión para esta factura
            existing_gestion = self.collection.find_one({
                "codigo_factura": gestion_data["codigo_factura"]
            })
            if existing_gestion:
                raise ValueError(
                    f"Ya existe gestión para la factura {gestion_data['codigo_factura']}"
                )
            
            # 3️⃣ Validar montos
            if gestion_data.get("monto_neto", Decimal("0")) <= Decimal("0"):
                raise ValueError("El monto neto debe ser mayor a cero")
            
            # Calcular detracción si no viene especificada
            if gestion_data.get("monto_detraccion") is None:
                tasa = gestion_data.get("tasa_detraccion", Decimal("4.0"))
                monto_neto = gestion_data.get("monto_neto", Decimal("0"))
                gestion_data["monto_detraccion"] = (monto_neto * tasa) / Decimal("100")
            
            # 4️⃣ Establecer estado de detracción basado en monto
            if gestion_data.get("monto_detraccion", Decimal("0")) < Decimal("400"):
                gestion_data["estado_detraccion"] = EstadoDetraccion.NO_APLICA
            
            # 5️⃣ Crear modelo
            gestion_data["ultima_actualizacion"] = datetime.now()
            gestion_model = FacturacionGestion(**gestion_data)
            
            # 6️⃣ Insertar en base de datos
            result = self.collection.insert_one(
                gestion_model.model_dump(by_alias=True)
            )
            
            # 7️⃣ Retornar creado
            created_gestion = self.collection.find_one(
                {"_id": result.inserted_id}
            )
            return self._format_gestion_response(created_gestion)
            
        except Exception as e:
            logger.error(f"Error al crear gestión de facturación: {str(e)}")
            raise
    
    def get_gestion_by_id(self, gestion_id: str) -> Optional[dict]:
        """Obtener gestión por ID"""
        try:
            if not ObjectId.is_valid(gestion_id):
                return None
            
            gestion = self.collection.find_one({"_id": ObjectId(gestion_id)})
            if gestion:
                return self._format_gestion_response(gestion)
            return None
            
        except Exception as e:
            logger.error(f"Error al obtener gestión: {str(e)}")
            return None
    
    
    def get_gestion_by_codigo_factura(self, codigo_factura: str) -> Optional[dict]:
        """Obtener gestión por código de factura"""
        try:
            gestion = self.collection.find_one({"codigo_factura": codigo_factura})
            if gestion:
                return self._format_gestion_response(gestion)
            return None
            
        except Exception as e:
            logger.error(f"Error al obtener gestión por código de factura: {str(e)}")
            return None
    
    def get_all_gestiones(
        self,
        filter_params: Optional[FacturacionGestionFilter] = None,
        page: int = 1,
        page_size: int = 10
    ) -> dict:
        """Obtener todas las gestiones con filtros extendidos y paginación"""
        try:
            query = self._build_query(filter_params)
            
            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size
            
            gestiones = list(
                self.collection.find(query)
                .sort("_id", -1)
                .skip(skip)
                .limit(page_size)
            )
            
            formatted_gestiones = [self._format_gestion_response(g) for g in gestiones]
            
            total_pages = ceil(total / page_size) if page_size > 0 else 0
            
            return {
                "items": formatted_gestiones,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
            
        except Exception as e:
            logger.error(f"Error al obtener gestiones: {str(e)}")
            raise
    
    def _build_query(self, filter_params: Optional[FacturacionGestionFilter]) -> dict:
        """Construir query de MongoDB con todos los filtros"""
        query = {}
        
        if not filter_params:
            return query
        
        # Filtros básicos
        if filter_params.codigo_factura:
            query["codigo_factura"] = safe_regex(filter_params.codigo_factura)
        
        if filter_params.numero_factura:
            query["datos_completos.numero_factura"] = safe_regex(filter_params.numero_factura)
        
        # Estados y prioridad
        if filter_params.estado_detraccion:
            query["estado_detraccion"] = filter_params.estado_detraccion
        
        if filter_params.estado_pago_neto:
            query["estado_pago_neto"] = filter_params.estado_pago_neto
        
        if filter_params.prioridad:
            query["prioridad"] = filter_params.prioridad
        
        # Gestión administrativa
        if filter_params.centro_costo:
            query["centro_costo"] = safe_regex(filter_params.centro_costo)
        
        if filter_params.responsable_gestion:
            query["responsable_gestion"] = safe_regex(filter_params.responsable_gestion)
        
        # Filtros de fechas - Probable pago
        self._add_date_range_filter(
            query, "fecha_probable_pago",
            filter_params.fecha_probable_inicio,
            filter_params.fecha_probable_fin
        )
        
        # Filtros de fechas - Emisión factura
        self._add_date_range_filter(
            query, "datos_completos.fecha_emision",
            filter_params.fecha_emision_inicio,
            filter_params.fecha_emision_fin
        )
        
        # Filtros de fechas - Vencimiento factura
        self._add_date_range_filter(
            query, "datos_completos.fecha_vencimiento",
            filter_params.fecha_vencimiento_inicio,
            filter_params.fecha_vencimiento_fin
        )
        
        # Filtros de fechas - Servicio
        self._add_date_range_filter(
            query, "datos_completos.fletes.servicio.fecha_servicio",
            filter_params.fecha_servicio_inicio,
            filter_params.fecha_servicio_fin
        )
        
        # Filtros de fechas - Pago detracción
        self._add_date_range_filter(
            query, "fecha_pago_detraccion",
            filter_params.fecha_pago_detraccion_inicio,
            filter_params.fecha_pago_detraccion_fin
        )
        
        # Filtros basados en snapshots - Datos principales
        if filter_params.nombre_cliente:
            query["datos_completos.fletes.servicio.nombre_cliente"] = safe_regex(filter_params.nombre_cliente)
        
        if filter_params.nombre_cuenta:
            query["datos_completos.fletes.servicio.nombre_cuenta"] = safe_regex(filter_params.nombre_cuenta)
        
        if filter_params.nombre_proveedor:
            query["datos_completos.fletes.servicio.nombre_proveedor"] = safe_regex(filter_params.nombre_proveedor)
        
        # Filtros de flota y personal
        if filter_params.placa_flota:
            query["datos_completos.fletes.servicio.placa_flota"] = safe_regex(filter_params.placa_flota)
        
        if filter_params.nombre_conductor:
            query["datos_completos.fletes.servicio.nombre_conductor"] = safe_regex(filter_params.nombre_conductor)
        
        if filter_params.nombre_auxiliar:
            query["datos_completos.fletes.servicio.nombre_auxiliar"] = safe_regex(filter_params.nombre_auxiliar)
        
        # Filtros de servicio
        if filter_params.tipo_servicio:
            query["datos_completos.fletes.servicio.tipo_servicio"] = safe_regex(filter_params.tipo_servicio)
        
        if filter_params.modalidad:
            query["datos_completos.fletes.servicio.modalidad"] = safe_regex(filter_params.modalidad)
        
        if filter_params.zona:
            query["datos_completos.fletes.servicio.zona"] = safe_regex(filter_params.zona)
        
        if filter_params.origen:
            query["datos_completos.fletes.servicio.origen"] = safe_regex(filter_params.origen)
        
        if filter_params.destino:
            query["datos_completos.fletes.servicio.destino"] = safe_regex(filter_params.destino)
        
        # Filtros de montos con rangos
        self._add_decimal_range_filter(
            query, "datos_completos.monto_total",
            filter_params.monto_total_min,
            filter_params.monto_total_max
        )
        
        self._add_decimal_range_filter(
            query, "monto_neto",
            filter_params.monto_neto_min,
            filter_params.monto_neto_max
        )
        
        self._add_decimal_range_filter(
            query, "monto_detraccion",
            filter_params.monto_detraccion_min,
            filter_params.monto_detraccion_max
        )
        
        # Filtros de GIA
        if filter_params.gia_rr:
            query["datos_completos.fletes.servicio.gia_rr"] = safe_regex(filter_params.gia_rr)
        
        if filter_params.gia_rt:
            query["datos_completos.fletes.servicio.gia_rt"] = safe_regex(filter_params.gia_rt)
        
        # Filtro de saldo pendiente
        if filter_params.tiene_saldo_pendiente is not None:
            if filter_params.tiene_saldo_pendiente:
                query["$expr"] = {"$gt": [
                    {"$subtract": ["$monto_neto", "$monto_pagado_acumulado"]},
                    0
                ]}
            else:
                query["$expr"] = {"$eq": [
                    {"$subtract": ["$monto_neto", "$monto_pagado_acumulado"]},
                    0
                ]}
        
        # Rangos de saldo pendiente (requiere aggregation)
        if filter_params.saldo_pendiente_min or filter_params.saldo_pendiente_max:
            saldo_conditions = []
            if filter_params.saldo_pendiente_min:
                saldo_conditions.append({
                    "$gte": [
                        {"$subtract": ["$monto_neto", "$monto_pagado_acumulado"]},
                        float(filter_params.saldo_pendiente_min)
                    ]
                })
            if filter_params.saldo_pendiente_max:
                saldo_conditions.append({
                    "$lte": [
                        {"$subtract": ["$monto_neto", "$monto_pagado_acumulado"]},
                        float(filter_params.saldo_pendiente_max)
                    ]
                })
            
            if saldo_conditions:
                if "$expr" in query:
                    query["$and"] = [
                        {"$expr": query["$expr"]},
                        {"$expr": {"$and": saldo_conditions}}
                    ]
                    del query["$expr"]
                else:
                    query["$expr"] = {"$and": saldo_conditions}
        
        # Búsqueda general (search)
        if filter_params.search:
            search_conditions = [
                {"codigo_factura": safe_regex(filter_params.search)},
                {"datos_completos.numero_factura": safe_regex(filter_params.search)},
                {"datos_completos.fletes.servicio.nombre_cliente": safe_regex(filter_params.search)},
                {"datos_completos.fletes.servicio.nombre_cuenta": safe_regex(filter_params.search)},
                {"datos_completos.fletes.servicio.nombre_proveedor": safe_regex(filter_params.search)},
                {"datos_completos.fletes.servicio.placa_flota": safe_regex(filter_params.search)},
                {"datos_completos.fletes.servicio.nombre_conductor": safe_regex(filter_params.search)},
                {"responsable_gestion": safe_regex(filter_params.search)},
                {"observaciones_admin": safe_regex(filter_params.search)}
            ]
            query["$or"] = search_conditions
        
        return query
    
    def _add_date_range_filter(self, query: dict, field: str, inicio: Optional[date], fin: Optional[date]):
        """Agregar filtro de rango de fechas"""
        if inicio or fin:
            date_query = {}
            if inicio:
                # Convertir date a datetime
                inicio_dt = datetime.combine(inicio, datetime.min.time()) if isinstance(inicio, date) and not isinstance(inicio, datetime) else inicio
                date_query["$gte"] = inicio_dt
            if fin:
                # Convertir date a datetime (incluir todo el día)
                fin_dt = datetime.combine(fin, datetime.max.time()) if isinstance(fin, date) and not isinstance(fin, datetime) else fin
                date_query["$lte"] = fin_dt
            if date_query:
                query[field] = date_query
    
    def _add_decimal_range_filter(self, query: dict, field: str, min_val: Optional[Decimal], max_val: Optional[Decimal]):
        """Agregar filtro de rango de valores decimales"""
        if min_val or max_val:
            range_query = {}
            if min_val:
                range_query["$gte"] = float(min_val)
            if max_val:
                range_query["$lte"] = float(max_val)
            if range_query:
                query[field] = range_query
    
    def update_gestion(self, gestion_id: str, update_data: dict) -> Optional[dict]:
        try:
            if not ObjectId.is_valid(gestion_id):
                return None
            
            gestion_actual = self.get_gestion_by_id(gestion_id)
            if not gestion_actual:
                return None
            
            if gestion_actual.get("estado_pago_neto") == "ANULADO":
                raise Exception("No se permiten modificaciones en facturas anuladas")
            
            update_dict = {k: v for k, v in update_data.items() if v is not None}
            if not update_dict:
                return gestion_actual

            monto_neto = Decimal(str(gestion_actual["monto_neto"]))

            # Lógica solicitada: Prioridad al estado seleccionado
            if "estado_pago_neto" in update_dict:
                estado = update_dict["estado_pago_neto"]
                if estado == EstadoPagoNeto.PAGADO:
                    update_dict["monto_pagado_acumulado"] = monto_neto
                elif estado == EstadoPagoNeto.PENDIENTE:
                    update_dict["monto_pagado_acumulado"] = Decimal("0")
                elif estado == EstadoPagoNeto.ANULADO:
                    # LÓGICA DE ANULACIÓN
                    update_dict["monto_pagado_acumulado"] = Decimal("0")
                    # Opcional: Si manejas un campo de 'valor_contable' o 'monto_neto' 
                    # podrías decidir si lo pones en 0 o lo dejas para histórico.
                    update_dict["monto_neto"] = Decimal("0")
                    # Resetear estado de detracción si existe
                    update_dict["estado_detraccion"] = EstadoDetraccion.NO_APLICA # O el valor que uses
                    update_dict["fecha_pago_detraccion"] = None
                    update_dict["fecha_probable_pago"] = None
                    update_dict["tasa_detraccion"] = Decimal("0")
                    update_dict["monto_detraccion"] = Decimal("0")
                    
                    numero_factura_comercial = gestion_actual.get("codigo_factura")
                    # obs_actuales = gestion_actual.get("observaciones") or ""
                    # update_dict["observaciones"] = f"{obs_actuales} [Factura ANULADA - Fletes Liberados]".strip()

                    if numero_factura_comercial:
                        factura_doc = self.facturas_collection.find_one({"numero_factura": numero_factura_comercial})
                        
                        if factura_doc:
                            codigo_factura_interno = factura_doc.get("codigo_factura")
                            
                            # Actualizar Factura Base
                            self.facturas_collection.update_one(
                                {"numero_factura": numero_factura_comercial},
                                {"$set": {
                                    "estado": "Anulada",
                                    "fecha_actualizacion": datetime.now()
                                }}
                            )

                            # Liberar Fletes usando el código interno (FAC-...)
                            if codigo_factura_interno:
                                self.fletes_collection.update_many(
                                    {"codigo_factura": codigo_factura_interno},
                                    {"$set": {
                                        "pertenece_a_factura": False,
                                        "factura_id": None,
                                        "codigo_factura": None,
                                        "estado_flete": "VALORIZADO",
                                        "fecha_actualizacion": datetime.now()
                                    }}
                                )
                    

            
            # Lógica secundaria: Si se envía el monto, calcular el estado (si no se forzó arriba)
            elif "monto_pagado_acumulado" in update_dict:
                nuevo_pagado = Decimal(str(update_dict["monto_pagado_acumulado"]))
                if nuevo_pagado <= Decimal("0"):
                    update_dict["estado_pago_neto"] = EstadoPagoNeto.PENDIENTE
                elif nuevo_pagado >= monto_neto:
                    update_dict["estado_pago_neto"] = EstadoPagoNeto.PAGADO
                else:
                    update_dict["estado_pago_neto"] = EstadoPagoNeto.PAGADO_PARCIAL

            if "fecha_pago_detraccion" in update_dict and update_dict["fecha_pago_detraccion"]:
                update_dict["estado_detraccion"] = EstadoDetraccion.PAGADO
            
            update_dict["ultima_actualizacion"] = datetime.now()

            # Serialización para MongoDB
            for k, v in update_dict.items():
                if isinstance(v, Enum):
                    update_dict[k] = v.value
                elif isinstance(v, Decimal):
                    update_dict[k] = float(v)
                elif isinstance(v, date) and not isinstance(v, datetime):
                    update_dict[k] = datetime.combine(v, datetime.min.time())

            self.collection.update_one(
                {"_id": ObjectId(gestion_id)},
                {"$set": update_dict}
            )
            
            return self.get_gestion_by_id(gestion_id)
            
        except Exception as e:
            logger.error(f"Error al actualizar gestión: {str(e)}")
            raise


    def delete_gestion(self, gestion_id: str) -> bool:
        """Eliminar gestión"""
        try:
            if not ObjectId.is_valid(gestion_id):
                return False
            
            result = self.collection.delete_one({"_id": ObjectId(gestion_id)})
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error al eliminar gestión: {str(e)}")
            return False
    
    def registrar_pago_parcial(self, gestion_id: str, monto_pago: Decimal, nro_operacion: str = None) -> dict:
        """Registrar un pago parcial"""
        try:
            gestion = self.get_gestion_by_id(gestion_id)
            if not gestion:
                raise ValueError("Gestión no encontrada")
            
            if monto_pago <= Decimal("0"):
                raise ValueError("El monto del pago debe ser mayor a cero")
            
            nuevo_acumulado = gestion["monto_pagado_acumulado"] + monto_pago
            
            if nuevo_acumulado > gestion["monto_neto"]:
                raise ValueError(f"El pago excede el monto neto. Saldo pendiente: {gestion['saldo_pendiente']}")
            
            update_data = {
                "monto_pagado_acumulado": nuevo_acumulado
            }
            
            if nro_operacion:
                update_data["nro_operacion_pago_neto"] = nro_operacion
            
            return self.update_gestion(gestion_id, update_data)
            
        except Exception as e:
            logger.error(f"Error al registrar pago parcial: {str(e)}")
            raise
    
    def get_gestiones_vencidas(self) -> List[dict]:
        """Obtener gestiones con pagos vencidos"""
        try:
            hoy = datetime.now().date()
            
            query = {
                "fecha_probable_pago": {"$lt": hoy},
                "estado_pago_neto": {
                    "$in": [
                        EstadoPagoNeto.PENDIENTE,
                        EstadoPagoNeto.PROGRAMADO,
                        EstadoPagoNeto.PAGADO_PARCIAL
                    ]
                }
            }
            
            gestiones = list(
                self.collection.find(query)
                .sort("fecha_probable_pago", 1)
            )
            
            for gestion in gestiones:
                self.collection.update_one(
                    {"_id": gestion["_id"]},
                    {"$set": {
                        "estado_pago_neto": EstadoPagoNeto.VENCIDO,
                        "ultima_actualizacion": datetime.now()
                    }}
                )
                gestion["estado_pago_neto"] = EstadoPagoNeto.VENCIDO
            
            return [self._format_gestion_response(g) for g in gestiones]
            
        except Exception as e:
            logger.error(f"Error al obtener gestiones vencidas: {str(e)}")
            return []
    
    def get_dashboard_stats(self, cliente_nombre: str = None) -> Dict[str, Any]:
        try:
            hoy_dt = datetime.combine(datetime.now().date(), time.min)
            hoy_str = hoy_dt.strftime("%Y-%m-%d")

            # 1. Definimos qué estados NO deben sumar dinero (Anuladas)
            estado_anulado = "Anulado" # Asegúrate de que coincida con tu DB

            pipeline = [
                # Filtro inicial opcional por cliente
                { "$match": {"datos_completos.fletes.servicio.nombre_cliente": cliente_nombre} if cliente_nombre else {} },
                
                {
                    "$facet": {
                        # Conteo general (incluye todo para saber qué pasó)
                        "metricas_globales": [
                            {"$group": {
                                "_id": None,
                                "total_items": {"$sum": 1},
                                "total_anuladas": {
                                    "$sum": {"$cond": [{"$eq": ["$estado_pago_neto", estado_anulado]}, 1, 0]}
                                }
                            }}
                        ],
                        "por_estado_pago": [
                            {"$group": {"_id": "$estado_pago_neto", "count": {"$sum": 1}}}
                        ],
                        # CÁLCULOS FINANCIEROS (Solo facturas NO anuladas)
                        "financiero": [
                            { "$match": { "estado_pago_neto": { "$ne": estado_anulado } } },
                            {
                                "$group": {
                                    "_id": None,
                                    "neto": {"$sum": {"$toDouble": "$monto_neto"}},
                                    "pagado": {"$sum": {"$toDouble": "$monto_pagado_acumulado"}},
                                    "detraccion": {"$sum": {"$toDouble": "$monto_detraccion"}},
                                    # Solo sumamos vencidas aquí dentro para asegurar que NO sean anuladas
                                    "vencidas_count": {
                                        "$sum": {
                                            "$cond": [
                                                { "$and": [
                                                    { "$lt": ["$datos_completos.fecha_vencimiento", hoy_str] },
                                                    { "$in": ["$estado_pago_neto", ["Pendiente", "Pagado Parcial"]]}
                                                ]},
                                                1, 0
                                            ]
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            ]

            result = list(self.collection.aggregate(pipeline))[0]

            # --- Extracción segura de datos ---
            globales = result["metricas_globales"][0] if result["metricas_globales"] else {}
            fin = result["financiero"][0] if result["financiero"] else {}
            
            total_neto = Decimal(str(fin.get("neto") or "0"))
            total_pagado = Decimal(str(fin.get("pagado") or "0"))
            total_detraccion = Decimal(str(fin.get("detraccion") or "0"))
            
            # El saldo pendiente real es lo que falta por pagar de facturas NO anuladas
            saldo_pendiente = total_neto - total_pagado

            return {
                "total_gestiones": globales.get("total_items", 0),
                "cant_anuladas": globales.get("total_anuladas", 0),
                "vencidas": fin.get("vencidas_count", 0),
                "por_estado_pago": {str(r["_id"]): r["count"] for r in result["por_estado_pago"]},
                "montos_totales": {
                    "neto": f"{total_neto:,.2f}",
                    "pagado": f"{total_pagado:,.2f}",
                    "detraccion": f"{total_detraccion:,.2f}",
                    "pendiente": f"{saldo_pendiente:,.2f}"
                }
            }
        except Exception as e:
            print(f"Error: {e}")
            return {}
    def export_to_excel(self, filter_params: Optional[FacturacionGestionFilter] = None) -> BytesIO:
        """Exportar gestiones a Excel con datos completos de snapshots"""
        try:
            gestiones = self._get_all_gestiones_sin_paginacion(filter_params)
            
            if not gestiones:
                df = pd.DataFrame(columns=[
                    "ID", "Código Factura", "Número Factura", "Cliente", "Proveedor",
                    "Placa", "Conductor", "Auxiliar", "Tipo Servicio", "Zona",
                    "Fecha Servicio", "Origen", "Destino",
                    "Estado Pago Neto", "Estado Detracción",
                    "Monto Total", "Monto Neto", "Monto Pagado", "Saldo Pendiente",
                    "Monto Detracción", "Tasa Detracción (%)",
                    "Fecha Probable Pago", "Prioridad", "Responsable"
                ])
            else:
                excel_data = []
                for gestion in gestiones:
                    # Extraer datos de snapshot si existen
                    datos = gestion.get("datos_completos", {})
                    flete = datos.get("fletes", [{}])[0] if datos.get("fletes") else {}
                    servicio = flete.get("servicio", {})
                    
                    excel_data.append({
                        "ID": gestion.get("id", ""),
                        "Código Factura": gestion.get("codigo_factura", ""),
                        "Número Factura": datos.get("numero_factura", ""),
                        "Cliente": servicio.get("nombre_cliente", ""),
                        "Proveedor": servicio.get("nombre_proveedor", ""),
                        "Placa": servicio.get("placa_flota", ""),
                        "Conductor": servicio.get("nombre_conductor", ""),
                        "Auxiliar": servicio.get("nombre_auxiliar", ""),
                        "Tipo Servicio": servicio.get("tipo_servicio", ""),
                        "Zona": servicio.get("zona", ""),
                        "Fecha Servicio": servicio.get("fecha_servicio", ""),
                        "Origen": servicio.get("origen", ""),
                        "Destino": servicio.get("destino", ""),
                        "Estado Pago Neto": gestion.get("estado_pago_neto", ""),
                        "Estado Detracción": gestion.get("estado_detraccion", ""),
                        "Monto Total": str(datos.get("monto_total", Decimal("0"))),
                        "Monto Neto": str(gestion.get("monto_neto", Decimal("0"))),
                        "Monto Pagado": str(gestion.get("monto_pagado_acumulado", Decimal("0"))),
                        "Saldo Pendiente": str(gestion.get("saldo_pendiente", Decimal("0"))),
                        "Monto Detracción": str(gestion.get("monto_detraccion", Decimal("0"))),
                        "Tasa Detracción (%)": str(gestion.get("tasa_detraccion", Decimal("4.0"))),
                        "Fecha Probable Pago": gestion.get("fecha_probable_pago", ""),
                        "Prioridad": gestion.get("prioridad", ""),
                        "Responsable": gestion.get("responsable_gestion", "")
                    })
                
                df = pd.DataFrame(excel_data)
            
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Gestión Facturación')
                
                from openpyxl.styles import Font, PatternFill, Alignment
                
                workbook = writer.book
                worksheet = writer.sheets['Gestión Facturación']
                
                header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
                header_font = Font(color="FFFFFF", bold=True)
                
                for cell in worksheet[1]:
                    cell.fill = header_fill
                    cell.font = header_font
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            output.seek(0)
            return output
            
        except Exception as e:
            logger.error(f"Error al exportar a Excel: {str(e)}")
            raise
    
    def _get_all_gestiones_sin_paginacion(
        self,
        filter_params: Optional[FacturacionGestionFilter] = None
    ) -> List[dict]:
        """Obtener TODAS las gestiones sin paginación (para exportación)"""
        try:
            query = self._build_query(filter_params)
            
            gestiones = list(
                self.collection.find(query)
                .sort("fecha_probable_pago", 1)
            )
            
            return [self._format_gestion_response(g) for g in gestiones]
            
        except Exception as e:
            logger.error(f"Error al obtener gestiones (sin paginación): {str(e)}")
            raise
    
    def _format_gestion_response(self, gestion: dict) -> dict:
        """Formatear respuesta de gestión incluyendo datos_completos"""
        if not gestion:
            return {}
        
        result = {
            "id": str(gestion["_id"]),
            "codigo_factura": gestion.get("codigo_factura", ""),
            "datos_completos": gestion.get("datos_completos"),
            "estado_detraccion": gestion.get("estado_detraccion", EstadoDetraccion.PENDIENTE),
            "tasa_detraccion": gestion.get("tasa_detraccion", Decimal("4.0")),
            "monto_detraccion": gestion.get("monto_detraccion", Decimal("0.0")),
            "nro_constancia_detraccion": gestion.get("nro_constancia_detraccion"),
            "fecha_pago_detraccion": gestion.get("fecha_pago_detraccion"),
            "estado_pago_neto": gestion.get("estado_pago_neto", EstadoPagoNeto.PENDIENTE),
            "monto_neto": gestion.get("monto_neto", Decimal("0.0")),
            "monto_pagado_acumulado": gestion.get("monto_pagado_acumulado", Decimal("0.0")),
            "banco_destino": gestion.get("banco_destino"),
            "cuenta_bancaria_destino": gestion.get("cuenta_bancaria_destino"),
            "nro_operacion_pago_neto": gestion.get("nro_operacion_pago_neto"),
            "fecha_probable_pago": gestion.get("fecha_probable_pago"),
            "prioridad": gestion.get("prioridad", PrioridadPago.MEDIA),
            "centro_costo": gestion.get("centro_costo"),
            "responsable_gestion": gestion.get("responsable_gestion"),
            "observaciones_admin": gestion.get("observaciones_admin"),
            "ultima_actualizacion": gestion.get("ultima_actualizacion")
        }
        
        result["saldo_pendiente"] = result["monto_neto"] - result["monto_pagado_acumulado"]
        
        return result
    
    def get_gestiones_por_vencer(self, dias: int = 7) -> List[dict]:
        """Obtener gestiones que están por vencer en los próximos X días"""
        try:
            hoy = datetime.now().date()
            fecha_limite = hoy + timedelta(days=dias)
            
            query = {
                "fecha_probable_pago": {
                    "$gte": hoy,
                    "$lte": fecha_limite
                },
                "estado_pago_neto": {
                    "$in": [
                        EstadoPagoNeto.PENDIENTE,
                        EstadoPagoNeto.PROGRAMADO,
                        EstadoPagoNeto.PAGADO_PARCIAL
                    ]
                }
            }
            
            gestiones = list(
                self.collection.find(query)
                .sort("fecha_probable_pago", 1)
            )
            
            return [self._format_gestion_response(g) for g in gestiones]
            
        except Exception as e:
            logger.error(f"Error al obtener gestiones por vencer: {str(e)}")
            return []

    def get_advanced_analytics(self, timeframe: str = "month") -> Dict[str, Any]:
        """
        timeframe: "day", "week", "month", "year"
        """
        try:
            hoy_dt = datetime.now()
            hoy_str = hoy_dt.strftime("%Y-%m-%d")
            
            # Formato de fecha para el agrupamiento temporal
            date_format = {
                "day": "%Y-%m-%d",
                "week": "%Y-W%V",
                "month": "%Y-%m",
                "year": "%Y"
            }.get(timeframe, "%Y-%m")

            pipeline = [
                {
                    "$facet": {
                        # ========== ANALÍTICAS FINANCIERAS (A NIVEL FACTURA) ==========
                        # NUNCA usar $unwind aquí porque multiplica los montos
                        
                        # 1. RESUMEN FINANCIERO TOTAL (Basado en Facturas)
                        "totales_generales": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$group": {
                                "_id": None,
                                "total_facturado": { "$sum": { "$toDouble": "$monto_neto" } },
                                "total_pagado": { "$sum": { "$toDouble": "$monto_pagado_acumulado" } },
                                "total_detraccion": { "$sum": { "$toDouble": "$monto_detraccion" } },
                                "cantidad_facturas": { "$sum": 1 }
                            }}
                        ],

                        # 2. ESTADOS DE FACTURAS (Cantidades)
                        "estados_facturas": [
                            { "$group": {
                                "_id": "$estado_pago_neto",
                                "cantidad": { "$sum": 1 }
                            }}
                        ],

                        # 3. FACTURAS VENCIDAS (A nivel factura, no flete)
                        "vencidas": [
                            { "$match": { 
                                "estado_pago_neto": { "$in": ["Pendiente", "Pagado Parcial"] },
                                "datos_completos.fecha_vencimiento": { "$lt": hoy_str }
                            }},
                            { "$count": "conteo" }
                        ],

                        # 4. TENDENCIA TEMPORAL DE FACTURACIÓN (Por Facturas)
                        "tendencia_temporal": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$addFields": {
                                "periodo": { "$dateToString": { 
                                    "format": date_format, 
                                    "date": { "$toDate": "$datos_completos.fecha_emision" } 
                                }}
                            }},
                            { "$group": {
                                "_id": "$periodo",
                                "monto": { "$sum": { "$toDouble": "$monto_neto" } },
                                "facturas": { "$sum": 1 }
                            }},
                            { "$sort": { "_id": 1 } }
                        ],

                        # 5. TENDENCIA MENSUAL DE VENTAS (Por Facturas)
                        "tendencia_ventas": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$addFields": {
                                "mes": { "$substr": ["$datos_completos.fecha_emision", 0, 7] }
                            }},
                            { "$group": {
                                "_id": "$mes",
                                "total": { "$sum": { "$toDouble": "$monto_neto" } },
                                "facturas": { "$sum": 1 }
                            }},
                            { "$sort": { "_id": 1 } }
                        ],

                        # ========== ANALÍTICAS OPERATIVAS (A NIVEL FLETE) ==========
                        # Aquí SÍ usamos $unwind porque analizamos operaciones individuales
                        
                        # 6. CONTEO TOTAL DE FLETES
                        "total_fletes": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$unwind": "$datos_completos.fletes" },
                            { "$count": "total" }
                        ],

                        # 7. TOP CONDUCTORES (Ranking por viajes)
                        "ranking_conductores": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$unwind": "$datos_completos.fletes" },
                            { "$group": {
                                "_id": "$datos_completos.fletes.servicio.nombre_conductor",
                                "viajes": { "$sum": 1 }
                            }},
                            { "$sort": { "viajes": -1 } },
                            { "$limit": 5 }
                        ],

                        # 8. RUTAS MÁS FRECUENTES
                        "rutas_frecuentes": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$unwind": "$datos_completos.fletes" },
                            { "$group": {
                                "_id": { 
                                    "orig": "$datos_completos.fletes.servicio.origen", 
                                    "dest": "$datos_completos.fletes.servicio.destino" 
                                },
                                "conteo": { "$sum": 1 }
                            }},
                            { "$sort": { "conteo": -1 } },
                            { "$limit": 5 }
                        ],

                        # 9. TOP CLIENTES (Por monto facturado total)
                        "top_clientes": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$unwind": "$datos_completos.fletes" },
                            { "$group": {
                                "_id": {
                                    "cliente": "$datos_completos.fletes.servicio.nombre_cliente",
                                    "factura": "$numero_factura"
                                },
                                "monto_factura": { "$first": { "$toDouble": "$monto_neto" } }
                            }},
                            { "$group": {
                                "_id": "$_id.cliente",
                                "total_facturado": { "$sum": "$monto_factura" },
                                "cantidad_facturas": { "$sum": 1 }
                            }},
                            { "$sort": { "total_facturado": -1 } },
                            { "$limit": 5 }
                        ],
                        
                        # 10. TOP PROVEEDORES (Por monto de fletes)
                        "top_proveedores": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$unwind": "$datos_completos.fletes" },
                            { "$group": {
                                "_id": "$datos_completos.fletes.servicio.nombre_proveedor",
                                "monto_fletes": { "$sum": { "$toDouble": "$datos_completos.fletes.monto_flete" } },
                                "servicios_realizados": { "$sum": 1 }
                            }},
                            { "$sort": { "monto_fletes": -1 } },
                            { "$limit": 5 }
                        ],
                        
                        # 11. EFICIENCIA DE FLOTA (Por placas)
                        "uso_placas": [
                            { "$match": { "estado_pago_neto": { "$ne": "Anulado" } } },
                            { "$unwind": "$datos_completos.fletes" },
                            { "$group": {
                                "_id": "$datos_completos.fletes.servicio.placa_flota",
                                "conteo": { "$sum": 1 },
                                "total_tonelaje": { "$sum": { "$toDouble": "$datos_completos.fletes.servicio.tn" } }
                            }},
                            { "$sort": { "conteo": -1 } },
                            { "$limit": 10 }
                        ]
                    }
                }
            ]

            raw = list(self.collection.aggregate(pipeline))[0]

            # Procesamiento de datos
            totales = raw["totales_generales"][0] if raw["totales_generales"] else {}
            total_fletes_obj = raw["total_fletes"][0] if raw["total_fletes"] else {}
            
            neto = Decimal(str(totales.get("total_facturado", 0)))
            pagado = Decimal(str(totales.get("total_pagado", 0)))

            return {
                "kpis": {
                    "facturado_total": f"{neto:,.2f}",
                    "pagado_total": f"{pagado:,.2f}",
                    "pendiente_total": f"{(neto - pagado):,.2f}",
                    "total_facturas": totales.get("cantidad_facturas", 0),
                    "total_fletes": total_fletes_obj.get("total", 0),
                    "facturas_vencidas": raw["vencidas"][0]["conteo"] if raw["vencidas"] else 0
                },
                "graficos": {
                    "linea_tiempo": {
                        "labels": [r["_id"] for r in raw["tendencia_temporal"]],
                        "datasets": [
                            {
                                "label": "Monto Facturado",
                                "data": [float(r["monto"]) for r in raw["tendencia_temporal"]]
                            },
                            {
                                "label": "Cant. Facturas",
                                "data": [r["facturas"] for r in raw["tendencia_temporal"]]
                            }
                        ]
                    },
                    "conductores_activos": {
                        "labels": [r["_id"] for r in raw["ranking_conductores"]],
                        "data": [r["viajes"] for r in raw["ranking_conductores"]]
                    },
                    "rutas_top": {
                        "labels": [f"{r['_id']['orig']} → {r['_id']['dest']}" for r in raw["rutas_frecuentes"]],
                        "data": [r["conteo"] for r in raw["rutas_frecuentes"]]
                    },
                    "clientes_top": {
                        "labels": [r["_id"] if r["_id"] else "Sin Cliente" for r in raw["top_clientes"]],
                        "data": [float(r["total_facturado"]) for r in raw["top_clientes"]]
                    },
                    "proveedores_top": {
                        "labels": [r["_id"] if r["_id"] else "Sin Proveedor" for r in raw["top_proveedores"]],
                        "data": [float(r["monto_fletes"]) for r in raw["top_proveedores"]]
                    },
                    "placas_activas": {
                        "labels": [r["_id"] if r["_id"] else "Sin Placa" for r in raw["uso_placas"]],
                        "data": [r["conteo"] for r in raw["uso_placas"]],
                        "tonelaje": [float(r["total_tonelaje"]) for r in raw["uso_placas"]]
                    },
                    "tendencia_mensual": {
                        "labels": [r["_id"] for r in raw["tendencia_ventas"]],
                        "data": [float(r["total"]) for r in raw["tendencia_ventas"]]
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error en analítica avanzada: {e}")
            return {}
        


    def get_all_gestiones_advance(
        self,
        filter_params: Optional[FacturacionGestionFilter] = None,
        page: int = 1,
        page_size: int = 100
    ) -> dict:
        """Obtener gestiones con KPIs financieros y paginación"""
        try:
            # 1. Construir el filtro (Clientes y Rango de Fechas)
            query = self._build_query(filter_params)
            skip = (page - 1) * page_size

            # 2. CÁLCULO SEPARADO: Total Vendido
            # 2.1 Fletes valorizados de las gestiones filtradas
            gestiones_filtradas = list(self.collection.find(query, {"codigo_factura": 1}))
            codigos_factura = [g["codigo_factura"] for g in gestiones_filtradas]
            
            facturas = list(self.facturas_collection.find(
                {"numero_factura": {"$in": codigos_factura}},
                {"_id": 1}
            ))
            facturas_ids = [str(f["_id"]) for f in facturas]
            
            # Fletes valorizados que pertenecen a facturas filtradas
            pipeline_facturados = [
                {
                    "$match": {
                        "factura_id": {"$in": facturas_ids},
                        "estado_flete": "VALORIZADO",
                        "monto_flete": {"$gt": 0}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total": {"$sum": {"$toDouble": "$monto_flete"}},
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            resultado_facturados = list(self.fletes_collection.aggregate(pipeline_facturados))
            total_facturados = resultado_facturados[0]["total"] if resultado_facturados else 0
            count_facturados = resultado_facturados[0]["count"] if resultado_facturados else 0
            
            # 2.2 Fletes valorizados que NO pertenecen a ninguna factura (pertenece_a_factura = false)
            pipeline_sin_factura = [
                {
                    "$match": {
                        "pertenece_a_factura": False,
                        "estado_flete": "VALORIZADO",
                        "monto_flete": {"$gt": 0}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total": {"$sum": {"$toDouble": "$monto_flete"}},
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            resultado_sin_factura = list(self.fletes_collection.aggregate(pipeline_sin_factura))
            total_sin_factura = resultado_sin_factura[0]["total"] if resultado_sin_factura else 0
            count_sin_factura = resultado_sin_factura[0]["count"] if resultado_sin_factura else 0
            
            # Total vendido = facturados filtrados + sin facturar
            total_vendido = total_facturados + total_sin_factura
            logger.info(f"Total vendido: {total_vendido} | Facturados: {total_facturados} ({count_facturados} fletes) | Sin factura: {total_sin_factura} ({count_sin_factura} fletes)")

            # 3. Pipeline de Agregación para el resto de KPIs
            pipeline = [
                {"$match": query},
                {
                    "$facet": {
                        # Rama 1: Cálculo de Totales (KPIs)
                        "totales": [
                            {
                                "$group": {
                                    "_id": None,
                                    
                                    # Suma directa de campos en la raíz
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
                                    }
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

            result = list(self.collection.aggregate(pipeline))[0]

            # 4. Extraer datos del Facet
            totales_data = result["totales"][0] if result["totales"] else {}
            items_raw = result["resultados"]
            total_docs = result["conteo"][0]["total"] if result["conteo"] else 0
            
            formatted_gestiones = [self._format_gestion_response(g) for g in items_raw]
            total_pages = ceil(total_docs / page_size) if page_size > 0 else 0

            return {
                "summary": {
                    "total_vendido": total_vendido,  # Calculado por separado
                    "total_facturado": totales_data.get("total_facturado", 0),
                    "total_pagado": totales_data.get("total_pagado_acumulado", 0),
                    "total_pendiente": totales_data.get("total_pendiente_neto", 0),
                    "total_detracciones": totales_data.get("total_detracciones", 0),
                    "total_pagado_detracc": totales_data.get("total_pagado_detracc", 0),
                    "total_pendiente_detracc": totales_data.get("total_pendiente_detracc", 0)
                },
                "items": formatted_gestiones,
                "total": total_docs,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }

        except Exception as e:
            logger.error(f"Error al obtener gestiones: {str(e)}")
            raise


def safe_regex(value: str):
    """Crear expresión regular segura para búsquedas"""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    return {"$regex": re.escape(value), "$options": "i"}