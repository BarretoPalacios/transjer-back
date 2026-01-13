from typing import List, Optional, Dict, Any
from bson import ObjectId
from app.modules.utils.core.code_generator.code_generator import generate_sequential_code
from app.core.database import get_database
from app.modules.facturacion.models.facturacion import Facturacion
from app.modules.facturacion.schemas.facturacion_schema import (
    FacturacionCreate, 
    FacturacionUpdate, 
    FacturacionFilter
)
from datetime import datetime, date, timedelta
from decimal import Decimal
import pandas as pd
from io import BytesIO
import logging
import math

logger = logging.getLogger(__name__)

class FacturacionService:
    def __init__(self, db):
        self.db = db 
        self.collection = db["facturacion"]
        self.fletes_collection = db["fletes"]
    
    def _convert_datetime_to_date(self, data: dict) -> dict:
        converted_data = data.copy()
        date_fields = ['fecha_emision', 'fecha_vencimiento', 'fecha_pago']
        
        for field in date_fields:
            if field in converted_data and converted_data[field] is not None:
                if isinstance(converted_data[field], datetime):
                    converted_data[field] = converted_data[field].date()
        
        return converted_data
    
    def _convert_decimal_to_float(self, data: dict) -> dict:
        converted_data = data.copy()
        if 'monto_total' in converted_data and converted_data['monto_total'] is not None:
            if isinstance(converted_data['monto_total'], Decimal):
                converted_data['monto_total'] = float(converted_data['monto_total'])
        
        return converted_data
    
    def _populate_fletes(self, factura: dict) -> dict:
        """Poblar información completa de los fletes"""
        if "fletes" in factura and isinstance(factura["fletes"], list):
            fletes_populated = []
            for flete_ref in factura["fletes"]:
                if isinstance(flete_ref, dict) and "id" in flete_ref:
                    flete_id = flete_ref["id"]
                    if ObjectId.is_valid(flete_id):
                        flete_data = self.fletes_collection.find_one({"_id": ObjectId(flete_id)})
                        if flete_data:
                            flete_data["id"] = str(flete_data["_id"])
                            del flete_data["_id"]
                            fletes_populated.append(flete_data)
                        else:
                            fletes_populated.append(flete_ref)
                    else:
                        fletes_populated.append(flete_ref)
                else:
                    fletes_populated.append(flete_ref)
            
            factura["fletes"] = fletes_populated
        
        return factura
    
    def _build_query(self, filter_params: Optional[FacturacionFilter] = None) -> dict:
        query = {}
        
        if filter_params:
            if filter_params.numero_factura:
                query["numero_factura"] = {"$regex": filter_params.numero_factura, "$options": "i"}
            
            if filter_params.estado:
                query["estado"] = filter_params.estado
            
            if filter_params.moneda:
                query["moneda"] = filter_params.moneda
            
            if filter_params.es_borrador is not None:
                query["es_borrador"] = filter_params.es_borrador
            
            if filter_params.flete_id:
                query["fletes.id"] = filter_params.flete_id
            
            # Filtros de fechas
            if filter_params.periodo:
                fecha_inicio, fecha_fin = self._get_period_date_range(filter_params.periodo)
                if fecha_inicio and fecha_fin:
                    query["fecha_emision"] = {"$gte": fecha_inicio, "$lte": fecha_fin}
            elif filter_params.fecha_emision:
                fecha_inicio = datetime.combine(filter_params.fecha_emision, datetime.min.time())
                fecha_fin = datetime.combine(filter_params.fecha_emision, datetime.max.time())
                query["fecha_emision"] = {"$gte": fecha_inicio, "$lte": fecha_fin}
            elif filter_params.fecha_emision_inicio or filter_params.fecha_emision_fin:
                fecha_query = {}
                if filter_params.fecha_emision_inicio:
                    fecha_query["$gte"] = datetime.combine(filter_params.fecha_emision_inicio, datetime.min.time())
                if filter_params.fecha_emision_fin:
                    fecha_query["$lte"] = datetime.combine(filter_params.fecha_emision_fin, datetime.max.time())
                if fecha_query:
                    query["fecha_emision"] = fecha_query
            
            if filter_params.fecha_vencimiento:
                fecha_inicio = datetime.combine(filter_params.fecha_vencimiento, datetime.min.time())
                fecha_fin = datetime.combine(filter_params.fecha_vencimiento, datetime.max.time())
                query["fecha_vencimiento"] = {"$gte": fecha_inicio, "$lte": fecha_fin}
            elif filter_params.fecha_vencimiento_inicio or filter_params.fecha_vencimiento_fin:
                fecha_query = {}
                if filter_params.fecha_vencimiento_inicio:
                    fecha_query["$gte"] = datetime.combine(filter_params.fecha_vencimiento_inicio, datetime.min.time())
                if filter_params.fecha_vencimiento_fin:
                    fecha_query["$lte"] = datetime.combine(filter_params.fecha_vencimiento_fin, datetime.max.time())
                if fecha_query:
                    query["fecha_vencimiento"] = fecha_query
            
            if filter_params.fecha_pago:
                fecha_inicio = datetime.combine(filter_params.fecha_pago, datetime.min.time())
                fecha_fin = datetime.combine(filter_params.fecha_pago, datetime.max.time())
                query["fecha_pago"] = {"$gte": fecha_inicio, "$lte": fecha_fin}
            elif filter_params.fecha_pago_inicio or filter_params.fecha_pago_fin:
                fecha_query = {}
                if filter_params.fecha_pago_inicio:
                    fecha_query["$gte"] = datetime.combine(filter_params.fecha_pago_inicio, datetime.min.time())
                if filter_params.fecha_pago_fin:
                    fecha_query["$lte"] = datetime.combine(filter_params.fecha_pago_fin, datetime.max.time())
                if fecha_query:
                    query["fecha_pago"] = fecha_query
            
            if filter_params.monto_total_minimo or filter_params.monto_total_maximo:
                monto_query = {}
                if filter_params.monto_total_minimo:
                    monto_query["$gte"] = float(filter_params.monto_total_minimo)
                if filter_params.monto_total_maximo:
                    monto_query["$lte"] = float(filter_params.monto_total_maximo)
                if monto_query:
                    query["monto_total"] = monto_query
        
        return query

    def create_factura(self, factura_data: dict) -> dict:
        try:
            # Validar número de factura único
            if "numero_factura" in factura_data and factura_data["numero_factura"]:
                if self.collection.find_one({"numero_factura": factura_data["numero_factura"]}):
                    raise ValueError("El número de factura ya está registrado")

            fletes_ids = []

            # Validar fletes
            if "fletes" in factura_data and isinstance(factura_data["fletes"], list):
                for flete_ref in factura_data["fletes"]:
                    flete_id = flete_ref.get("id")
                    if not ObjectId.is_valid(flete_id):
                        raise ValueError(f"ID de flete inválido: {flete_id}")

                    flete = self.fletes_collection.find_one({"_id": ObjectId(flete_id)})
                    if not flete:
                        raise ValueError(f"El flete {flete_id} no existe")

                    if flete.get("pertenece_a_factura"):
                        raise ValueError(f"El flete {flete_id} ya fue facturado")

                    fletes_ids.append(ObjectId(flete_id))

            # Generar código de factura
            codigo_factura = generate_sequential_code(
                counters_collection=self.db["counters"],
                target_collection=self.collection,
                sequence_name="facturas",
                field_name="codigo_factura",
                prefix="FAC-",
                length=10
            )

            factura_data.update({
                "codigo_factura": codigo_factura,
                "fecha_registro": datetime.now(),
                "fecha_actualizacion": None,
                "estado": "Borrador",  # Estado inicial
                "es_borrador": True
            })

            factura_data = self._convert_decimal_to_float(factura_data)

            # Validar y crear modelo
            factura_model = Facturacion(**factura_data)

            result = self.collection.insert_one(
                factura_model.model_dump(by_alias=True)
            )

            # Marcar fletes como facturados
            if fletes_ids:
                self.fletes_collection.update_many(
                    {"_id": {"$in": fletes_ids}},
                    {"$set": {"pertenece_a_factura": True,
                               "factura_id": str(result.inserted_id),
                                 "codigo_factura": codigo_factura}}
                )

            created_factura = self.collection.find_one({"_id": result.inserted_id})
            created_factura["id"] = str(created_factura["_id"])
            del created_factura["_id"]

            return self._populate_fletes(
                self._convert_datetime_to_date(created_factura)
            )

        except Exception as e:
            logger.error(f"Error al crear factura: {str(e)}")
            raise

    def _get_period_date_range(self, periodo: str) -> tuple:
        hoy = datetime.now()
        
        if periodo == 'hoy':
            fecha_inicio = hoy.replace(hour=0, minute=0, second=0, microsecond=0)
            fecha_fin = hoy.replace(hour=23, minute=59, second=59, microsecond=999999)
            return fecha_inicio, fecha_fin
            
        elif periodo == 'semana':
            fecha_inicio = hoy - timedelta(days=hoy.weekday())
            fecha_inicio = fecha_inicio.replace(hour=0, minute=0, second=0, microsecond=0)
            fecha_fin = fecha_inicio + timedelta(days=6)
            fecha_fin = fecha_fin.replace(hour=23, minute=59, second=59, microsecond=999999)
            return fecha_inicio, fecha_fin
            
        elif periodo == 'mes':
            fecha_inicio = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if hoy.month == 12:
                fecha_fin = hoy.replace(year=hoy.year + 1, month=1, day=1)
            else:
                fecha_fin = hoy.replace(month=hoy.month + 1, day=1)
            fecha_fin = fecha_fin - timedelta(days=1)
            fecha_fin = fecha_fin.replace(hour=23, minute=59, second=59, microsecond=999999)
            return fecha_inicio, fecha_fin
            
        elif periodo == 'año':
            fecha_inicio = hoy.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            fecha_fin = hoy.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)
            return fecha_inicio, fecha_fin
            
        else:
            return None, None
    
    def get_all_facturas(
        self, 
        filter_params: Optional[FacturacionFilter] = None,
        page: int = 1,
        page_size: int = 10,
        sort_by: str = "codigo_factura",
        sort_order: int = -1
    ) -> Dict[str, Any]:
        try:
            page = max(1, page)
            page_size = max(1, min(page_size, 100))
            query = self._build_query(filter_params)
            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size
            
            facturas_list = list(
                self.collection
                .find(query)
                .sort(sort_by, sort_order)
                .skip(skip)
                .limit(page_size)
            )
            
            items = []
            for factura in facturas_list:
                factura["id"] = str(factura["_id"])
                del factura["_id"]
                factura = self._convert_datetime_to_date(factura)
                factura = self._populate_fletes(factura)
                items.append(factura)
            
            total_pages = math.ceil(total / page_size) if total > 0 else 0
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
            
        except Exception as e:
            logger.error(f"Error al obtener facturas: {str(e)}")
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "has_next": False,
                "has_prev": False
            }
    
    def get_facturas_por_periodo(
        self, 
        periodo: str,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        try:
            if periodo not in ['hoy', 'semana', 'mes', 'año']:
                raise ValueError(f"Período '{periodo}' no válido")
            
            fecha_inicio, fecha_fin = self._get_period_date_range(periodo)
            
            if not fecha_inicio or not fecha_fin:
                return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0, "has_next": False, "has_prev": False}
            
            query = {"fecha_emision": {"$gte": fecha_inicio, "$lte": fecha_fin}}
            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size
            
            facturas_list = list(
                self.collection.find(query).sort("fecha_emision", -1).skip(skip).limit(page_size)
            )
            
            items = []
            for factura in facturas_list:
                factura["id"] = str(factura["_id"])
                del factura["_id"]
                factura = self._convert_datetime_to_date(factura)
                factura = self._populate_fletes(factura)
                items.append(factura)
            
            total_pages = math.ceil(total / page_size) if total > 0 else 0
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
            
        except Exception as e:
            logger.error(f"Error al obtener facturas por período: {str(e)}")
            return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0, "has_next": False, "has_prev": False}
    
    def get_facturas_por_fecha_rango(
        self, 
        fecha_inicio: date, 
        fecha_fin: date,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        try:
            fecha_inicio_dt = datetime.combine(fecha_inicio, datetime.min.time())
            fecha_fin_dt = datetime.combine(fecha_fin, datetime.max.time())
            query = {"fecha_emision": {"$gte": fecha_inicio_dt, "$lte": fecha_fin_dt}}
            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size
            
            facturas_list = list(
                self.collection.find(query).sort("fecha_emision", -1).skip(skip).limit(page_size)
            )
            
            items = []
            for factura in facturas_list:
                factura["id"] = str(factura["_id"])
                del factura["_id"]
                factura = self._convert_datetime_to_date(factura)
                factura = self._populate_fletes(factura)
                items.append(factura)
            
            total_pages = math.ceil(total / page_size) if total > 0 else 0
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
            
        except Exception as e:
            logger.error(f"Error al obtener facturas por rango: {str(e)}")
            return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0, "has_next": False, "has_prev": False}
    
    def get_facturas_por_estado(
        self, 
        estado: str,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        try:
            query = {"estado": estado}
            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size
            
            facturas_list = list(
                self.collection.find(query).sort("fecha_emision", -1).skip(skip).limit(page_size)
            )
            
            items = []
            for factura in facturas_list:
                factura["id"] = str(factura["_id"])
                del factura["_id"]
                factura = self._convert_datetime_to_date(factura)
                factura = self._populate_fletes(factura)
                items.append(factura)
            
            total_pages = math.ceil(total / page_size) if total > 0 else 0
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
            
        except Exception as e:
            logger.error(f"Error al obtener facturas por estado: {str(e)}")
            return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0, "has_next": False, "has_prev": False}
    
    def get_facturas_vencidas(
        self,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        try:
            hoy = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            # Cambio: Buscar facturas "Emitida" en lugar de "Pendiente"
            query = {"fecha_vencimiento": {"$lt": hoy}, "estado": "Emitida"}
            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size
            
            facturas_list = list(
                self.collection.find(query).sort("fecha_vencimiento", 1).skip(skip).limit(page_size)
            )
            
            items = []
            for factura in facturas_list:
                factura["id"] = str(factura["_id"])
                del factura["_id"]
                factura = self._convert_datetime_to_date(factura)
                factura = self._populate_fletes(factura)
                items.append(factura)
            
            total_pages = math.ceil(total / page_size) if total > 0 else 0
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
            
        except Exception as e:
            logger.error(f"Error al obtener facturas vencidas: {str(e)}")
            return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0, "has_next": False, "has_prev": False}
    
    def get_facturas_por_vencer(
        self, 
        dias: int = 7,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        try:
            hoy = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            fecha_limite = hoy + timedelta(days=dias)
            # Cambio: Buscar facturas "Emitida" en lugar de "Pendiente"
            query = {"fecha_vencimiento": {"$gte": hoy, "$lte": fecha_limite}, "estado": "Emitida"}
            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size
            
            facturas_list = list(
                self.collection.find(query).sort("fecha_vencimiento", 1).skip(skip).limit(page_size)
            )
            
            items = []
            for factura in facturas_list:
                factura["id"] = str(factura["_id"])
                del factura["_id"]
                factura = self._convert_datetime_to_date(factura)
                factura = self._populate_fletes(factura)
                items.append(factura)
            
            total_pages = math.ceil(total / page_size) if total > 0 else 0
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
            
        except Exception as e:
            logger.error(f"Error al obtener facturas por vencer: {str(e)}")
            return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0, "has_next": False, "has_prev": False}

    def get_factura_by_id(self, factura_id: str) -> Optional[dict]:
        try:
            if not ObjectId.is_valid(factura_id):
                return None
            
            factura = self.collection.find_one({"_id": ObjectId(factura_id)})
            if factura:
                factura["id"] = str(factura["_id"])
                del factura["_id"]
                factura = self._convert_datetime_to_date(factura)
                factura = self._populate_fletes(factura)
            return factura
            
        except Exception as e:
            logger.error(f"Error al obtener factura: {str(e)}")
            return None
    
    def get_factura_by_numero(self, numero_factura: str) -> Optional[dict]:
        try:
            factura = self.collection.find_one({"numero_factura": numero_factura})
            if factura:
                factura["id"] = str(factura["_id"])
                del factura["_id"]
                factura = self._convert_datetime_to_date(factura)
                factura = self._populate_fletes(factura)
            return factura
            
        except Exception as e:
            logger.error(f"Error al obtener factura por número: {str(e)}")
            return None
    
    def update_factura(self, factura_id: str, update_data: dict) -> Optional[dict]:
        try:
            if not ObjectId.is_valid(factura_id):
                return None 
            
            update_dict = {k: v for k, v in update_data.items() if v is not None}
            
            if not update_dict:
                return self.get_factura_by_id(factura_id)
            
            # Convertir fechas
            date_fields = ['fecha_emision', 'fecha_vencimiento', 'fecha_pago']
            for field in date_fields:
                if field in update_dict and isinstance(update_dict[field], date):
                    if not isinstance(update_dict[field], datetime):
                        update_dict[field] = datetime.combine(update_dict[field], datetime.min.time())
            
            # Convertir monto_total
            if 'monto_total' in update_dict and isinstance(update_dict['monto_total'], Decimal):
                update_dict['monto_total'] = float(update_dict['monto_total'])
            
            update_dict["fecha_actualizacion"] = datetime.now()
            
            self.collection.update_one(
                {"_id": ObjectId(factura_id)},
                {"$set": update_dict}
            )
            
            return self.get_factura_by_id(factura_id)
            
        except Exception as e:
            logger.error(f"Error al actualizar factura: {str(e)}")
            raise
    
    def delete_factura(self, factura_id: str) -> bool:
        try:
            if not ObjectId.is_valid(factura_id):
                return False

            factura = self.collection.find_one({"_id": ObjectId(factura_id)})
            if not factura:
                return False

            fletes_ids = []

            # Obtener fletes asociados
            if "fletes" in factura and isinstance(factura["fletes"], list):
                for flete_ref in factura["fletes"]:
                    flete_id = flete_ref.get("id")
                    if ObjectId.is_valid(flete_id):
                        fletes_ids.append(ObjectId(flete_id))

            # Desmarcar fletes como facturados
            if fletes_ids:
                self.fletes_collection.update_many(
                    {"_id": {"$in": fletes_ids}},
                    {"$set": {"pertenece_a_factura": False, "factura_id": None, "codigo_factura": None}}
                )

            # Eliminar factura
            result = self.collection.delete_one({"_id": ObjectId(factura_id)})

            return result.deleted_count > 0

        except Exception as e:
            logger.error(f"Error al eliminar factura: {str(e)}")
            return False
    
    def marcar_como_pagada(self, factura_id: str, fecha_pago: Optional[date] = None) -> Optional[dict]:
        try:
            if not ObjectId.is_valid(factura_id):
                return None
            
            if fecha_pago is None:
                fecha_pago = date.today()
            
            update_data = {
                "estado": "Pagada",
                "fecha_pago": datetime.combine(fecha_pago, datetime.min.time()),
                "fecha_actualizacion": datetime.now()
            }
            
            self.collection.update_one(
                {"_id": ObjectId(factura_id)},
                {"$set": update_data}
            )
            
            return self.get_factura_by_id(factura_id)
            
        except Exception as e:
            logger.error(f"Error al marcar factura como pagada: {str(e)}")
            raise
    
    # def _crear_facturacion_gestion(self, factura_data: dict, numero_factura: str) -> dict:
    #     try:
    #         monto_total = Decimal(str(factura_data.get("monto_total", 0)))
    #         aplica_detraccion = monto_total > Decimal("400.0")
            
    #         if aplica_detraccion:
    #             tasa_detraccion = Decimal("4.0")
    #             monto_detraccion = (monto_total * tasa_detraccion / Decimal("100")).quantize(Decimal("0.01"))
    #             estado_detraccion = "Pendiente"
    #         else:
    #             tasa_detraccion = Decimal("0.0")
    #             monto_detraccion = Decimal("0.0")
    #             estado_detraccion = "No Aplica"
            
    #         monto_neto = monto_total - monto_detraccion
            
    #         facturacion_gestion = {
    #             "codigo_factura": numero_factura,
    #             "estado_detraccion": estado_detraccion,
    #             "tasa_detraccion": float(tasa_detraccion),
    #             "monto_detraccion": float(monto_detraccion),
    #             "nro_constancia_detraccion": None,
    #             "fecha_pago_detraccion": None,
    #             "estado_pago_neto": "Pendiente",
    #             "monto_neto": float(monto_neto),
    #             "monto_pagado_acumulado": 0.0,
    #             "banco_destino": None,
    #             "cuenta_bancaria_destino": None,
    #             "nro_operacion_pago_neto": None,
    #             "fecha_probable_pago": None,
    #             "prioridad": "Media",
    #             "centro_costo": None,
    #             "responsable_gestion": None,
    #             "observaciones_admin": None,
    #             "ultima_actualizacion": datetime.now()
    #         }
            
    #         logger.info(f"Registro de facturación-gestión creado para factura {numero_factura}")
    #         return facturacion_gestion
            
    #     except Exception as e:
    #         logger.error(f"Error al crear facturación-gestión: {str(e)}")
    #         raise

    # def emitir_factura(self, factura_id: str, numero_factura: str, fecha_emision: Optional[date] = None, fecha_vencimiento: Optional[date] = None) -> Optional[dict]:
    #     try:
    #         if not ObjectId.is_valid(factura_id):
    #             return None
            
    #         existing_factura = self.collection.find_one({"numero_factura": numero_factura})
    #         if existing_factura and str(existing_factura["_id"]) != factura_id:
    #             raise ValueError(f"El número de factura {numero_factura} ya está registrado")
            
    #         factura_actual = self.collection.find_one({"_id": ObjectId(factura_id)})
    #         if not factura_actual:
    #             raise ValueError(f"Factura con ID {factura_id} no encontrada")
            
    #         if fecha_emision is None:
    #             fecha_emision = date.today()
            
    #         if fecha_vencimiento is None:
    #             fecha_vencimiento = fecha_emision + timedelta(days=30)
            
    #         update_data = {
    #             "numero_factura": numero_factura,
    #             "fecha_emision": datetime.combine(fecha_emision, datetime.min.time()),
    #             "fecha_vencimiento": datetime.combine(fecha_vencimiento, datetime.min.time()),
    #             "es_borrador": False,
    #             "estado": "Emitida",
    #             "fecha_actualizacion": datetime.now()
    #         }
            
    #         self.collection.update_one(
    #             {"_id": ObjectId(factura_id)},
    #             {"$set": update_data}
    #         )
            
    #         facturacion_gestion = self._crear_facturacion_gestion(factura_actual, numero_factura)
            
    #         db = self.collection.database
    #         facturacion_gestion_collection = db["facturacion_gestion"]
    #         facturacion_gestion_collection.insert_one(facturacion_gestion)
            
    #         logger.info(f"Factura {numero_factura} emitida exitosamente")
            
    #         return self.get_factura_by_id(factura_id)
            
    #     except Exception as e:
    #         logger.error(f"Error al emitir factura: {str(e)}")
    #         raise
    
    def _obtener_datos_completos_snapshot(self, factura_data: dict, numero_factura: str) -> dict:
        db = self.collection.database
        
        # Manejo seguro de la lista de fletes para evitar el error 'list' object has no attribute 'get'
        fletes_raw = factura_data.get("fletes", [])
        fletes_ids = []
        for f in fletes_raw:
            if isinstance(f, dict) and "id" in f:
                fletes_ids.append(f["id"])
            elif isinstance(f, (str, ObjectId)):
                fletes_ids.append(f)

        snapshot_fletes = []
        for f_id in fletes_ids:
            # 1. Buscar el Flete
            flete_doc = db["fletes"].find_one({"_id": ObjectId(f_id)})
            if not flete_doc: 
                continue
            
            # 2. Buscar el Servicio del flete en la colección correcta
            srv_id = flete_doc.get("servicio_id")
            servicio_doc = db["servicio_principal"].find_one({"_id": ObjectId(srv_id)})
            
            if servicio_doc:
                # 3. Mapear info de Servicio con tus rutas de datos actualizadas
                # Nota: Ajustado el acceso a conductor que es una lista en tu JSON
                conductor_data = servicio_doc.get("conductor", [{}])
                nombre_conductor = conductor_data[0].get("nombre", "") if isinstance(conductor_data, list) and len(conductor_data) > 0 else ""

                srv_snap = {
                    "codigo_servicio": servicio_doc.get("codigo_servicio_principal"),
                    "nombre_cliente": servicio_doc.get("cliente", {}).get("nombre"),
                    "nombre_cuenta": servicio_doc.get("cuenta", {}).get("nombre"),
                    "nombre_proveedor": servicio_doc.get("proveedor", {}).get("nombre"),
                    "placa_flota": servicio_doc.get("flota", {}).get("placa"),
                    "nombre_conductor": nombre_conductor,
                    "nombre_auxiliar": servicio_doc.get("auxiliar", [{}])[0].get("nombres_completos", "") if servicio_doc.get("auxiliar") else "",
                    "m3": str(servicio_doc.get("m3", "")),
                    "tn": str(servicio_doc.get("tn", "")),
                    "tipo_servicio": servicio_doc.get("tipo_servicio"),
                    "modalidad": servicio_doc.get("modalidad_servicio"),
                    "zona": servicio_doc.get("zona"),
                    "fecha_servicio": servicio_doc.get("fecha_servicio"),
                    "fecha_salida": servicio_doc.get("fecha_salida"),
                    "gia_rr": servicio_doc.get("gia_rr"),
                    "gia_rt": servicio_doc.get("gia_rt"),
                    "origen": servicio_doc.get("origen"),
                    "destino": servicio_doc.get("destino")
                }
                
                # 4. Mapear info de Flete
                snapshot_fletes.append({
                    "codigo_flete": flete_doc.get("codigo_flete"),
                    "monto_flete": float(flete_doc.get("monto_flete", 0)),
                    "servicio": srv_snap
                })

        # 5. Retornar el Snapshot de Factura completo
        return {
            "numero_factura": numero_factura,
            "fecha_emision": factura_data.get("fecha_emision"),
            "fecha_vencimiento": factura_data.get("fecha_vencimiento"),
            "monto_total": float(factura_data.get("monto_total", 0)),
            "fletes": snapshot_fletes
        }

    def _crear_facturacion_gestion(self, factura_data: dict, numero_factura: str, snapshot_data: dict) -> dict:
        try:
            monto_total = Decimal(str(factura_data.get("monto_total", 0)))
            aplica_detraccion = monto_total > Decimal("400.0")
            
            if aplica_detraccion:
                tasa_detraccion = Decimal("4.0")
                monto_detraccion = (monto_total * tasa_detraccion / Decimal("100")).quantize(Decimal("0.01"))
                estado_detraccion = "Pendiente"
            else:
                tasa_detraccion = Decimal("0.0")
                monto_detraccion = Decimal("0.0")
                estado_detraccion = "No Aplica"
            
            monto_neto = monto_total - monto_detraccion
            
            return {
                "codigo_factura": numero_factura,
                "datos_completos": snapshot_data,  # Insertamos el snapshot inmutable
                "estado_detraccion": estado_detraccion,
                "tasa_detraccion": float(tasa_detraccion),
                "monto_detraccion": float(monto_detraccion),
                "nro_constancia_detraccion": None,
                "fecha_pago_detraccion": None,
                "estado_pago_neto": "Pendiente",
                "monto_neto": float(monto_neto),
                "monto_pagado_acumulado": 0.0,
                "banco_destino": None,
                "cuenta_bancaria_destino": None,
                "nro_operacion_pago_neto": None,
                "fecha_probable_pago": None,
                "prioridad": "Media",
                "centro_costo": None,
                "responsable_gestion": None,
                "observaciones_admin": None,
                "ultima_actualizacion": datetime.now()
            }
        except Exception as e:
            logger.error(f"Error al crear facturación-gestión: {str(e)}")
            raise

    def emitir_factura(self, factura_id: str, numero_factura: str, fecha_emision: Optional[date] = None, fecha_vencimiento: Optional[date] = None) -> Optional[dict]:
        try:
            if not ObjectId.is_valid(factura_id): return None
            
            # Validaciones de existencia y duplicados...
            factura_actual = self.collection.find_one({"_id": ObjectId(factura_id)})
            if not factura_actual: raise ValueError("Factura no encontrada")
            
            if fecha_emision is None: fecha_emision = date.today()
            if fecha_vencimiento is None: fecha_vencimiento = fecha_emision + timedelta(days=30)
            
            # 1. Primero preparamos los datos temporales para el snapshot
            factura_actual["fecha_emision"] = datetime.combine(fecha_emision, datetime.min.time())
            factura_actual["fecha_vencimiento"] = datetime.combine(fecha_vencimiento, datetime.min.time())
            
            # 2. GENERAR EL SNAPSHOT (Trae data de fletes y servicios)
            snapshot = self._obtener_datos_completos_snapshot(factura_actual, numero_factura)
            
            # 3. Actualizar la Factura a Emitida
            update_data = {
                "numero_factura": numero_factura,
                "fecha_emision": factura_actual["fecha_emision"],
                "fecha_vencimiento": factura_actual["fecha_vencimiento"],
                "es_borrador": False,
                "estado": "Emitida",
                "fecha_actualizacion": datetime.now()
            }
            self.collection.update_one({"_id": ObjectId(factura_id)}, {"$set": update_data})
            
            # 4. Crear Gestión con el Snapshot incluido
            facturacion_gestion = self._crear_facturacion_gestion(factura_actual, numero_factura, snapshot)
            
            db = self.collection.database
            db["facturacion_gestion"].insert_one(facturacion_gestion)
            
            logger.info(f"Factura {numero_factura} emitida con Snapshot completo")
            return self.get_factura_by_id(factura_id)
            
        except Exception as e:
            logger.error(f"Error al emitir factura: {str(e)}")
            raise


    def export_to_excel(self, filter_params: Optional[FacturacionFilter] = None) -> BytesIO:
        try:
            query = self._build_query(filter_params)
            facturas_list = list(self.collection.find(query).sort("fecha_emision", -1))
            
            if not facturas_list:
                df = pd.DataFrame(columns=[
                    "Código Factura", "Número Factura", "Fecha Emisión", "Fecha Vencimiento",
                    "Fecha Pago", "Estado", "Es Borrador", "Monto Total", "Moneda", 
                    "Fletes IDs", "Descripción"
                ])
            else:
                excel_data = []
                for factura in facturas_list:
                    factura_converted = self._convert_datetime_to_date(factura)
                    
                    fletes_ids = []
                    if "fletes" in factura_converted and isinstance(factura_converted["fletes"], list):
                        for flete_ref in factura_converted["fletes"]:
                            if isinstance(flete_ref, dict) and "id" in flete_ref:
                                fletes_ids.append(flete_ref["id"])
                    
                    excel_data.append({
                        "Código Factura": factura_converted.get("codigo_factura", ""),
                        "Número Factura": factura_converted.get("numero_factura", ""),
                        "Fecha Emisión": factura_converted.get("fecha_emision", ""),
                        "Fecha Vencimiento": factura_converted.get("fecha_vencimiento", ""),
                        "Fecha Pago": factura_converted.get("fecha_pago", ""),
                        "Estado": factura_converted.get("estado", ""),
                        "Es Borrador": factura_converted.get("es_borrador", True),
                        "Monto Total": factura_converted.get("monto_total", 0),
                        "Moneda": factura_converted.get("moneda", ""),
                        "Fletes IDs": ", ".join(fletes_ids),
                        "Descripción": factura_converted.get("descripcion", "")
                    })
                
                df = pd.DataFrame(excel_data)
            
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Facturas')
            
            output.seek(0)
            return output
            
        except Exception as e:
            logger.error(f"Error al exportar a Excel: {str(e)}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        try:
            # 1. Conteo total de documentos (todos los estados)
            total = self.collection.count_documents({})
            
            # 2. Pipeline para montos por estado
            # Actualizado para incluir "Emitida" en lugar de "Pendiente"
            pipeline_estado = [
                {"$match": {"estado": {"$in": ["Emitida", "Pagada"]}}},
                {"$group": {
                    "_id": "$estado", 
                    "count": {"$sum": 1}, 
                    "total_monto": {"$sum": "$monto_total"}
                }}
            ]
            
            estados = {}
            for result in self.collection.aggregate(pipeline_estado):
                estados[result["_id"]] = {
                    "count": result["count"],
                    "total_monto": result["total_monto"]
                }
            
            # 3. Lógica de fechas para el mes actual
            mes_inicio = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if datetime.now().month == 12:
                mes_fin = datetime.now().replace(year=datetime.now().year + 1, month=1, day=1)
            else:
                mes_fin = datetime.now().replace(month=datetime.now().month + 1, day=1)
            mes_fin = mes_fin - timedelta(days=1)
            mes_fin = mes_fin.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # 4. Conteo de facturas del mes (Solo Emitida y Pagada)
            facturas_mes_actual = self.collection.count_documents({
                "fecha_emision": {"$gte": mes_inicio, "$lte": mes_fin},
                "estado": {"$in": ["Emitida", "Pagada"]}
            })
            
            # 5. Pipeline para monto total del mes (Solo Emitida y Pagada)
            pipeline_mes = [
                {
                    "$match": {
                        "fecha_emision": {"$gte": mes_inicio, "$lte": mes_fin},
                        "estado": {"$in": ["Emitida", "Pagada"]}
                    }
                },
                {"$group": {"_id": None, "total": {"$sum": "$monto_total"}}}
            ]
            
            total_mes = 0
            for result in self.collection.aggregate(pipeline_mes):
                total_mes = result["total"]
            
            # 6. Alertas (Facturas vencidas y por vencer no incluyen las Pagadas)
            hoy = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Cambio: Buscar facturas "Emitida" en lugar de "Pendiente"
            facturas_vencidas = self.collection.count_documents({
                "fecha_vencimiento": {"$lt": hoy},
                "estado": "Emitida"  # Cambiado de "Pendiente" a "Emitida"
            })
            
            fecha_limite = hoy + timedelta(days=7)
            # Cambio: Buscar facturas "Emitida" en lugar de "Pendiente"
            facturas_por_vencer = self.collection.count_documents({
                "fecha_vencimiento": {"$gte": hoy, "$lte": fecha_limite},
                "estado": "Emitida"  # Cambiado de "Pendiente" a "Emitida"
            })
            
            facturas_borrador = self.collection.count_documents({"es_borrador": True})
            
            return {
                "total": total,
                "por_estado": estados,
                "facturas_mes_actual": facturas_mes_actual,
                "total_facturado_mes": total_mes,
                "facturas_vencidas": facturas_vencidas,
                "facturas_por_vencer": facturas_por_vencer,
                "facturas_borrador": facturas_borrador
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {str(e)}")
            return {}

# Factory function para crear instancia del servicio
def get_facturacion_service():
    db = get_database()
    return FacturacionService(db)