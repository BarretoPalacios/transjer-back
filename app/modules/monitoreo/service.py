from datetime import datetime
from typing import Optional
from math import ceil
import re

class MonitoreoGerencia:
    def __init__(self, db):
        self.db = db
        self.collection = db["fletes"]

    def get_fletes_and_metrics(
        self,
        codigo_flete: Optional[str] = None,
        estado_flete: Optional[str] = None,
        pertenece_a_factura: Optional[bool] = None,
        codigo_factura: Optional[str] = None,
        monto_min: Optional[float] = None,
        monto_max: Optional[float] = None,
        cliente: Optional[str] = None,
        placa: Optional[str] = None,
        conductor: Optional[str] = None,
        tipo_servicio: Optional[str] = None,
        zona: Optional[str] = None,
        estado_servicio: Optional[str] = None,
        fecha_servicio_desde: Optional[datetime] = None,
        fecha_servicio_hasta: Optional[datetime] = None,
        fecha_creacion_desde: Optional[datetime] = None,
        fecha_creacion_hasta: Optional[datetime] = None,
        page: int = 1,
        page_size: int = 10
    ) -> dict:
        try:
            pipeline = []
            match_flete = {}

            def safe_regex(value):
                return {"$regex": re.escape(value), "$options": "i"}

            if codigo_flete: match_flete["codigo_flete"] = safe_regex(codigo_flete)
            if estado_flete: match_flete["estado_flete"] = estado_flete
            if pertenece_a_factura is not None: match_flete["pertenece_a_factura"] = pertenece_a_factura
            if codigo_factura: match_flete["codigo_factura"] = safe_regex(codigo_factura)
            
            if monto_min is not None or monto_max is not None:
                match_flete["monto_flete"] = {}
                if monto_min is not None: match_flete["monto_flete"]["$gte"] = monto_min
                if monto_max is not None: match_flete["monto_flete"]["$lte"] = monto_max

            if fecha_creacion_desde or fecha_creacion_hasta:
                match_flete["fecha_creacion"] = {}
                if fecha_creacion_desde: match_flete["fecha_creacion"]["$gte"] = fecha_creacion_desde
                if fecha_creacion_hasta: match_flete["fecha_creacion"]["$lte"] = fecha_creacion_hasta

            if match_flete:
                pipeline.append({"$match": match_flete})

            pipeline.append({
                "$lookup": {
                    "from": "servicio_principal",
                    "let": {"servicio_id_obj": "$servicio_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": ["$_id", {"$toObjectId": "$$servicio_id_obj"}]
                                }
                            }
                        }
                    ],
                    "as": "servicio"
                }
            })
            
            pipeline.append({"$unwind": {"path": "$servicio", "preserveNullAndEmptyArrays": True}})

            match_servicio = {}
            if placa:
                placa_clean = placa.replace("-", "").strip()
                letras = placa_clean[:3]
                numeros = placa_clean[3:]
                regex_placa = f"^{letras}-?{numeros}$"
                match_servicio["servicio.flota.placa"] = {"$regex": regex_placa, "$options": "i"}

            if cliente:
                match_servicio["$or"] = [
                    {"servicio.cliente.nombre": safe_regex(cliente)},
                    {"servicio.cliente.razon_social": safe_regex(cliente)},
                    {"servicio.cuenta.nombre": safe_regex(cliente)}
                ]

            if conductor:
                match_servicio["$or"] = [
                    {"servicio.conductor.nombres_completos": safe_regex(conductor)},
                    {"servicio.conductor.nombre": safe_regex(conductor)}
                ]

            if tipo_servicio: match_servicio["servicio.tipo_servicio"] = tipo_servicio
            if zona: match_servicio["servicio.zona"] = safe_regex(zona)
            if estado_servicio: match_servicio["servicio.estado"] = estado_servicio

            if fecha_servicio_desde:
                match_servicio.setdefault("servicio.fecha_servicio", {})["$gte"] = fecha_servicio_desde
            if fecha_servicio_hasta:
                match_servicio.setdefault("servicio.fecha_servicio", {})["$lte"] = fecha_servicio_hasta

            if match_servicio:
                pipeline.append({"$match": match_servicio})

            stats_pipeline = pipeline.copy()
            stats_pipeline.append({
                "$group": {
                    "_id": None,
                    "total_count": {"$sum": 1},
                    "monto_total": {"$sum": "$monto_flete"},
                    "total_pendientes": {
                        "$sum": {"$cond": [{"$eq": ["$estado_flete", "PENDIENTE"]}, 1, 0]}
                    },
                    "valorizados_con_factura": {
                        "$sum": {"$cond": [{"$eq": ["$pertenece_a_factura", True]}, 1, 0]}
                    },
                    "valorizados_sin_factura": {
                        "$sum": {"$cond": [{"$eq": ["$pertenece_a_factura", False]}, 1, 0]}
                    }
                }
            })
            
            stats_result = list(self.collection.aggregate(stats_pipeline))
            stats = stats_result[0] if stats_result else {
                "total_count": 0, "monto_total": 0, "total_pendientes": 0, 
                "valorizados_con_factura": 0, "valorizados_sin_factura": 0
            }

            pipeline.append({"$sort": {"_id": -1}})
            skip = (page - 1) * page_size
            pipeline.append({"$skip": skip})
            pipeline.append({"$limit": page_size})

            pipeline.append({
                "$project": {
                    "_id": 0,
                    "id": {"$toString": "$_id"},
                    "codigo_flete": 1,
                    "monto_flete": 1,
                    "estado_flete": 1,
                    "pertenece_a_factura": 1,
                    "codigo_factura": 1,
                    "fecha_creacion": 1,
                    "servicio": {
                        "$cond": {
                            "if": {"$gt": ["$servicio", None]},
                            "then": {
                                "id": {"$toString": "$servicio._id"},
                                "codigo_servicio_principal": "$servicio.codigo_servicio_principal",
                                "tipo_servicio": "$servicio.tipo_servicio",
                                "modalidad_servicio": "$servicio.modalidad_servicio",
                                "zona": "$servicio.zona",
                                "fecha_servicio": "$servicio.fecha_servicio",
                                "fecha_salida": "$servicio.fecha_salida",
                                "hora_cita": "$servicio.hora_cita",
                                "estado": "$servicio.estado",
                                "descripcion": "$servicio.descripcion",
                                "origen": "$servicio.origen",
                                "destino": "$servicio.destino",
                                "m3": "$servicio.m3",
                                "tn": "$servicio.tn",
                                "cliente": "$servicio.cliente",
                                "cuenta": "$servicio.cuenta",
                                "proveedor": "$servicio.proveedor",
                                "flota": "$servicio.flota",
                                "conductor": "$servicio.conductor",
                                "auxiliar": "$servicio.auxiliar"
                            },
                            "else": None
                        }
                    }
                }
            })

            fletes = list(self.collection.aggregate(pipeline))
            total = stats.get("total_count", 0)
            total_pages = ceil(total / page_size) if page_size > 0 else 0

            return {
                "items": fletes,
                "metrics": {
                    "total_fletes": total,
                    "monto_total_acumulado": round(float(stats.get("monto_total", 0)), 2),
                    "total_pendientes": stats.get("total_pendientes", 0),
                    "valorizados_con_factura": stats.get("valorizados_con_factura", 0),
                    "valorizados_sin_factura": stats.get("valorizados_sin_factura", 0)
                },
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1
                }
            }

        except Exception as e:
            print(f"Error: {str(e)}")
            raise

    