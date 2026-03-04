from app.modules.fletes.fletes_services import FleteService
from datetime import datetime
from typing import Optional
from math import ceil


class MonitoreoGerencia:
    def __init__(self, db):
        self.db = db
        self.flete_service = FleteService(db)
        self.collection = db["fletes"]

    def facturacion_de_placas(
        self,
        placa: Optional[str] = None,
        fecha_inicio: Optional[str] = None,
        fecha_fin: Optional[str] = None,
        page: int = 1,
        page_size: int = 50
    ) -> dict:
        fecha_desde = datetime.strptime(fecha_inicio, "%Y-%m-%d") if fecha_inicio else None
        fecha_hasta = datetime.strptime(fecha_fin, "%Y-%m-%d") if fecha_fin else None

        return self.flete_service.get_fletes_advanced(
            placa=placa,
            fecha_servicio_desde=fecha_desde,
            fecha_servicio_hasta=fecha_hasta,
            page=page,
            page_size=page_size
        )

    def get_metricas_placa(
        self,
        placa: Optional[str] = None,
        fecha_inicio: Optional[str] = None,
        fecha_fin: Optional[str] = None,
    ) -> dict:

        fecha_desde = datetime.strptime(fecha_inicio, "%Y-%m-%d").replace(hour=0, minute=0, second=0) if fecha_inicio else None
        fecha_hasta = datetime.strptime(fecha_fin, "%Y-%m-%d").replace(hour=23, minute=59, second=59) if fecha_fin else None

        pipeline = []

        # Join con servicios
        pipeline.append({
            "$lookup": {
                "from": "servicio_principal",
                "let": {"servicio_id_str": "$servicio_id"},
                "pipeline": [
                    {"$addFields": {"servicio_id_str": {"$toString": "$_id"}}},
                    {"$match": {"$expr": {"$eq": ["$servicio_id_str", "$$servicio_id_str"]}}}
                ],
                "as": "servicio"
            }
        })

        pipeline.append({
            "$unwind": {"path": "$servicio", "preserveNullAndEmptyArrays": True}
        })

        # Filtros
        match = {}
        if placa and placa.strip():
            match["servicio.flota.placa"] = {"$regex": placa, "$options": "i"}
        if fecha_desde:
            match.setdefault("servicio.fecha_servicio", {})["$gte"] = fecha_desde
        if fecha_hasta:
            match.setdefault("servicio.fecha_servicio", {})["$lte"] = fecha_hasta
        if match:
            pipeline.append({"$match": match})

        # $facet con todo incluyendo agrupamiento por placa
        pipeline.append({
            "$facet": {

                "resumen_montos": [
                    {
                        "$group": {
                            "_id": None,
                            "total_fletes":   {"$sum": 1},
                            "monto_total":    {"$sum": "$monto_flete"},
                            "monto_promedio": {"$avg": "$monto_flete"},
                            "monto_maximo":   {"$max": "$monto_flete"},
                            "monto_minimo":   {"$min": "$monto_flete"},
                        }
                    }
                ],

                "por_estado_flete": [
                    {
                        "$group": {
                            "_id": "$estado_flete",
                            "cantidad": {"$sum": 1},
                            "monto":    {"$sum": "$monto_flete"}
                        }
                    },
                    {"$sort": {"cantidad": -1}}
                ],

                "facturados_vs_pendientes": [
                    {
                        "$group": {
                            "_id": "$pertenece_a_factura",
                            "cantidad": {"$sum": 1},
                            "monto":    {"$sum": "$monto_flete"}
                        }
                    }
                ],

                "por_mes": [
                    {
                        "$group": {
                            "_id": {
                                "anio": {"$year": "$servicio.fecha_servicio"},
                                "mes":  {"$month": "$servicio.fecha_servicio"}
                            },
                            "cantidad": {"$sum": 1},
                            "monto":    {"$sum": "$monto_flete"}
                        }
                    },
                    {"$sort": {"_id.anio": 1, "_id.mes": 1}}
                ],

                "por_tipo_servicio": [
                    {
                        "$group": {
                            "_id": "$servicio.tipo_servicio",
                            "cantidad": {"$sum": 1},
                            "monto":    {"$sum": "$monto_flete"}
                        }
                    },
                    {"$sort": {"monto": -1}}
                ],

                # ↓ NUEVO: agrupamiento completo por placa sobre los 500 fletes
                "por_placa": [
                    {
                        "$group": {
                            "_id": "$servicio.flota.placa",
                            "vehiculo":      {"$first": "$servicio.flota.tipo_vehiculo"},
                            "cantidad":      {"$sum": 1},
                            "monto_total":   {"$sum": "$monto_flete"},
                            "facturados":    {"$sum": {"$cond": ["$pertenece_a_factura", 1, 0]}},
                            "pendientes":    {"$sum": {"$cond": ["$pertenece_a_factura", 0, 1]}},
                            # Acumular clientes para sacar el principal
                            "clientes":      {"$push": "$servicio.cliente.nombre"},
                            # Acumular fletes para el desglose lateral
                            "fletes": {
                                "$push": {
                                    "codigo_flete":       "$codigo_flete",
                                    "monto_flete":        "$monto_flete",
                                    "pertenece_a_factura":"$pertenece_a_factura",
                                    "origen":             "$servicio.origen",
                                    "destino":            "$servicio.destino",
                                    "fecha_servicio":     "$servicio.fecha_servicio",
                                    "estado_flete":       "$estado_flete"
                                }
                            }
                        }
                    },
                    {"$sort": {"monto_total": -1}}
                ]
            }
        })

        result = list(self.collection.aggregate(pipeline))
        if not result:
            return self._empty_metricas()

        data = result[0]

        # Formatear resumen
        resumen_raw = data["resumen_montos"][0] if data["resumen_montos"] else {}
        resumen = {
            "total_fletes":   resumen_raw.get("total_fletes", 0),
            "monto_total":    round(resumen_raw.get("monto_total", 0), 2),
            "monto_promedio": round(resumen_raw.get("monto_promedio", 0), 2),
            "monto_maximo":   round(resumen_raw.get("monto_maximo", 0), 2),
            "monto_minimo":   round(resumen_raw.get("monto_minimo", 0), 2),
        }

        # Formatear facturación
        facturacion = {"facturados": {"cantidad": 0, "monto": 0}, "pendientes": {"cantidad": 0, "monto": 0}}
        for item in data["facturados_vs_pendientes"]:
            key = "facturados" if item["_id"] else "pendientes"
            facturacion[key] = {"cantidad": item["cantidad"], "monto": round(item["monto"], 2)}

        # Formatear por mes
        MESES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
        por_mes = [
            {
                "periodo":  f"{MESES[i['_id']['mes'] - 1]} {i['_id']['anio']}",
                "anio":     i["_id"]["anio"],
                "mes":      i["_id"]["mes"],
                "cantidad": i["cantidad"],
                "monto":    round(i["monto"], 2)
            }
            for i in data["por_mes"]
        ]

        # ↓ NUEVO: formatear por_placa con cliente principal
        def get_cliente_principal(clientes: list) -> str:
            if not clientes:
                return "N/A"
            # El cliente que más aparece en los fletes de esa placa
            conteo = {}
            for c in clientes:
                if c:
                    conteo[c] = conteo.get(c, 0) + 1
            return max(conteo, key=conteo.get) if conteo else "N/A"

        por_placa = [
            {
                "placa":             item["_id"] or "SIN PLACA",
                "vehiculo":          item.get("vehiculo") or "N/A",
                "cantidad_fletes":   item["cantidad"],
                "monto_total":       round(item["monto_total"], 2),
                "facturados":        item["facturados"],
                "pendientes":        item["pendientes"],
                "cliente_principal": get_cliente_principal(item["clientes"]),
                "fletes":            item["fletes"]  # desglose lateral listo
            }
            for item in data["por_placa"]
        ]

        return {
            "placa":            placa if placa else "TODAS",
            "periodo":          {"desde": fecha_inicio, "hasta": fecha_fin},
            "resumen":          resumen,
            "facturacion":      facturacion,
            "por_estado_flete": [
                {"estado": i["_id"] or "Sin estado", "cantidad": i["cantidad"], "monto": round(i["monto"], 2)}
                for i in data["por_estado_flete"]
            ],
            "por_tipo_servicio": [
                {"tipo": i["_id"] or "Sin tipo", "cantidad": i["cantidad"], "monto": round(i["monto"], 2)}
                for i in data["por_tipo_servicio"]
            ],
            "por_mes":   por_mes,
            "por_placa": por_placa  # ← listo para la tabla del frontend
        }


    def _empty_metricas(self) -> dict:
        return {
            "resumen": {"total_fletes": 0, "monto_total": 0, "monto_promedio": 0, "monto_maximo": 0, "monto_minimo": 0},
            "facturacion": {"facturados": {"cantidad": 0, "monto": 0}, "pendientes": {"cantidad": 0, "monto": 0}},
            "por_estado_flete": [],
            "por_tipo_servicio": [],
            "por_mes": []
        }

    def get_reporte_completo(
        self,
        placa: Optional[str] = None,
        fecha_inicio: Optional[str] = None,
        fecha_fin: Optional[str] = None,
        page: int = 1,
        page_size: int = 50
    ) -> dict:
        """
        Combina el listado de fletes + métricas en una sola respuesta.
        """
        
        fletes = self.facturacion_de_placas(
            placa=placa,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            page=page,
            page_size=page_size
        )

        metricas = self.get_metricas_placa(
            placa=placa,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin
        )

        return {
            "metricas": metricas,
            "fletes": fletes
        }

    def get_reporte_pendientes_por_placa(
        self,
        placa: Optional[str] = None,
        fecha_servicio_desde: Optional[datetime] = None,
        fecha_servicio_hasta: Optional[datetime] = None
    ):
        """
        Reporte consolidado de fletes agrupado por placa.
        Aplica para todos los fletes (no filtra por estado ni facturación).
        """
        try:
            # Filtro base vacío — todos los fletes
            match_flete = {}

            # Filtro de fecha para el servicio
            filtro_fecha = {}
            if fecha_servicio_desde or fecha_servicio_hasta:
                filtro_fecha["fecha_servicio"] = {}
                if fecha_servicio_desde:
                    filtro_fecha["fecha_servicio"]["$gte"] = fecha_servicio_desde
                if fecha_servicio_hasta:
                    filtro_fecha["fecha_servicio"]["$lte"] = fecha_servicio_hasta

            pipeline = [
                {"$match": match_flete},
                {
                    "$lookup": {
                        "from": "servicio_principal",
                        "let": {"serv_id": "$servicio_id"},
                        "pipeline": [
                            {
                                "$match": {
                                    "$and": [
                                        {"$expr": {"$eq": ["$_id", {"$toObjectId": "$$serv_id"}]}},
                                        filtro_fecha if filtro_fecha else {}
                                    ]
                                }
                            }
                        ],
                        "as": "info_servicio"
                    }
                },
                # Elimina fletes cuyo servicio no cumplió el filtro de fecha
                {"$unwind": "$info_servicio"},

                # Filtro opcional por placa
                {
                    "$match": {
                        "info_servicio.flota.placa": {
                            "$regex": placa, "$options": "i"
                        } if placa else {"$exists": True}
                    }
                },

                {
                    "$group": {
                        "_id": "$info_servicio.flota.placa",
                        "placa":             {"$first": "$info_servicio.flota.placa"},
                        "vehiculo":          {"$first": "$info_servicio.flota.tipo_vehiculo"},
                        "monto_total":       {"$sum": "$monto_flete"},
                        "cantidad_fletes":   {"$sum": 1},
                        "facturados":        {"$sum": {"$cond": ["$pertenece_a_factura", 1, 0]}},
                        "pendientes":        {"$sum": {"$cond": ["$pertenece_a_factura", 0, 1]}},
                        "monto_facturado":   {"$sum": {"$cond": ["$pertenece_a_factura", "$monto_flete", 0]}},
                        "monto_pendiente":   {"$sum": {"$cond": ["$pertenece_a_factura", 0, "$monto_flete"]}},
                        "cliente_principal": {"$first": "$info_servicio.cliente.nombre"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "placa":           1,
                        "vehiculo":        1,
                        "cantidad_fletes": 1,
                        "monto_total":     {"$round": ["$monto_total", 2]},
                        "facturados":      1,
                        "pendientes":      1,
                        "monto_facturado": {"$round": ["$monto_facturado", 2]},
                        "monto_pendiente": {"$round": ["$monto_pendiente", 2]},
                        "cliente_principal": 1,
                    }
                },
                {"$sort": {"monto_total": -1}}
            ]

            return list(self.collection.aggregate(pipeline))

        except Exception as e:
            logger.error(f"Error en reporte por placa: {str(e)}")
            return []