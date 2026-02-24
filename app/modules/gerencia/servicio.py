import logging
from datetime import datetime, date, timedelta , time
from typing import Dict, Any, Optional, List
from pymongo.collection import Collection
from math import ceil
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
from io import BytesIO
from openpyxl.utils import get_column_letter
from bson import ObjectId
from app.core.database import get_database
from app.modules.fletes.fletes_services import FleteService
from app.modules.fletes.fletes_schemas import FleteFilter

logger = logging.getLogger(__name__)

class GerenciaService:
    def __init__(self, db):
        self.db = db
        self.fletes_collection = db["fletes"]
        self.servicio_principal_collection = db["servicio_principal"]
        self.collection = db["facturacion_gestion"]  # Colección de gestiones
        self.facturas_collection = db["facturacion"]  # Colección de facturas   


    def analisis_de_fletes(self, mes: Optional[int] = None, anio: Optional[int] = None) -> Dict[str, Any]:
            try:
                pipeline = []

                pipeline.append({
                    "$lookup": {
                        "from": "servicio_principal",
                        "let": {"serv_id": "$servicio_id"},
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$eq": ["$_id", {"$toObjectId": "$$serv_id"}]
                                    }
                                }
                            },
                            {"$project": {"fecha_servicio": 1}}
                        ],
                        "as": "info_servicio"
                    }
                })

                pipeline.append({"$unwind": "$info_servicio"})

                if mes is not None and anio is not None:
                    mes, anio = int(mes), int(anio)
                    fecha_inicio = datetime(anio, mes, 1)
                    fecha_fin = datetime(anio + 1, 1, 1) if mes == 12 else datetime(anio, mes + 1, 1)

                    pipeline.append({
                        "$match": {
                            "info_servicio.fecha_servicio": {"$gte": fecha_inicio, "$lt": fecha_fin}
                        }
                    })

                pipeline.append({
                    "$group": {
                        "_id": None,
                        "conteo_total": {"$sum": 1},
                        "cant_pendiente": {"$sum": {"$cond": [{"$eq": ["$estado_flete", "PENDIENTE"]}, 1, 0]}},
                        "monto_pendiente": {
                            "$sum": {"$cond": [{"$eq": ["$estado_flete", "PENDIENTE"]}, {"$toDouble": "$monto_flete"}, 0]}
                        },
                        "cant_val_sin_fac": {
                            "$sum": {"$cond": [{"$and": [{"$eq": ["$estado_flete", "VALORIZADO"]}, {"$eq": ["$pertenece_a_factura", False]}]}, 1, 0]}
                        },
                        "monto_val_sin_fac": {
                            "$sum": {"$cond": [{"$and": [{"$eq": ["$estado_flete", "VALORIZADO"]}, {"$eq": ["$pertenece_a_factura", False]}]}, {"$toDouble": "$monto_flete"}, 0]}
                        },
                        "cant_val_con_fac": {
                            "$sum": {"$cond": [{"$and": [{"$eq": ["$estado_flete", "VALORIZADO"]}, {"$eq": ["$pertenece_a_factura", True]}]}, 1, 0]}
                        },
                        "monto_val_con_fac": {
                            "$sum": {"$cond": [{"$and": [{"$eq": ["$estado_flete", "VALORIZADO"]}, {"$eq": ["$pertenece_a_factura", True]}]}, {"$toDouble": "$monto_flete"}, 0]}
                        }
                    }
                })

                pipeline.append({
                    "$project": {
                        "_id": 0,
                        "periodo": {"$literal": f"{mes}/{anio}" if mes else "HISTORICO TOTAL"},
                        "conteo_total": 1,
                        "detalles": {
                            "pendientes": {
                                "cantidad": "$cant_pendiente",
                                "monto": {"$round": ["$monto_pendiente", 2]}
                            },
                            "valorizados_sin_factura": {
                                "cantidad": "$cant_val_sin_fac",
                                "monto": {"$round": ["$monto_val_sin_fac", 2]}
                            },
                            "valorizados_con_factura": {
                                "cantidad": "$cant_val_con_fac",
                                "monto": {"$round": ["$monto_val_con_fac", 2]}
                            }
                        },
                        "venta_total_valorizada": {"$round": [{"$add": ["$monto_val_sin_fac", "$monto_val_con_fac"]}, 2]}
                    }
                })

                resultado = list(self.fletes_collection.aggregate(pipeline))

                if not resultado:
                    return {
                        "periodo": f"{mes}/{anio}" if mes else "HISTORICO TOTAL",
                        "conteo_total": 0,
                        "detalles": {
                            "pendientes": {"cantidad": 0, "monto": 0.0},
                            "valorizados_sin_factura": {"cantidad": 0, "monto": 0.0},
                            "valorizados_con_factura": {"cantidad": 0, "monto": 0.0}
                        },
                        "venta_total_valorizada": 0.0
                    }

                return resultado[0]

            except Exception as e:
                return {"error": str(e)}

    def analisis_de_facturas(self, mes: Optional[int] = None, anio: Optional[int] = None) -> Dict[str, Any]:
        try:
            pipeline = []
            
            # 1. Filtro base: Omitir Anulados
            pipeline.append({"$match": {"estado_pago_neto": {"$ne": "Anulado"}}})

            # 2. Preparar campos numéricos y fecha de referencia
            pipeline.append({
                "$addFields": {
                    "fecha_referencia": { "$arrayElemAt": ["$datos_completos.fletes.servicio.fecha_servicio", 0] },
                    "m_neto": {"$toDouble": "$monto_neto"},
                    "m_pagado": {"$toDouble": "$monto_pagado_acumulado"},
                    "m_pendiente": {"$subtract": [{"$toDouble": "$monto_neto"}, {"$toDouble": "$monto_pagado_acumulado"}]}
                }
            })

            # 3. Filtro por periodo (opcional)
            if mes is not None and anio is not None:
                mes, anio = int(mes), int(anio)
                fecha_inicio = datetime(anio, mes, 1)
                fecha_fin = datetime(anio + 1, 1, 1) if mes == 12 else datetime(anio, mes + 1, 1)
                pipeline.append({"$match": {"fecha_referencia": {"$gte": fecha_inicio, "$lt": fecha_fin}}})

            # 4. Agrupación con lógica de ESTADO para vencidos
            pipeline.append({
                "$group": {
                    "_id": None,
                    "conteo_facturas": {"$sum": 1},
                    "total_bruto": {"$sum": {"$toDouble": "$datos_completos.monto_total"}},
                    "total_neto": {"$sum": "$m_neto"},
                    "total_pagado": {"$sum": "$m_pagado"},
                    
                    # --- PAGADAS ---
                    # Se consideran pagadas si el estado es "Pagado" o el saldo es 0
                    "cant_pagadas": {
                        "$sum": {"$cond": [{"$eq": ["$estado_pago_neto", "Pagado"]}, 1, 0]}
                    },
                    
                    # --- VENCIDAS (Filtrado por estado explícito) ---
                    "cant_vencidas": {
                        "$sum": {"$cond": [{"$eq": ["$estado_pago_neto", "Vencido"]}, 1, 0]}
                    },
                    "monto_vencido": {
                        "$sum": {"$cond": [{"$eq": ["$estado_pago_neto", "Vencido"]}, "$m_pendiente", 0]}
                    },
                    
                    # --- POR VENCER (Pendientes que no están vencidas ni pagadas) ---
                    "cant_por_vencer": {
                        "$sum": {"$cond": [{"$eq": ["$estado_pago_neto", "Pendiente"]}, 1, 0]}
                    },
                    "monto_por_vencer": {
                        "$sum": {"$cond": [{"$eq": ["$estado_pago_neto", "Pendiente"]}, "$m_pendiente", 0]}
                    }
                }
            })

            # 5. Proyección Final
            pipeline.append({
                "$project": {
                    "_id": 0,
                    "conteo_facturas": 1,
                    "monto_total_bruto": {"$round": ["$total_bruto", 2]},
                    "monto_neto_total": {"$round": ["$total_neto", 2]},
                    "pagado": {
                        "cantidad": "$cant_pagadas",
                        "monto": {"$round": ["$total_pagado", 2]}
                    },
                    "vencido": {
                        "cantidad": "$cant_vencidas",
                        "monto": {"$round": ["$monto_vencido", 2]}
                    },
                    "por_vencer": {
                        "cantidad": "$cant_por_vencer",
                        "monto": {"$round": ["$monto_por_vencer", 2]}
                    },
                    "monto_total_pendiente": {"$round": [{"$subtract": ["$total_neto", "$total_pagado"]}, 2]}
                }
            })

            resultado = list(self.collection.aggregate(pipeline))

            if not resultado:
                return {
                    "periodo": f"{mes}/{anio}" if mes else "HISTORICO TOTAL",
                    "conteo_facturas": 0,
                    "monto_total_bruto": 0.0,
                    "monto_neto_total": 0.0,
                    "pagado": {"cantidad": 0, "monto": 0.0},
                    "vencido": {"cantidad": 0, "monto": 0.0},
                    "por_vencer": {"cantidad": 0, "monto": 0.0},
                    "monto_total_pendiente": 0.0
                }

            res = resultado[0]
            res["periodo"] = f"{mes}/{anio}" if mes else "HISTORICO TOTAL"
            return res

        except Exception as e:
            print(f"Error: {e}")
            return {"error": str(e)}

    def obtener_resumen_financiero(self, mes: Optional[int] = None, anio: Optional[int] = None) -> Dict[str, Any]:
        """
        Consolida el análisis detallado de fletes y el estado de cobranza 
        de facturas (Pagadas, Vencidas y Por Vencer).
        """
        try:
            # 1. Ejecutar ambos análisis actualizados
            fletes = self.analisis_de_fletes(mes=mes, anio=anio)
            facturas = self.analisis_de_facturas(mes=mes, anio=anio)

            # 2. Verificar si hubo errores en las funciones individuales
            if "error" in fletes:
                return {"error": f"Error en análisis de fletes: {fletes['error']}"}
            if "error" in facturas:
                return {"error": f"Error en análisis de facturas: {facturas['error']}"}

            return {
                "fletes":fletes,
                "facturas":facturas
            }

        except Exception as e:
            print(f"Error crítico en consolidación: {e}")
            return {"error": str(e)}

    def obtener_fletes_por_fecha_servicio(
            self, 
            fecha_inicio: Optional[datetime] = None, 
            fecha_fin: Optional[datetime] = None,
            pagina: int = 1,
            limite: int = 20
        ) -> Dict[str, Any]:
            try:
                
                db = get_database()
                fletesService = FleteService(db)

                resultado_fletes = fletesService.get_fletes_advanced(
                    fecha_servicio_desde=fecha_inicio,
                    fecha_servicio_hasta=fecha_fin,
                    page=pagina,
                    page_size=limite
                )

                resultados_combinados = []
                
                for item in resultado_fletes["items"]:
                    servicio_id = item.get("servicio_id")
                    detalle_servicio = {}
                    
                    if servicio_id:
                        try:
                            search_id = ObjectId(servicio_id) if isinstance(servicio_id, str) else servicio_id
                            detalle_servicio = self.servicios_collection.find_one({"_id": search_id}) or {}
                        except:
                            detalle_servicio = {}

                    def serializar(obj):
                        if isinstance(obj, ObjectId): return str(obj)
                        if isinstance(obj, datetime): return obj.isoformat()
                        if isinstance(obj, dict): return {k: serializar(v) for k, v in obj.items()}
                        if isinstance(obj, list): return [serializar(i) for i in obj]
                        return obj

                    resultados_combinados.append({
                        "flete": serializar(item),
                        "servicio": serializar(detalle_servicio)
                    })

                return {
                    "paginacion": {
                        "total_registros": resultado_fletes["total"],
                        "total_paginas": resultado_fletes["total_pages"],
                        "pagina_actual": pagina,
                        "limite_por_pagina": limite
                    },
                    "resultados": resultados_combinados
                }

            except Exception as e:
                logger.error(f"Error en obtener_fletes_por_fecha_servicio: {str(e)}")
                return {"error": str(e)}

  





































    def get_resumen_financiero_cliente(
        self,
        nombre_cliente: Optional[str] = None,
        fecha_inicio: Optional[datetime] = None,
        fecha_fin: Optional[datetime] = None,
        mes: Optional[int] = None,  # Nuevo parámetro
        anio: Optional[int] = None   # Nuevo parámetro
    ) -> Dict[str, Any]:

        try:
            pipeline = []
            ahora = datetime.now()

            # --- Lógica de fechas ---
            if mes and anio:
                fecha_inicio = datetime(anio, mes, 1)
                if mes == 12:
                    siguiente_mes = datetime(anio + 1, 1, 1)
                else:
                    siguiente_mes = datetime(anio, mes + 1, 1)
                fecha_fin = siguiente_mes - timedelta(seconds=1)

            # 1. Filtros dinámicos
            match_filters = {}

            if nombre_cliente:
                match_filters["datos_completos.fletes.servicio.nombre_cliente"] = {
                    "$regex": f"^{nombre_cliente}$",
                    "$options": "i"
                }

            if fecha_inicio or fecha_fin:
                date_filter = {}
                if fecha_inicio: date_filter["$gte"] = fecha_inicio
                if fecha_fin: date_filter["$lte"] = fecha_fin
                
                # Filtramos documentos donde AL MENOS UN servicio cumple la fecha
                match_filters["datos_completos.fletes.0.servicio.fecha_servicio"] = date_filter

            if match_filters:
                pipeline.append({"$match": match_filters})

            # --- PASO CLAVE: Evitar duplicidad si usas unwind o filtros de array ---
            # Si un documento tiene varios fletes del mismo mes, el match lo trae una vez.
            # Agrupamos por el ID de la factura primero para asegurar montos únicos.

            # 2. Agrupación por Cliente
            pipeline.append({
                "$group": {
                    "_id": { 
                        # Usamos el primer nombre de cliente que encuentre en el array
                        "$arrayElemAt": ["$datos_completos.fletes.servicio.nombre_cliente", 0] 
                    },
                    "total_facturas": {"$sum": 1},
                    "total_facturado": {"$sum": "$datos_completos.monto_total"},
                    "total_detraccion": {"$sum": "$monto_detraccion"},
                    "total_neto": {"$sum": "$monto_neto"},
                    "total_neto_pagado": {
                        "$sum": {
                            "$cond": [{"$eq": ["$estado_pago_neto", "Pagado"]}, "$monto_neto", 0]
                        }
                    },
                    "total_neto_pendiente": {
                        "$sum": {
                            "$cond": [{"$ne": ["$estado_pago_neto", "Pagado"]}, "$monto_neto", 0]
                        }
                    },
                    "total_neto_vencido": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$lt": ["$datos_completos.fecha_vencimiento", ahora]},
                                        {"$ne": ["$estado_pago_neto", "Pagado"]}
                                    ]
                                },
                                "$monto_neto",
                                0
                            ]
                        }
                    },
                    "total_neto_por_vencer": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$gte": ["$datos_completos.fecha_vencimiento", ahora]},
                                        {"$ne": ["$estado_pago_neto", "Pagado"]}
                                    ]
                                },
                                "$monto_neto",
                                0
                            ]
                        }
                    }
                }
            })

            # 3. Orden y ejecución (resto del código igual...)
            pipeline.append({"$sort": {"total_neto_vencido": -1, "total_neto_pendiente": -1}})

            resultados = list(
                self.db["facturacion_gestion"].aggregate(pipeline)
            )

            # 4. Totales globales
            resumen_global = {
                "clientes_activos": len(resultados),
                "gran_total_facturado": round(sum(r["total_facturado"] for r in resultados), 2),
                "gran_total_detraccion": round(sum(r["total_detraccion"] for r in resultados), 2),
                "gran_total_neto": round(sum(r["total_neto"] for r in resultados), 2),
                "gran_total_neto_pagado": round(sum(r["total_neto_pagado"] for r in resultados), 2),
                "gran_total_neto_pendiente": round(sum(r["total_neto_pendiente"] for r in resultados), 2),
                "gran_total_neto_vencido": round(sum(r["total_neto_vencido"] for r in resultados), 2),
                "gran_total_neto_por_vencer": round(sum(r["total_neto_por_vencer"] for r in resultados), 2)
            }

            return {
                "resumen_general": resumen_global,
                "detalle_por_cliente": [
                    {
                        "cliente": r["_id"],
                        "nro_facturas": r["total_facturas"],

                        "facturado": round(float(r["total_facturado"]), 2),
                        "detraccion": round(float(r["total_detraccion"]), 2),
                        "neto_total": round(float(r["total_neto"]), 2),

                        "neto_pagado": round(float(r["total_neto_pagado"]), 2),
                        "neto_pendiente": round(float(r["total_neto_pendiente"]), 2),
                        "neto_vencido": round(float(r["total_neto_vencido"]), 2),
                        "neto_por_vencer": round(float(r["total_neto_por_vencer"]), 2),

                        "porcentaje_morosidad": round(
                            (r["total_neto_vencido"] / r["total_neto"] * 100), 2
                        ) if r["total_neto"] > 0 else 0
                    }
                    for r in resultados
                ]
            }

        except Exception as e:
            print(f"Error en agregación financiera: {e}")
            raise

    def export_resumen_financiero_to_excel(
        self,
        nombre_cliente: Optional[str] = None,
        fecha_inicio: Optional[datetime] = None,
        fecha_fin: Optional[datetime] = None,
        mes: Optional[int] = None,
        anio: Optional[int] = None
    ) -> BytesIO:
        try:
            # 1. Obtener los datos de la función original
            resumen = self.get_resumen_financiero_cliente(
                nombre_cliente, fecha_inicio, fecha_fin, mes, anio
            )
            
            detalle_clientes = resumen["detalle_por_cliente"]
            resumen_global = resumen["resumen_general"]

            if not detalle_clientes:
                # DataFrame vacío con cabeceras si no hay datos
                df = pd.DataFrame(columns=[
                    "Cliente", "N° Facturas", "Total Facturado", "Detracción", 
                    "Neto Total", "Neto Pagado", "Neto Pendiente", 
                    "Neto Vencido", "Neto por Vencer", "% Morosidad"
                ])
            else:
                excel_data = []
                for r in detalle_clientes:
                    excel_data.append({
                        "Cliente": r["cliente"],
                        "N° Facturas": r["nro_facturas"],
                        "Total Facturado": r["facturado"],
                        "Detracción": r["detraccion"],
                        "Facturado con Detraccion": r["neto_total"],
                        "Cobrado": r["neto_pagado"],
                        "Pendiente": r["neto_pendiente"],
                        "Vencido": r["neto_vencido"],
                        "Por Vencer": r["neto_por_vencer"],
                        # "% Morosidad": f"{r['porcentaje_morosidad']}%"
                    })
                
                # 2. Agregar fila de TOTALES al final para mayor claridad
                excel_data.append({
                    "Cliente": "TOTAL GENERAL",
                    "N° Facturas": sum(r["nro_facturas"] for r in detalle_clientes),
                    "Total Facturado": resumen_global["gran_total_facturado"],
                    "Detracción": resumen_global["gran_total_detraccion"],
                    "Facturado con Detraccion": resumen_global["gran_total_neto"],
                    "Cobrado": resumen_global["gran_total_neto_pagado"],
                    "Pendiente": resumen_global["gran_total_neto_pendiente"],
                    "Vencido": resumen_global["gran_total_neto_vencido"],
                    "Por Vencer": resumen_global["gran_total_neto_por_vencer"],
                    # "% Morosidad": "" # No se suma el porcentaje directamente
                })

                df = pd.DataFrame(excel_data)

            # 3. Proceso de escritura similar a tu ejemplo
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                sheet_name = 'Resumen Financiero'
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                
                worksheet = writer.sheets[sheet_name]
                
                # Ajuste automático de columnas
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    ) + 2
                    worksheet.column_dimensions[get_column_letter(idx + 1)].width = min(max_length, 50)

            output.seek(0)
            return output

        except Exception as e:
            # Asumiendo que tienes un logger configurado, si no, usa print
            print(f"Error al exportar resumen financiero: {str(e)}")
            raise

    