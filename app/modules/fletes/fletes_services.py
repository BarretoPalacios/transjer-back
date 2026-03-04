from io import BytesIO
import pandas as pd
from typing import List, Optional, Dict, Any
from bson import ObjectId
from app.modules.utils.core.code_generator.code_generator import generate_sequential_code
from app.modules.fletes.fletes_model import Flete
from app.modules.fletes.fletes_schemas import FleteFilter
from datetime import datetime
import logging
from math import ceil
import re

logger = logging.getLogger(__name__)

class FleteService:
    def __init__(self, db):
        self.db = db 
        self.collection = db["fletes"]
        self.servicios_collection = db["servicio_principal"]  # Colección de servicios
        self.facturas_collection = db["facturacion"]
    
    def create_flete(self, flete_data: dict) -> dict:
        """Crear un nuevo flete con valores por defecto de facturación"""
        try:
            # Generar código automáticamente
            codigo_flete = generate_sequential_code(
                counters_collection=self.db["counters"],
                target_collection=self.collection,
                sequence_name="fletes",
                field_name="codigo_flete",
                prefix="FLT-",
                length=10
            )

            flete_data["codigo_flete"] = codigo_flete
            flete_data["fecha_creacion"] = datetime.now()
            
            # Asegurar valores iniciales de facturación
            flete_data.setdefault("pertenece_a_factura", False)
            flete_data.setdefault("factura_id", None)
            flete_data.setdefault("codigo_factura", None)

            # Crear modelo
            flete_model = Flete(**flete_data)

            # Insertar
            result = self.collection.insert_one(
                flete_model.model_dump(by_alias=True)
            )

            return self.get_flete_by_id(str(result.inserted_id))

        except Exception as e:
            logger.error(f"Error al crear flete: {str(e)}")
            raise
    
    def delete_flete(self, flete_id: str) -> bool:
            """
            Elimina un flete y actualiza el estado del servicio asociado.
            """
            try:
                # 1. Buscar el flete antes de eliminarlo para obtener la referencia al servicio
                flete = self.collection.find_one({"_id": ObjectId(flete_id)})
                
                if not flete:
                    logger.warning(f"Intento de eliminar flete inexistente: {flete_id}")
                    return False

                servicio_id = flete.get("servicio_id") # Asumiendo que guardas esta relación

                # 2. Eliminar el flete
                result = self.collection.delete_one({"_id": ObjectId(flete_id)})

                if result.deleted_count > 0:
                    # 3. Si hay un servicio asociado, actualizar su estado
                    if servicio_id:
                        self.db["servicio_principal"].update_one(
                            {"_id": ObjectId(servicio_id)},
                            {"$set": {
                                "estado": "Cancelado",
                                  "flete_asignado": False,
                                  "es_editable": False,
                                  "es_eliminable": False}}
                        )
                    
                    logger.info(f"Flete {flete_id} eliminado exitosamente y servicio {servicio_id} actualizado.")
                    return True
                
                return False

            except Exception as e:
                logger.error(f"Error al eliminar flete {flete_id}: {str(e)}")
                raise
    
    def get_flete_by_id(self, flete_id: str) -> Optional[dict]:
        """Obtener flete por ID"""
        try:
            if not ObjectId.is_valid(flete_id):
                return None
            
            flete = self.collection.find_one({"_id": ObjectId(flete_id)})
            if flete:
                flete["id"] = str(flete["_id"])
                del flete["_id"]
            return flete
            
        except Exception as e:
            logger.error(f"Error al obtener flete: {str(e)}")
            return None

    def get_all_fletes(
        self,
        filter_params: Optional[FleteFilter] = None,
        page: int = 1,
        page_size: int = 10
    ) -> dict:
        """Obtener fletes con filtros de facturación y paginación"""
        try:
            query = {}

            if filter_params:
                if filter_params.codigo_flete:
                    query["codigo_flete"] = safe_regex(filter_params.codigo_flete)

                if filter_params.servicio_id:
                    query["servicio_id"] = filter_params.servicio_id
                
                if filter_params.codigo_servicio:
                    query["codigo_servicio"] = safe_regex(filter_params.codigo_servicio)

                if filter_params.estado_flete:
                    query["estado_flete"] = filter_params.estado_flete

                # Filtros de Facturación
                if filter_params.pertenece_a_factura is not None:
                    query["pertenece_a_factura"] = filter_params.pertenece_a_factura
                
                if filter_params.codigo_factura:
                    query["codigo_factura"] = safe_regex(filter_params.codigo_factura)

                # Filtros de Monto
                if filter_params.monto_flete_min is not None:
                    query.setdefault("monto_flete", {})["$gte"] = filter_params.monto_flete_min
                if filter_params.monto_flete_max is not None:
                    query.setdefault("monto_flete", {})["$lte"] = filter_params.monto_flete_max

                # Filtros de Fecha
                if filter_params.fecha_creacion_desde:
                    query.setdefault("fecha_creacion", {})["$gte"] = filter_params.fecha_creacion_desde
                if filter_params.fecha_creacion_hasta:
                    query.setdefault("fecha_creacion", {})["$lte"] = filter_params.fecha_creacion_hasta

                
            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size

            fletes = list(
                self.collection.find(query)
                .sort("fecha_creacion", -1)
                .skip(skip)
                .limit(page_size)
            )

            for flete in fletes:
                flete["id"] = str(flete["_id"])
                del flete["_id"]

            total_pages = ceil(total / page_size) if page_size > 0 else 0

            return {
                "items": fletes,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }

        except Exception as e:
            logger.error(f"Error al obtener fletes: {str(e)}")
            raise

    def get_fletes_advanced(
        self,
        # Filtros de Flete
        codigo_flete: Optional[str] = None,
        estado_flete: Optional[str] = None,
        pertenece_a_factura: Optional[bool] = None,
        codigo_factura: Optional[str] = None,
        monto_min: Optional[float] = None,
        monto_max: Optional[float] = None,
        
        # Filtros del Servicio asociado
        cliente: Optional[str] = None,  # Nombre del cliente
        placa: Optional[str] = None,  # Placa del vehículo
        conductor: Optional[str] = None,  # Nombre del conductor
        tipo_servicio: Optional[str] = None,  # REGULAR, etc.
        zona: Optional[str] = None,  # LIMA, etc.
        estado_servicio: Optional[str] = None,  # Completado, Programado, etc.
        
        # Filtros de Fecha del Servicio
        fecha_servicio_desde: Optional[datetime] = None,
        fecha_servicio_hasta: Optional[datetime] = None,
        
        # Filtros de Fecha del Flete
        fecha_creacion_desde: Optional[datetime] = None,
        fecha_creacion_hasta: Optional[datetime] = None,
        
        # Paginación
        page: int = 1,
        page_size: int = 10
    ) -> dict:
        """
        Obtener fletes con información del servicio asociado y filtros avanzados.
        Utiliza agregación de MongoDB para hacer join con servicios.
        """
        try:
            # Pipeline de agregación
            pipeline = []
            
            # 1. Filtros iniciales sobre fletes
            match_flete = {}
            
            if codigo_flete:
                match_flete["codigo_flete"] = safe_regex(codigo_flete)
            
            if estado_flete:
                match_flete["estado_flete"] = estado_flete
            
            if pertenece_a_factura is not None:
                match_flete["pertenece_a_factura"] = pertenece_a_factura
            
            if codigo_factura:
                match_flete["codigo_factura"] = safe_regex(codigo_factura)
            
            if monto_min is not None:
                match_flete.setdefault("monto_flete", {})["$gte"] = monto_min
            
            if monto_max is not None:
                match_flete.setdefault("monto_flete", {})["$lte"] = monto_max
            
            if fecha_creacion_desde:
                match_flete.setdefault("fecha_creacion", {})["$gte"] = fecha_creacion_desde
            
            if fecha_creacion_hasta:
                match_flete.setdefault("fecha_creacion", {})["$lte"] = fecha_creacion_hasta
            
            if match_flete:
                pipeline.append({"$match": match_flete})
            
            # 2. Lookup para unir con servicios
            pipeline.append({
                "$lookup": {
                    "from": "servicio_principal",
                    "let": {"servicio_id_str": "$servicio_id"},
                    "pipeline": [
                        {
                            "$addFields": {
                                "servicio_id_str": {"$toString": "$_id"}
                            }
                        },
                        {
                            "$match": {
                                "$expr": {"$eq": ["$servicio_id_str", "$$servicio_id_str"]}
                            }
                        }
                    ],
                    "as": "servicio"
                }
            })
            
            # 3. Desempaquetar el servicio (convertir array a objeto)
            pipeline.append({
                "$unwind": {
                    "path": "$servicio",
                    "preserveNullAndEmptyArrays": True
                }
            })
            
            # 4. Filtros sobre el servicio asociado
            match_servicio = {}
            
            if cliente:
                match_servicio["$or"] = [
                    {"servicio.cliente.nombre": safe_regex(cliente)},
                    {"servicio.cliente.razon_social": safe_regex(cliente)},
                    {"servicio.cuenta.nombre": safe_regex(cliente)}
                ]
            
            if placa:
                match_servicio["servicio.flota.placa"] = safe_regex(placa)
            
            if conductor:
                match_servicio["$or"] = [
                    {"servicio.conductor.nombres_completos": safe_regex(conductor)},
                    {"servicio.conductor.nombre": safe_regex(conductor)},
                    {"servicio.cuenta.nombre_conductor": safe_regex(conductor)}
                ]
            
            if tipo_servicio:
                match_servicio["servicio.tipo_servicio"] = tipo_servicio
            
            if zona:
                match_servicio["servicio.zona"] = safe_regex(zona)
            
            if estado_servicio:
                match_servicio["servicio.estado"] = estado_servicio
            
            if fecha_servicio_desde:
                # Forzar inicio del día
                desde = fecha_servicio_desde.replace(hour=0, minute=0, second=0, microsecond=0)
                match_servicio.setdefault("servicio.fecha_servicio", {})["$gte"] = desde

            if fecha_servicio_hasta:
                # Forzar fin del día
                hasta = fecha_servicio_hasta.replace(hour=23, minute=59, second=59, microsecond=999999)
                match_servicio.setdefault("servicio.fecha_servicio", {})["$lte"] = hasta
            
            if match_servicio:
                pipeline.append({"$match": match_servicio})
            
            # 5. Contar total de documentos
            count_pipeline = pipeline.copy()
            count_pipeline.append({"$count": "total"})
            
            count_result = list(self.collection.aggregate(count_pipeline))
            total = count_result[0]["total"] if count_result else 0
            
            # 6. Ordenar, paginar y proyectar
            pipeline.append({"$sort": {"_id": -1}})
            
            skip = (page - 1) * page_size
            pipeline.append({"$skip": skip})
            pipeline.append({"$limit": page_size})
            
            # 7. Proyección final para limpiar el resultado
            pipeline.append({
                "$project": {
                    "id": {"$toString": "$_id"},
                    "codigo_flete": 1,
                    "servicio_id": 1,
                    "codigo_servicio": 1,
                    "estado_flete": 1,
                    "monto_flete": 1,
                    "pertenece_a_factura": 1,
                    "factura_id": 1,
                    "codigo_factura": 1,
                    "fecha_pago": 1,
                    "observaciones": 1,
                    "fecha_creacion": 1,
                    "fecha_actualizacion": 1,
                    "usuario_creador": 1,
                    "servicio": {
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
                    "_id": 0
                }
            })
            
            # Ejecutar pipeline
            fletes = list(self.collection.aggregate(pipeline))
            
            total_pages = ceil(total / page_size) if page_size > 0 else 0
            
            return {
                "items": fletes,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
            
        except Exception as e:
            logger.error(f"Error al obtener fletes avanzados: {str(e)}")
            raise
    
    def update_flete(self, flete_id: str, update_data: dict) -> Optional[dict]:
        try:
            if not ObjectId.is_valid(flete_id):
                return None
            
            update_dict = {k: v for k, v in update_data.items() if v is not None}
            if not update_dict:
                return self.get_flete_by_id(flete_id)
            
            monto = update_dict.get("monto_flete")
            if monto is not None and monto > 0:
                update_dict["estado_flete"] = "VALORIZADO"

            update_dict["fecha_actualizacion"] = datetime.now()
            
            self.collection.update_one(
                {"_id": ObjectId(flete_id)},
                {"$set": update_dict}
            )
            
            return self.get_flete_by_id(flete_id)
            
        except Exception as e:
            logger.error(f"Error al actualizar flete: {str(e)}")
            raise

    def get_stats(self) -> Dict[str, Any]:
        """Estadísticas incluyendo resumen de facturación"""
        try:
            pipeline_stats = [
                {
                    "$group": {
                        "_id": "$estado_flete",
                        "total_monto": {"$sum": "$monto_flete"},
                        "cantidad": {"$sum": 1}
                    }
                }
            ]
            
            # Estadísticas de facturación
            facturados = self.collection.count_documents({"pertenece_a_factura": True})
            no_facturados = self.collection.count_documents({"pertenece_a_factura": False})
            
            montos_por_estado = {res["_id"]: {"total": res["total_monto"], "cantidad": res["cantidad"]} 
                                 for res in self.collection.aggregate(pipeline_stats)}
            
            return {
                "total_general": self.collection.count_documents({}),
                "por_estado": montos_por_estado,
                "facturacion": {
                    "facturados_count": facturados,
                    "pendientes_facturar_count": no_facturados
                }
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {str(e)}")
            return {}


    def get_reporte_pendientes_por_cliente(
        self, 
        nombre_cliente: Optional[str] = None,
        fecha_servicio_desde: Optional[datetime] = None,
        fecha_servicio_hasta: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Reporte consolidado filtrando por la fecha real del servicio y nombre de cliente.
        """
        try:
            # 1. Filtro base: Solo fletes valorizados que no se han facturado aún
            match_flete = {
                "estado_flete": "VALORIZADO",
                "pertenece_a_factura": False
            }

            # 2. Construcción dinámica del filtro de fecha para el servicio
            # Buscamos en el campo 'fecha_servicio' dentro de la otra colección
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
                                        # Unión por ID
                                        {"$expr": {"$eq": ["$_id", {"$toObjectId": "$$serv_id"}]}},
                                        # Filtro de fecha de servicio
                                        filtro_fecha if filtro_fecha else {}
                                    ]
                                }
                            }
                        ],
                        "as": "info_servicio"
                    }
                },
                # 3. Importante: Unwind elimina los fletes cuyo servicio no cumplió la fecha
                {"$unwind": "$info_servicio"},
                
                # 4. Filtro por nombre de cliente (Opcional)
                {
                    "$match": {
                        "info_servicio.cliente.nombre": {
                            "$regex": nombre_cliente, "$options": "i"
                        } if nombre_cliente else {"$exists": True}
                    }
                },
                {
                    "$group": {
                        "_id": "$info_servicio.cliente.nombre", 
                        "nombre_cliente": {"$first": "$info_servicio.cliente.nombre"},
                        "monto_total_pendiente": {"$sum": "$monto_flete"},
                        "cantidad_fletes": {"$sum": 1}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "nombre_cliente": 1,
                        "monto_total_pendiente": {"$round": ["$monto_total_pendiente", 2]},
                        "cantidad_fletes": 1
                    }
                },
                {"$sort": {"monto_total_pendiente": -1}}
            ]

            return list(self.collection.aggregate(pipeline))

        except Exception as e:
            logger.error(f"Error en reporte por fecha de servicio: {str(e)}")
            return []

    def export_fletes_to_excel(self, filter_params = None) -> BytesIO:
            try:
                query = {}
                if filter_params:
                    if hasattr(filter_params, "dict"):
                        query = filter_params.dict(exclude_none=True)
                    elif isinstance(filter_params, dict):
                        query = filter_params.copy()
                
                cliente_a_filtrar = query.pop("cliente_nombre", None)
                fecha_desde = query.pop("fecha_servicio_desde", None)
                fecha_hasta = query.pop("fecha_servicio_hasta", None)
                
                query = {k: v for k, v in query.items() if v is not None}

                fletes = list(self.collection.find(query))
                
                excel_data = []
                
                for flete in fletes:
                    # --- BUSQUEDA DE SERVICIO ---
                    servicio_id = flete.get("servicio_id")
                    srv = {}
                    if servicio_id:
                        try:
                            search_id = ObjectId(servicio_id) if isinstance(servicio_id, str) else servicio_id
                            srv = self.servicios_collection.find_one({"_id": search_id}) or {}
                        except:
                            srv = {}

                    # --- NUEVA LÓGICA: BUSQUEDA DE FACTURA ---
                    factura_id = flete.get("factura_id")
                    num_factura_real = "PENDIENTE" # Valor por defecto
                    
                    if flete.get("pertenece_a_factura") and factura_id:
                        try:
                            f_id = ObjectId(factura_id) if isinstance(factura_id, str) else factura_id
                            # Buscamos en la colección de facturas
                            factura_doc = self.facturas_collection.find_one({"_id": f_id})
                            if factura_doc:
                                # Priorizamos 'numero_factura', si no existe usamos 'codigo_factura'
                                num_factura_real = factura_doc.get("numero_factura", factura_doc.get("codigo_factura", ""))
                        except Exception as e:
                            logger.warning(f"Error buscando factura {factura_id}: {str(e)}")

                    # --- LÓGICA DE FILTRADO MANUAL (SERVICIO) ---
                    nombre_cliente_db = srv.get("cliente", {}).get("nombre", "")
                    if cliente_a_filtrar and cliente_a_filtrar.lower() not in nombre_cliente_db.lower():
                        continue

                    f_srv = srv.get("fecha_servicio")
                    if isinstance(f_srv, dict) and "$date" in f_srv:
                        f_srv = f_srv["$date"]
                    
                    if f_srv:
                        if isinstance(f_srv, str):
                            from dateutil import parser
                            f_srv = parser.parse(f_srv)
                        
                        if fecha_desde and f_srv < fecha_desde:
                            continue
                        if fecha_hasta and f_srv > fecha_hasta:
                            continue
                    elif fecha_desde or fecha_hasta:
                        continue

                    # --- PROCESAMIENTO DE DATOS ---
                    conductor_info = srv.get("conductor", [{}])[0] if srv.get("conductor") else {}
                    auxiliar_info = srv.get("auxiliar", [{}])[0] if srv.get("auxiliar") else {}
                    
                    def format_mongo_date(date_field):
                        if isinstance(date_field, dict) and "$date" in date_field:
                            return date_field["$date"]
                        return date_field

                    excel_data.append({
                        "Código Flete": flete.get("codigo_flete", ""),
                        "Monto Flete": float(flete.get("monto_flete", 0)),
                        "Estado Flete": flete.get("estado_flete", ""),
                        # CAMBIO AQUÍ: Usamos la variable num_factura_real que obtuvimos arriba
                        "Factura": num_factura_real, 
                        "Fecha Creación": format_mongo_date(flete.get("fecha_creacion")),
                        "Código Servicio": srv.get("codigo_servicio_principal", flete.get("codigo_servicio", "")),
                        "Fecha Servicio": format_mongo_date(srv.get("fecha_servicio")),
                        "Fecha Salida": format_mongo_date(srv.get("fecha_salida")),
                        "Cliente": nombre_cliente_db,
                        "RUC Cliente": srv.get("cliente", {}).get("ruc", ""),
                        "Proveedor": srv.get("proveedor", {}).get("nombre", ""),
                        "Placa": srv.get("flota", {}).get("placa", ""),
                        "Conductor": conductor_info.get("nombre", ""),
                        "Auxiliar": auxiliar_info.get("nombre", ""),
                        "Origen": srv.get("origen", ""),
                        "Destino": srv.get("destino", ""),
                        "Zona": srv.get("zona", ""),
                        "Tipo": srv.get("tipo_servicio", ""),
                        "Modalidad Servicio": srv.get("modalidad_servicio", ""),
                        "M3": srv.get("m3", ""),
                        "TN": srv.get("tn", ""),
                        "Guía RR": srv.get("gia_rr", ""),
                        "Guía RT": srv.get("gia_rt", ""),
                        "Estado Servicio": srv.get("estado", ""),
                        "Observaciones": flete.get("observaciones", ""),
                        "Descripcion":srv.get("descripcion", "")
                    })
                
                # ... (el resto del código de pandas y openpyxl se mantiene igual)
                if not excel_data:
                    df = pd.DataFrame(columns=["Código Flete", "Monto Flete", "Cliente", "Estado Flete"])
                else:
                    df = pd.DataFrame(excel_data)

                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    from openpyxl.styles import Font, PatternFill, Alignment

                    df.to_excel(writer, index=False, sheet_name='Reporte Fletes')
                    # ... (lógica de estilos)
                    worksheet = writer.sheets['Reporte Fletes']
                    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
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
                            except: pass
                        worksheet.column_dimensions[column_letter].width = min(max_length + 3, 50)

                output.seek(0)
                return output
                
            except Exception as e:
                logger.error(f"Error en export_fletes_to_excel: {str(e)}")
                raise


def safe_regex(value: str):
    if not value or not value.strip():
        return None
    return {"$regex": re.escape(value.strip()), "$options": "i"}