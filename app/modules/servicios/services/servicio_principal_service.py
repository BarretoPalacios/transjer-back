from typing import List, Optional, Dict, Any
from bson import ObjectId
from app.modules.utils.core.code_generator.code_generator import generate_sequential_code
from app.modules.fletes.fletes_services import FleteService
from app.core.database import get_database
from app.modules.servicios.models.servicio_principal import ServicioPrincipal, EstadoServicio
from app.modules.servicios.schemas.servicio_principal_schema import (
    ServicioPrincipalCreate, 
    ServicioPrincipalUpdate, 
    ServicioPrincipalFilter,
    CambioEstadoRequest,
    CierreServicioRequest
)
from datetime import datetime, date, time, timedelta
import pandas as pd
from io import BytesIO
import logging
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)

class ServicioPrincipalService:
    def __init__(self, db):
        self.db = db 
        self.collection = db["servicio_principal"]
    
    def _convert_datetime_to_date(self, data: dict) -> dict:
        converted_data = data.copy()
        
        date_fields = ['fecha_servicio', 'fecha_salida']
        
        for field in date_fields:
            if field in converted_data and converted_data[field] is not None:
                if isinstance(converted_data[field], datetime):
                    converted_data[field] = converted_data[field].date()
        
        if 'hora_cita' in converted_data and converted_data['hora_cita'] is not None:
            if isinstance(converted_data['hora_cita'], datetime):
                converted_data['hora_cita'] = converted_data['hora_cita'].time()
        
        return converted_data
    
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
    
    def create_servicio(self, servicio_data: dict) -> dict:
        try:
            if "codigo_servicio_principal" in servicio_data and servicio_data["codigo_servicio_principal"]:
                existing_servicio = self.collection.find_one({
                    "codigo_servicio_principal": servicio_data["codigo_servicio_principal"]
                })
                if existing_servicio:
                    raise ValueError(
                        f"El código de servicio {servicio_data['codigo_servicio_principal']} ya está registrado"
                    )
            
            if "codigo_servicio_principal" not in servicio_data or not servicio_data["codigo_servicio_principal"]:
                codigo_servicio_principal = generate_sequential_code(
                    counters_collection=self.db["counters"],
                    target_collection=self.collection,
                    sequence_name="servicios",
                    field_name="codigo_servicio_principal",
                    prefix="SRV-",
                    length=10
                )
                servicio_data["codigo_servicio_principal"] = codigo_servicio_principal
            
            servicio_data["fecha_registro"] = datetime.now()
            servicio_data["estado"] = servicio_data.get("estado", EstadoServicio.PROGRAMADO)
            servicio_data["historial_estados"] = []
            servicio_data["es_editable"] = True
            servicio_data["es_eliminable"] = False
            servicio_data["servicio_cerrado"] = False
            servicio_data["fecha_cierre"] = None
            servicio_data["fecha_ultima_modificacion"] = None
            servicio_data["fecha_completado"] = None
            servicio_data["pertenece_a_factura"] = False
            
            servicio_model = ServicioPrincipal(**servicio_data)
            
            result = self.collection.insert_one(servicio_model.model_dump(by_alias=True))
            
            created_servicio = self.collection.find_one({"_id": result.inserted_id})
            if created_servicio:
                created_servicio["id"] = str(created_servicio["_id"])
                del created_servicio["_id"]
                created_servicio = self._convert_datetime_to_date(created_servicio)
            
            return created_servicio
            
        except Exception as e:
            logger.error(f"Error al crear servicio: {str(e)}")
            raise
    
    def get_all_servicios(
        self, 
        filter_params: Optional[ServicioPrincipalFilter] = None,
        page: int = 1,
        page_size: int = 100
    ) -> Dict[str, Any]:
        try:
            query = {
                # "estado": {"$in": ["Programado", "Reprogramado"]}
            }
            
            if filter_params:
                if filter_params.codigo_servicio_principal:
                    query["codigo_servicio_principal"] = filter_params.codigo_servicio_principal
                if filter_params.mes:
                    query["mes"] = filter_params.mes
                
                if filter_params.tipo_servicio:
                    query["tipo_servicio"] = filter_params.tipo_servicio
                
                if filter_params.modalidad_servicio:
                    query["modalidad_servicio"] = filter_params.modalidad_servicio
                
                if filter_params.zona:
                    query["zona"] = filter_params.zona
                
                if filter_params.estado:
                    query["estado"] = filter_params.estado
                
                if filter_params.servicio_cerrado is not None:
                    query["servicio_cerrado"] = filter_params.servicio_cerrado
                
                if filter_params.es_editable is not None:
                    query["es_editable"] = filter_params.es_editable
                
                if filter_params.pertenece_a_factura is not None:
                    query["pertenece_a_factura"] = filter_params.pertenece_a_factura
                
                if filter_params.solicitud:
                    query["solicitud"] = filter_params.solicitud
                
                if filter_params.origen:
                    query["origen"] = {"$regex": filter_params.origen, "$options": "i"}
                
                if filter_params.destino:
                    query["destino"] = {"$regex": filter_params.destino, "$options": "i"}
                
                if filter_params.responsable:
                    query["responsable"] = {"$regex": filter_params.responsable, "$options": "i"}
                
                if filter_params.gia_rr:
                    query["gia_rr"] = {"$regex": filter_params.gia_rr, "$options": "i"}
                
                if filter_params.gia_rt:
                    query["gia_rt"] = {"$regex": filter_params.gia_rt, "$options": "i"}
                
                if filter_params.periodo:
                    fecha_inicio, fecha_fin = self._get_period_date_range(filter_params.periodo)
                    if fecha_inicio and fecha_fin:
                        query["fecha_servicio"] = {"$gte": fecha_inicio, "$lte": fecha_fin}
                
                elif filter_params.fecha_servicio:
                    fecha_inicio = datetime.combine(filter_params.fecha_servicio, datetime.min.time())
                    fecha_fin = datetime.combine(filter_params.fecha_servicio, datetime.max.time())
                    query["fecha_servicio"] = {"$gte": fecha_inicio, "$lte": fecha_fin}
                
                elif filter_params.fecha_inicio or filter_params.fecha_fin:
                    fecha_query = {}
                    if filter_params.fecha_inicio:
                        fecha_query["$gte"] = datetime.combine(filter_params.fecha_inicio, datetime.min.time())
                    if filter_params.fecha_fin:
                        fecha_query["$lte"] = datetime.combine(filter_params.fecha_fin, datetime.max.time())
                    if fecha_query:
                        query["fecha_servicio"] = fecha_query
                
                if filter_params.cliente_nombre:
                    cliente_conditions = [
                        {"cliente.razon_social": {"$regex": filter_params.cliente_nombre, "$options": "i"}},
                        {"cliente.nombre": {"$regex": filter_params.cliente_nombre, "$options": "i"}},
                        {"cliente.nombre_comercial": {"$regex": filter_params.cliente_nombre, "$options": "i"}}
                    ]
                    if "$or" not in query:
                        query["$or"] = cliente_conditions
                    else:
                        existing_or = query.pop("$or")
                        query["$and"] = [
                            {"$or": existing_or},
                            {"$or": cliente_conditions}
                        ]
                
                if filter_params.proveedor_nombre:
                    proveedor_conditions = [
                        {"proveedor.razon_social": {"$regex": filter_params.proveedor_nombre, "$options": "i"}},
                        {"proveedor.nombre": {"$regex": filter_params.proveedor_nombre, "$options": "i"}},
                        {"proveedor.nombre_comercial": {"$regex": filter_params.proveedor_nombre, "$options": "i"}}
                    ]
                    if "$or" not in query and "$and" not in query:
                        query["$or"] = proveedor_conditions
                    else:
                        if "$and" not in query:
                            query["$and"] = []
                        query["$and"].append({"$or": proveedor_conditions})
                
                if filter_params.cuenta_nombre:
                    query["cuenta.nombre"] = {"$regex": filter_params.cuenta_nombre, "$options": "i"}
                
                if filter_params.flota_placa:
                    query["flota.placa"] = {"$regex": filter_params.flota_placa, "$options": "i"}
                
                if filter_params.conductor_nombre:
                    conductor_conditions = [
                        {"conductor.nombres": {"$regex": filter_params.conductor_nombre, "$options": "i"}},
                        {"conductor.apellidos": {"$regex": filter_params.conductor_nombre, "$options": "i"}},
                        {"conductor.nombre": {"$regex": filter_params.conductor_nombre, "$options": "i"}},
                        {"$expr": {
                            "$regexMatch": {
                                "input": {"$concat": ["$conductor.nombres", " ", "$conductor.apellidos"]},
                                "regex": filter_params.conductor_nombre,
                                "options": "i"
                            }
                        }}
                    ]
                    if "$or" not in query and "$and" not in query:
                        query["$or"] = conductor_conditions
                    else:
                        if "$and" not in query:
                            query["$and"] = []
                        query["$and"].append({"$or": conductor_conditions})
            
            total_count = self.collection.count_documents(query)
            
            skip = (page - 1) * page_size
            
            servicios_list = list(
                self.collection.find(query)
                .sort("codigo_servicio_principal", 1)
                .skip(skip)
                .limit(page_size)
            )
            
            result = []
            for servicio in servicios_list:
                servicio["id"] = str(servicio["_id"])
                del servicio["_id"]
                servicio = self._convert_datetime_to_date(servicio)
                result.append(servicio)
            
            total_pages = (total_count + page_size - 1) // page_size
            
           

            return {
                "data": result,
                "pagination": {
                    "total": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1
                }
            }
            
        except Exception as e:
            logger.error(f"Error al obtener servicios: {str(e)}")
            return {
                "data": [],
                "pagination": {
                    "total": 0,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": 0,
                    "has_next": False,
                    "has_prev": False
                }
            }
    
    def get_servicio_by_id(self, servicio_id: str) -> Optional[dict]:
        try:
            if not ObjectId.is_valid(servicio_id):
                return None
            
            servicio = self.collection.find_one({"_id": ObjectId(servicio_id)})
            if servicio:
                servicio["id"] = str(servicio["_id"])
                del servicio["_id"]
                servicio = self._convert_datetime_to_date(servicio)
            return servicio
            
        except Exception as e:
            logger.error(f"Error al obtener servicio: {str(e)}")
            return None

    def get_servicio_by_codigo_principal(self, codigo_servicio: str) -> Optional[dict]:
            try:
                servicio = self.collection.find_one({
                    "codigo_servicio_principal": codigo_servicio
                })
                
                if servicio:
                    servicio["id"] = str(servicio["_id"])
                    del servicio["_id"]
                    servicio = self._convert_datetime_to_date(servicio)
                    return servicio
                    
                return None
                
            except Exception as e:
                logger.error(f"Error al obtener servicio: {str(e)}")
                return None
    
    def update_servicio(self, servicio_id: str, update_data: dict) -> Optional[dict]:
        try:
            if not ObjectId.is_valid(servicio_id):
                return None
            
            servicio_actual = self.collection.find_one({"_id": ObjectId(servicio_id)})
            if not servicio_actual:
                return None
            
            if not servicio_actual.get("es_editable", True):
                estado = servicio_actual.get("estado", "")
                servicio_cerrado = servicio_actual.get("servicio_cerrado", False)
                
                if servicio_cerrado:
                    raise ValueError("No se puede editar un servicio cerrado")
                else:
                    raise ValueError(f"No se puede editar un servicio en estado {estado}")
            
            update_dict = {k: v for k, v in update_data.items() if v is not None}
            
            if "estado" in update_dict:
                del update_dict["estado"]
                logger.warning("Se intentó actualizar el estado mediante update_servicio. Use cambiar_estado_servicio()")
            
            campos_protegidos = [
                "historial_estados", "es_editable", "es_eliminable",
                "servicio_cerrado", "fecha_cierre",
                "fecha_completado", "fecha_registro", "pertenece_a_factura"
            ]
            for campo in campos_protegidos:
                if campo in update_dict:
                    del update_dict[campo]
            
            if not update_dict:
                return self.get_servicio_by_id(servicio_id)
            
            date_fields = ['fecha_servicio', 'fecha_salida']
            for field in date_fields:
                if field in update_dict and isinstance(update_dict[field], date):
                    if not isinstance(update_dict[field], datetime):
                        update_dict[field] = datetime.combine(update_dict[field], datetime.min.time())
            
            if 'hora_cita' in update_dict and isinstance(update_dict['hora_cita'], time):
                update_dict['hora_cita'] = datetime.combine(date.today(), update_dict['hora_cita'])
            
            update_dict["fecha_ultima_modificacion"] = datetime.now()
            
            self.collection.update_one(
                {"_id": ObjectId(servicio_id)},
                {"$set": update_dict}
            )
            
            return self.get_servicio_by_id(servicio_id)
            
        except Exception as e:
            logger.error(f"Error al actualizar servicio: {str(e)}")
            raise

    def cambiar_estado_servicio(
            self,
            servicio_id: str,
            cambio_request: CambioEstadoRequest
        ) -> Dict[str, Any]:
            try:
                if not ObjectId.is_valid(servicio_id):
                    raise ValueError("ID de servicio inválido")
                
                servicio_db = self.collection.find_one({"_id": ObjectId(servicio_id)})
                if not servicio_db:
                    raise ValueError("Servicio no encontrado")
                
                servicio_db["id"] = str(servicio_db["_id"])
                del servicio_db["_id"]
                servicio_db = self._convert_datetime_to_date(servicio_db)
                
                servicio = ServicioPrincipal(**servicio_db)
                estado_anterior = servicio.estado
                
                # 1. Aplicar el cambio de estado en el modelo
                servicio.cambiar_estado(
                    nuevo_estado=cambio_request.nuevo_estado,
                    justificacion=cambio_request.justificacion,
                    usuario=cambio_request.usuario
                )
                
                estados_de_cierre = [EstadoServicio.COMPLETADO, EstadoServicio.CANCELADO]
                update_data = servicio.model_dump(by_alias=True, exclude={"id"})
                
                # 2. Actualizar el servicio en la DB
                self.collection.update_one(
                    {"_id": ObjectId(servicio_id)},
                    {"$set": update_data}
                )
                
                # --- LÓGICA DE FLETE AUTOMÁTICO ---
                flete_creado_ahora = False
                if cambio_request.nuevo_estado == EstadoServicio.COMPLETADO:
                    # Verificar si ya existe un flete para este servicio
                    flete_existente = self.db["fletes"].find_one({"servicio_id": servicio_id})
                    
                    if not flete_existente:
                        
                        flete_service = FleteService(self.db)
                        
                        # Crear flete con monto 0.0 y estado PENDIENTE
                        flete_data = {
                            "servicio_id": servicio_id,
                            "codigo_servicio": servicio.codigo_servicio_principal,
                            "monto_flete": 0.0,
                            "estado_flete": "PENDIENTE",
                            "usuario_creador": cambio_request.usuario,
                            "observaciones": f"Generado automáticamente al completar servicio {servicio.codigo_servicio_principal}"
                        }
                        flete_service.create_flete(flete_data)
                        flete_creado_ahora = True
                        logger.info(f"Flete automático creado para el servicio {servicio.codigo_servicio_principal}")
                # ----------------------------------

                # 3. Lógica de Histórico
                historico_creado = None
                if cambio_request.nuevo_estado in estados_de_cierre:
                    tipo_historico = "cancelado" if cambio_request.nuevo_estado == EstadoServicio.CANCELADO else "completado"
                    historico_data = {
                        "servicio_id": servicio_id,
                        "codigo_servicio": servicio.codigo_servicio_principal,
                        "tipo": tipo_historico,
                        "estado_final": cambio_request.nuevo_estado,
                        "fecha_registro": datetime.now(),
                        "usuario": cambio_request.usuario,
                        "justificacion": cambio_request.justificacion
                    }
                    result = self.db["historico_servicios"].insert_one(historico_data)
                    historico_data["id"] = str(result.inserted_id)
                    historico_creado = historico_data

                servicio_actualizado = self.get_servicio_by_id(servicio_id)
                
                response = {
                    "servicio": servicio_actualizado,
                    "permisos": {
                        "puede_editar": servicio.es_editable,
                        "puede_eliminar": servicio.es_eliminable,
                        "puede_cambiar_estado": (
                            servicio.estado != EstadoServicio.COMPLETADO and 
                            not servicio.servicio_cerrado
                        ),
                        "esta_cerrado": servicio.servicio_cerrado
                    },
                    "cambios_aplicados": {
                        "estado_cambiado": True,
                        "estado_anterior": estado_anterior,
                        "estado_nuevo": cambio_request.nuevo_estado,
                        "flete_generado": flete_creado_ahora, # Informamos si se creó el flete
                        "guardado_en_historico": historico_creado is not None
                    }
                }
                
                return response
                    
            except ValueError as e:
                logger.error(f"Error de validación al cambiar estado: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Error al cambiar estado del servicio: {str(e)}")
                raise

    # def _crear_factura_desde_servicio(
    #     self, 
    #     servicio: ServicioPrincipal, 
    #     usuario: str
    # ) -> Dict[str, Any]:
    #     try:
    #         genera_codigo_factura = generate_sequential_code(
    #             counters_collection=self.db["counters"],
    #             target_collection=self.collection,
    #             sequence_name="facturas",
    #             field_name="codigo_factura",
    #             prefix="FAC-",
    #             length=10
    #         )
    #         codigo_factura = genera_codigo_factura
    #         numero_factura = "FAC-SIN-NUMERO"
            
    #         monto_total = float(0)
    #         subtotal = monto_total / 1.18
    #         igv = monto_total - subtotal
            
    #         fecha_emision = datetime.now().date()
    #         fecha_vencimiento = fecha_emision + timedelta(days=30)
            
    #         servicio_data = servicio.model_dump(exclude={"id"})
            
    #         descripcion = (
    #             f"Servicio de {servicio.tipo_servicio} - "
    #             f"{servicio.origen or 'N/A'} a {servicio.destino or 'N/A'}"
    #         )
            
    #         factura_data = {
    #             "codigo_factura": codigo_factura,
    #             "numero_factura": numero_factura,
    #             "servicio": servicio_data,
    #             "fecha_emision": datetime.combine(fecha_emision, datetime.min.time()),
    #             "fecha_vencimiento": datetime.combine(fecha_vencimiento, datetime.min.time()),
    #             "fecha_pago": None,
    #             "estado": "Pendiente",
    #             "monto": monto_total,
    #             "moneda": "PEN",
    #             "subtotal": subtotal,
    #             "igv": igv,
    #             "descuento": 0.00,
    #             "descripcion": descripcion,
    #             "observaciones": f"Factura generada automáticamente al completar servicio - COD:{servicio.codigo_servicio_principal}",
    #             "metodo_pago": None,
    #             "fecha_registro": datetime.now(),
    #             "fecha_actualizacion": None
    #         }
            
    #         result = self.db["facturacion"].insert_one(factura_data)
    #         factura_data["id"] = str(result.inserted_id)
            
    #         logger.info(
    #             f"Factura {numero_factura} (código: {codigo_factura}) creada exitosamente "
    #             f"para servicio {servicio.codigo_servicio_principal}. Monto: {monto_total:.2f}"
    #         )
            
    #         return factura_data
            
    #     except Exception as e:
    #         logger.error(f"Error al crear factura desde servicio: {str(e)}")
    #         raise

    # def _generar_numero_factura(self) -> str:
    #     try:
    #         year = datetime.now().year
    #         prefix = f"FAC-{year}-"
            
    #         ultima_factura = self.db["facturacion"].find_one(
    #             {"codigo_factura": {"$regex": f"^{prefix}"}},
    #             sort=[("fecha_registro", -1)]
    #         )
            
    #         if ultima_factura and "codigo_factura" in ultima_factura:
    #             ultimo_numero = int(ultima_factura["codigo_factura"].split("-")[-1])
    #             nuevo_numero = ultimo_numero + 1
    #         else:
    #             nuevo_numero = 1
            
    #         return f"{prefix}{nuevo_numero:06d}"
            
    #     except Exception as e:
    #         logger.error(f"Error al generar código de factura: {str(e)}")
    #         return f"FAC-{datetime.now().year}-{int(datetime.now().timestamp())}"

    def cerrar_servicio(
        self,
        servicio_id: str,
        cierre_request: CierreServicioRequest
    ) -> Dict[str, Any]:
        try:
            if not ObjectId.is_valid(servicio_id):
                raise ValueError("ID de servicio inválido")
            
            servicio_db = self.collection.find_one({"_id": ObjectId(servicio_id)})
            if not servicio_db:
                raise ValueError("Servicio no encontrado")
            
            servicio_db["id"] = str(servicio_db["_id"])
            del servicio_db["_id"]
            servicio_db = self._convert_datetime_to_date(servicio_db)
            
            servicio = ServicioPrincipal(**servicio_db)
            
            servicio.cerrar_servicio(usuario=cierre_request.usuario)
            
            self.collection.update_one(
                {"codigo_servicio_principal": servicio.codigo_servicio_principal},
                {"$set": servicio.model_dump(by_alias=True)}
            )
            
            servicio_actualizado = self.get_servicio_by_id(servicio_db["id"])
            
            return {
                "servicio": servicio_actualizado,
                "mensaje": "Servicio cerrado exitosamente",
                "permisos": {
                    "puede_editar": False,
                    "puede_eliminar": False,
                    "puede_cambiar_estado": False,
                    "esta_cerrado": True
                }
            }
            
        except ValueError as e:
            logger.error(f"Error al cerrar servicio: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error al cerrar servicio: {str(e)}")
            raise

    def get_historial_servicio(self, servicio_id: str) -> Dict[str, Any]:
        try:
            servicio = self.get_servicio_by_id(servicio_id)
            if not servicio:
                raise ValueError("Servicio no encontrado")
            
            return {
                "codigo_servicio": servicio.get("codigo_servicio_principal"),
                "estado_actual": servicio.get("estado"),
                "historial_estados": servicio.get("historial_estados", []),
                "total_cambios": len(servicio.get("historial_estados", [])),
                "servicio_cerrado": servicio.get("servicio_cerrado", False)
            }
            
        except Exception as e:
            logger.error(f"Error al obtener historial: {str(e)}")
            raise
    
    def verificar_permisos_servicio(self, servicio_id: str) -> Dict[str, Any]:
        try:
            servicio_db = self.get_servicio_by_id(servicio_id)
            if not servicio_db:
                raise ValueError("Servicio no encontrado")
            
            servicio = ServicioPrincipal(**servicio_db)
            
            puede_editar, msg_editar = servicio.puede_editar()
            puede_eliminar, msg_eliminar = servicio.puede_eliminar()
            
            return {
                "codigo_servicio": servicio.codigo_servicio_principal,
                "estado": servicio.estado,
                "servicio_cerrado": servicio.servicio_cerrado,
                "permisos": {
                    "puede_editar": puede_editar,
                    "mensaje_editar": msg_editar,
                    "puede_eliminar": puede_eliminar,
                    "mensaje_eliminar": msg_eliminar,
                    "puede_cambiar_estado": servicio.estado != EstadoServicio.COMPLETADO and not servicio.servicio_cerrado,
                    "esta_cerrado": servicio.servicio_cerrado
                }
            }
            
        except Exception as e:
            logger.error(f"Error al verificar permisos: {str(e)}")
            raise
    
    def export_to_excel(
        self, 
        filter_params: Optional[ServicioPrincipalFilter] = None
    ) -> BytesIO:
        try:
            result = self.get_all_servicios(filter_params, page=1, page_size=10000)
            servicios_list = result["data"]

            def obtener_nombre_persona(persona_dict):
                if not isinstance(persona_dict, dict):
                    return ""
                # Busca en orden de prioridad
                return (
                    persona_dict.get("nombres_completos") or 
                    persona_dict.get("nombres") or 
                    persona_dict.get("nombre") or 
                    ""
                )
            
            if not servicios_list:
                df = pd.DataFrame(columns=[
                    "Código", "Mes", "Solicitud", "Tipo Servicio", "Modalidad Servicio", 
                    "Zona", "Estado", "Fecha Servicio", "Fecha Salida", "Hora Cita",
                    "Cliente", "Proveedor", "Cuenta", "Flota", "Conductores", "Auxiliares",
                    "M3", "TN", "GIA RR", "GIA RT", "Descripción", "Origen", "Destino", 
                    "Cliente Destino", "Responsable", "Fecha Registro", "Editable", 
                    "Servicio Cerrado", "Pertenece a Factura"
                ])
            else:
                excel_data = []
                for servicio in servicios_list:
                    conductores = servicio.get("conductor") or []    
                    auxiliares = servicio.get("auxiliar") or []
                    
                    excel_data.append({
                        "Código": servicio.get("codigo_servicio_principal", ""),
                        "Mes": servicio.get("mes", ""),
                        "Solicitud": servicio.get("solicitud", ""),
                        "Tipo Servicio": servicio.get("tipo_servicio", ""),
                        "Modalidad Servicio": servicio.get("modalidad_servicio", ""),
                        "Zona": servicio.get("zona", ""),
                        "Estado": servicio.get("estado", ""),
                        "Fecha Servicio": servicio.get("fecha_servicio", ""),
                        "Fecha Salida": servicio.get("fecha_salida", ""),
                        "Hora Cita": servicio.get("hora_cita", ""),
                        "Cliente": servicio.get("cliente", {}).get("razon_social", "") or servicio.get("cliente", {}).get("nombre", ""),
                        "Proveedor": servicio.get("proveedor", {}).get("razon_social", "") or servicio.get("proveedor", {}).get("nombre", ""),
                        "Cuenta": servicio.get("cuenta", {}).get("nombre", ""),
                        "Flota": servicio.get("flota", {}).get("placa", "") if servicio.get("flota") else "",
                        "Conductores": ", ".join(
                            obtener_nombre_persona(c) for c in conductores
                        ).strip(", "),
                        
                        "Auxiliares": ", ".join(
                            obtener_nombre_persona(a) for a in auxiliares
                        ).strip(", "),
                        "M3": servicio.get("m3", ""),
                        "TN": servicio.get("tn", ""),
                        "GIA RR": servicio.get("gia_rr", ""),
                        "GIA RT": servicio.get("gia_rt", ""),
                        "Descripción": servicio.get("descripcion", ""),
                        "Origen": servicio.get("origen", ""),
                        "Destino": servicio.get("destino", ""),
                        "Cliente Destino": servicio.get("cliente_destino", ""),
                        "Responsable": servicio.get("responsable", ""),
                        "Fecha Registro": servicio.get("fecha_registro", ""),
                        "Editable": "Sí" if servicio.get("es_editable", True) else "No",
                        "Servicio Cerrado": "Sí" if servicio.get("servicio_cerrado", False) else "No",
                        "Pertenece a Factura": "Sí" if servicio.get("pertenece_a_factura", False) else "No"
                    })
                
                df = pd.DataFrame(excel_data)
            
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Servicios')
                
                worksheet = writer.sheets['Servicios']
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    ) + 2
                    worksheet.column_dimensions[get_column_letter(idx + 1)].width = min(max_length, 50)
            
            output.seek(0)
            return output
            
        except Exception as e:
            logger.error(f"Error al exportar a Excel: {str(e)}")
            raise

    def import_from_excel(self, file_content: bytes) -> Dict[str, Any]:
        try:
            import io
            
            df = pd.read_excel(io.BytesIO(file_content))
            
            created = 0
            errors = []
            
            for index, row in df.iterrows():
                try:
                    fecha_servicio = None
                    if pd.notna(row.get("Fecha Servicio")):
                        if isinstance(row.get("Fecha Servicio"), str):
                            fecha_servicio = datetime.strptime(row.get("Fecha Servicio"), "%Y-%m-%d").date()
                        else:
                            fecha_servicio = pd.to_datetime(row.get("Fecha Servicio")).date()
                    
                    fecha_salida = None
                    if pd.notna(row.get("Fecha Salida")):
                        if isinstance(row.get("Fecha Salida"), str):
                            fecha_salida = datetime.strptime(row.get("Fecha Salida"), "%Y-%m-%d").date()
                        else:
                            fecha_salida = pd.to_datetime(row.get("Fecha Salida")).date()
                    
                    hora_cita = None
                    if pd.notna(row.get("Hora Cita")):
                        if isinstance(row.get("Hora Cita"), str):
                            hora_cita = datetime.strptime(row.get("Hora Cita"), "%H:%M:%S").time()
                        else:
                            hora_cita = pd.to_datetime(row.get("Hora Cita")).time()
                    
                    servicio_data = {
                        "cuenta": {"id": str(row.get("Cuenta ID", "")), "nombre": str(row.get("Cuenta", ""))},
                        "cliente": {"id": str(row.get("Cliente ID", "")), "nombre": str(row.get("Cliente", ""))},
                        "proveedor": {"id": str(row.get("Proveedor ID", "")), "nombre": str(row.get("Proveedor", ""))},
                        "flota": {"id": str(row.get("Flota ID", "")), "placa": str(row.get("Flota", ""))} if pd.notna(row.get("Flota")) else None,
                        "conductor": [{"id": str(row.get("Conductor ID", "")), "nombres": str(row.get("Conductores", ""))}] if pd.notna(row.get("Conductores")) else None,
                        "auxiliar": [{"id": str(row.get("Auxiliar ID", "")), "nombres": str(row.get("Auxiliares", ""))}] if pd.notna(row.get("Auxiliares")) else None,
                        "m3": str(row.get("M3", "")).strip() if pd.notna(row.get("M3")) else None,
                        "tn": str(row.get("TN", "")).strip() if pd.notna(row.get("TN")) else None,
                        "mes": str(row.get("Mes", "")).strip(),
                        "solicitud": str(row.get("Solicitud", "")).strip() if pd.notna(row.get("Solicitud")) else None,
                        "tipo_servicio": str(row.get("Tipo Servicio", "")).strip(),
                        "modalidad_servicio": str(row.get("Modalidad Servicio", "")).strip(),
                        "zona": str(row.get("Zona", "")).strip(),
                        "fecha_servicio": fecha_servicio,
                        "fecha_salida": fecha_salida,
                        "hora_cita": hora_cita,
                        "gia_rr": str(row.get("GIA RR", "")).strip() if pd.notna(row.get("GIA RR")) else None,
                        "gia_rt": str(row.get("GIA RT", "")).strip() if pd.notna(row.get("GIA RT")) else None,
                        "descripcion": str(row.get("Descripción", "")).strip() if pd.notna(row.get("Descripción")) else None,
                        "origen": str(row.get("Origen", "")).strip(),
                        "destino": str(row.get("Destino", "")).strip(),
                        "cliente_destino": str(row.get("Cliente Destino", "")).strip() if pd.notna(row.get("Cliente Destino")) else None,
                        "responsable": str(row.get("Responsable", "")).strip() if pd.notna(row.get("Responsable")) else None
                    }
                    
                    campos_requeridos = {
                        "mes": "Mes",
                        "tipo_servicio": "Tipo de Servicio",
                        "modalidad_servicio": "Modalidad de Servicio",
                        "zona": "Zona",
                        "fecha_servicio": "Fecha de Servicio",
                        "origen": "Origen",
                        "destino": "Destino"
                    }
                    
                    for campo, nombre in campos_requeridos.items():
                        if not servicio_data.get(campo):
                            errors.append(f"Fila {index + 2}: {nombre} es requerido")
                            continue
                    
                    self.create_servicio(servicio_data)
                    created += 1
                        
                except Exception as e:
                    errors.append(f"Fila {index + 2}: Error - {str(e)}")
                    continue
            
            return {
                "total_rows": len(df),
                "created": created,
                "errors": errors,
                "has_errors": len(errors) > 0
            }
            
        except Exception as e:
            logger.error(f"Error al importar desde Excel: {str(e)}")
            raise

    def get_stats(self) -> Dict[str, Any]:
        try:
            total = self.collection.count_documents({})
            
            pipeline_tipo = [
                {"$group": {"_id": "$tipo_servicio", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            tipos = {result["_id"]: result["count"] for result in self.collection.aggregate(pipeline_tipo)}
            
            pipeline_zona = [
                {"$group": {"_id": "$zona", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            zonas = {result["_id"]: result["count"] for result in self.collection.aggregate(pipeline_zona)}
            
            pipeline_mes = [
                {"$group": {"_id": "$mes", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            meses = {result["_id"]: result["count"] for result in self.collection.aggregate(pipeline_mes)}
            
            pipeline_estado = [
                {"$group": {"_id": "$estado", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            estados = {result["_id"]: result["count"] for result in self.collection.aggregate(pipeline_estado)}
            
            hoy_inicio = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            hoy_fin = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            servicios_hoy = self.collection.count_documents({
                "fecha_servicio": {"$gte": hoy_inicio, "$lte": hoy_fin}
            })
            
            primer_dia_mes = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if datetime.now().month == 12:
                ultimo_dia_mes = datetime.now().replace(year=datetime.now().year + 1, month=1, day=1)
            else:
                ultimo_dia_mes = datetime.now().replace(month=datetime.now().month + 1, day=1)
            ultimo_dia_mes = ultimo_dia_mes - timedelta(days=1)
            ultimo_dia_mes = ultimo_dia_mes.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            servicios_mes_actual = self.collection.count_documents({
                "fecha_servicio": {"$gte": primer_dia_mes, "$lte": ultimo_dia_mes}
            })
            
            inicio_semana = datetime.now() - timedelta(days=datetime.now().weekday())
            inicio_semana = inicio_semana.replace(hour=0, minute=0, second=0, microsecond=0)
            fin_semana = inicio_semana + timedelta(days=6)
            fin_semana = fin_semana.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            servicios_semana_actual = self.collection.count_documents({
                "fecha_servicio": {"$gte": inicio_semana, "$lte": fin_semana}
            })
            
            servicios_programados = self.collection.count_documents({"estado": EstadoServicio.PROGRAMADO})
            servicios_completados = self.collection.count_documents({"estado": EstadoServicio.COMPLETADO})
            servicios_cancelados = self.collection.count_documents({"estado": EstadoServicio.CANCELADO})
            servicios_reprogramados = self.collection.count_documents({"estado": EstadoServicio.REPROGRAMADO})
            
            servicios_editables = self.collection.count_documents({"es_editable": True})
            servicios_no_editables = self.collection.count_documents({"es_editable": False})
            servicios_cerrados = self.collection.count_documents({"servicio_cerrado": True})
            servicios_con_factura = self.collection.count_documents({"pertenece_a_factura": True})
            servicios_sin_factura = self.collection.count_documents({"pertenece_a_factura": False})
            
            servicios_lima = self.collection.count_documents({
                "zona": {"$regex": "^Lima", "$options": "i"}
            })
            servicios_provincia = self.collection.count_documents({"zona": "Provincia"})
            
            return {
                "total": total,
                "servicios_hoy": servicios_hoy,
                "servicios_semana_actual": servicios_semana_actual,
                "servicios_mes_actual": servicios_mes_actual,
                "total_lima": servicios_lima,
                "total_provincia": servicios_provincia,
                "total_programados": servicios_programados,
                "total_completados": servicios_completados,
                "total_cancelados": servicios_cancelados,
                "total_reprogramados": servicios_reprogramados,
                "total_editables": servicios_editables,
                "total_no_editables": servicios_no_editables,
                "total_cerrados": servicios_cerrados,
                "total_con_factura": servicios_con_factura,
                "total_sin_factura": servicios_sin_factura,
                "por_tipo": tipos,
                "por_zona": zonas,
                "por_mes": meses,
                "por_estado": estados
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {str(e)}")
            return {}



    def importar_excel_servicios_historicos(self, file_content: bytes) -> Dict[str, Any]:
        """
        Método robusto para importar Excel
        """
        try:
            # Importar el servicio funcional
            from app.modules.servicios.services.carga_excel_servicios import CargaExcelFuncionalServicios
            
            # Crear instancia
            carga_service = CargaExcelFuncionalServicios(self.db)
            
            # Procesar
            resultado = carga_service.cargar_excel_funcional(file_content)
            
            # Agregar estadísticas
            resultado["timestamp"] = datetime.now().isoformat()
            resultado["servicios_totales"] = self.collection.count_documents({})
            
            logger.info(f"✅ Importación completada: {resultado['servicios_creados']} creados, {resultado['errores']} errores")
            
            return resultado
            
        except Exception as e:
            logger.error(f"❌ Error en importación: {str(e)}", exc_info=True)
            # Retornar error controlado
            return {
                "success": False,
                "error": str(e),
                "total_filas_excel": 0,
                "servicios_creados": 0,
                "errores": 1,
                "detalle_errores": [{"error": str(e)}]
            }
    
    # Método de debug mejorado
    def debug_excel(self, file_content: bytes) -> Dict[str, Any]:
        """
        Método de debug que no inserta, solo analiza
        """
        try:
            import pandas as pd
            from io import BytesIO
            
            # Leer Excel
            df = pd.read_excel(BytesIO(file_content))
            
            # Información básica
            info = {
                "filas": len(df),
                "columnas": len(df.columns),
                "nombres_columnas": list(df.columns),
                "tipos_datos": {},
                "primera_fila": {},
                "muestra": {}
            }
            
            # Tipos de datos
            for col in df.columns:
                if len(df) > 0:
                    val = df[col].iloc[0]
                    info["tipos_datos"][col] = {
                        "tipo_python": str(type(val)),
                        "valor_ejemplo": str(val)[:100] if not pd.isna(val) else None,
                        "es_nan": pd.isna(val)
                    }
            
            # Primera fila completa
            if len(df) > 0:
                primera_fila = df.iloc[0]
                info["primera_fila"] = {
                    col: None if pd.isna(val) else str(val)
                    for col, val in primera_fila.items()
                }
            
            # Muestra de 3 filas
            muestra = []
            for i in range(min(3, len(df))):
                fila = df.iloc[i]
                muestra.append({
                    "fila": i+1,
                    "cliente": str(fila.get('CLIENTE', 'N/A')) if 'CLIENTE' in df.columns else 'N/A',
                    "fecha": str(fila.get('F. DE SERVICIO', 'N/A')) if 'F. DE SERVICIO' in df.columns else 'N/A',
                    "proveedor": str(fila.get('PROVEEDOR', 'N/A')) if 'PROVEEDOR' in df.columns else 'N/A'
                })
            info["muestra"] = muestra
            
            # Datos nulos
            info["nulos_por_columna"] = df.isnull().sum().to_dict()
            
            return {
                "success": True,
                "info": info,
                "recomendaciones": self._generar_recomendaciones(df)
            }
            
        except Exception as e:
            logger.error(f"Error en debug: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _generar_recomendaciones(self, df: pd.DataFrame) -> List[str]:
        """Genera recomendaciones basadas en el Excel"""
        recomendaciones = []
        
        # Verificar columnas esperadas
        columnas_esperadas = ['CUENTA', 'CLIENTE', 'PROVEEDOR', 'F. DE SERVICIO']
        columnas_encontradas = [col.upper() for col in df.columns]
        
        for col_esperada in columnas_esperadas:
            if col_esperada not in columnas_encontradas:
                # Buscar similar
                similares = [c for c in columnas_encontradas if col_esperada in c or c in col_esperada]
                if similares:
                    recomendaciones.append(f"'{col_esperada}' no encontrada. Similar: {similares[0]}")
                else:
                    recomendaciones.append(f"Columna '{col_esperada}' no encontrada")
        
        # Verificar nulos
        for col in df.columns:
            nulos = df[col].isnull().sum()
            if nulos > 0:
                porcentaje = (nulos / len(df)) * 100
                if porcentaje > 50:
                    recomendaciones.append(f"Columna '{col}' tiene {porcentaje:.1f}% valores nulos")
        
        return recomendaciones

    def obtener_analiticas_generales(self):
            pipeline = [
                {
                    "$facet": {
                        # 1. Resumen de Estados
                        "estados": [
                            { "$group": { "_id": "$estado", "cantidad": { "$sum": 1 } } }
                        ],
                        # 2. Top Clientes
                        "top_clientes": [
                            { "$group": { "_id": "$cliente.nombre", "total": { "$sum": 1 } } },
                            { "$sort": { "total": -1 } },
                            { "$limit": 5 }
                        ],
                        # 3. Top Placas
                        "top_placas": [
                            { "$group": { "_id": "$flota.placa", "total": { "$sum": 1 } } },
                            { "$sort": { "total": -1 } },
                            { "$limit": 5 }
                        ],
                        # 4. Top 5 Conductores (Desglosando la lista de conductores)
                        "top_conductores": [
                            { "$unwind": "$conductor" },
                            { "$group": { "_id": "$conductor.nombre", "total": { "$sum": 1 } } },
                            { "$sort": { "total": -1 } },
                            { "$limit": 5 }
                        ],
                        # 5. Estadísticas por Periodo (Mes)
                        "servicios_por_mes": [
                            { "$group": { "_id": "$mes", "total": { "$sum": 1 } } },
                            { "$sort": { "total": -1 } }
                        ],
                        # 6. Top Modalidades
                        "top_modalidades": [
                            { "$group": { "_id": "$modalidad_servicio", "total": { "$sum": 1 } } },
                            { "$sort": { "total": -1 } }
                        ],
                        # 7. Total Local vs Provincia
                        "tipos_servicio": [
                            { "$group": { "_id": "$tipo_servicio", "total": { "$sum": 1 } } }
                        ]
                    }
                }
            ]

            resultado = list(self.collection.aggregate(pipeline))
            
            if not resultado:
                return None

            data = resultado[0]

            # Formateo de datos rápidos
            stats_estados = {item["_id"]: item["cantidad"] for item in data["estados"]}
            stats_tipos = {item["_id"]: item["total"] for item in data["tipos_servicio"]}

            return {
                "resumen_general": {
                    "total_fletes": sum(stats_estados.values()),
                    "programados": stats_estados.get("Programado", 0),
                    "cancelados": stats_estados.get("Cancelado", 0),
                    "completados": stats_estados.get("Completado", 0),
                    "total_locales": stats_tipos.get("Local", 0),
                    "total_provincia": stats_tipos.get("Provincia", 0)
                },
                "top_conductores": data["top_conductores"],
                "top_clientes": data["top_clientes"],
                "top_placas": data["top_placas"],
                "top_modalidades": data["top_modalidades"],
                "servicios_por_mes": data["servicios_por_mes"]
            }

            