from typing import List, Optional, Dict, Any
from bson import ObjectId
from app.modules.utils.core.code_generator.code_generator import generate_sequential_code
from app.core.database import get_database
from app.modules.gastos_adicionales.model import GastoAdicional
from app.modules.gastos_adicionales.schema import (
    GastoAdicionalCreate, 
    GastoAdicionalUpdate, 
    GastoAdicionalFilter
)
from datetime import datetime
import pandas as pd
from io import BytesIO
import logging
import re
from math import ceil

logger = logging.getLogger(__name__)

class GastoAdicionalService:
    def __init__(self, db):
        self.db = db 
        self.collection = db["gastos_adicionales"]
        self.fletes_collection = db["fletes"]
    
    def create_gasto(self, gasto_data: dict) -> dict:
        """Crear un nuevo gasto adicional"""
        try:
            # Generar código automáticamente
            codigo_gasto = generate_sequential_code(
                counters_collection=self.db["counters"],
                target_collection=self.collection,
                sequence_name="gastos_adicionales",
                field_name="codigo_gasto",
                prefix="G-",
                length=8
            )

            gasto_data["codigo_gasto"] = codigo_gasto
            
            # Definir estado_facturacion basado en se_factura_cliente
            if gasto_data.get("se_factura_cliente"):
                if "estado_facturacion" not in gasto_data or gasto_data["estado_facturacion"] == "N/A":
                    gasto_data["estado_facturacion"] = "Pendiente"
            else:
                gasto_data["estado_facturacion"] = "N/A"
            
            # Agregar fecha de registro si no existe
            if "fecha_registro" not in gasto_data:
                gasto_data["fecha_registro"] = datetime.now()

            # Crear modelo
            gasto_model = GastoAdicional(**gasto_data)

            # Insertar
            result = self.collection.insert_one(
                gasto_model.model_dump(by_alias=True)
            )

            # Retornar creado
            created_gasto = self.collection.find_one(
                {"_id": result.inserted_id}
            )
            
            if created_gasto:
                created_gasto["id"] = str(created_gasto["_id"])
                del created_gasto["_id"]
                
                # Asegurar que fecha_registro exista
                if "fecha_registro" not in created_gasto:
                    created_gasto["fecha_registro"] = datetime.now()

            return created_gasto

        except Exception as e:
            logger.error(f"Error al crear gasto adicional: {str(e)}")
            raise
    
    def get_gasto_by_id(self, gasto_id: str) -> Optional[dict]:
        """Obtener gasto por ID"""
        try:
            if not ObjectId.is_valid(gasto_id):
                return None
            
            gasto = self.collection.find_one({"_id": ObjectId(gasto_id)})
            if gasto:
                gasto["id"] = str(gasto["_id"])
                del gasto["_id"]
                
                # Asegurar que fecha_registro exista
                if "fecha_registro" not in gasto:
                    gasto["fecha_registro"] = datetime.now()
                    
            return gasto
            
        except Exception as e:
            logger.error(f"Error al obtener gasto: {str(e)}")
            return None
    
    def get_gasto_by_codigo(self, codigo_gasto: str) -> Optional[dict]:
        """Obtener gasto por código"""
        try:
            gasto = self.collection.find_one({"codigo_gasto": codigo_gasto})
            if gasto:
                gasto["id"] = str(gasto["_id"])
                del gasto["_id"]
                
                # Asegurar que fecha_registro exista
                if "fecha_registro" not in gasto:
                    gasto["fecha_registro"] = datetime.now()
                    
            return gasto
            
        except Exception as e:
            logger.error(f"Error al obtener gasto por código: {str(e)}")
            return None
    
    def get_all_gastos(
        self,
        filter_params: Optional[GastoAdicionalFilter] = None,
        page: int = 1,
        page_size: int = 10
    ) -> dict:
        """Obtener todos los gastos con filtros opcionales y paginación"""
        try:
            query = {}

            if filter_params:
                if filter_params.id_flete:
                    query["id_flete"] = safe_regex(filter_params.id_flete)

                if filter_params.codigo_gasto:
                    query["codigo_gasto"] = safe_regex(filter_params.codigo_gasto)

                if filter_params.tipo_gasto:
                    query["tipo_gasto"] = safe_regex(filter_params.tipo_gasto)

                if filter_params.se_factura_cliente is not None:
                    query["se_factura_cliente"] = filter_params.se_factura_cliente

                if filter_params.estado_facturacion:
                    query["estado_facturacion"] = filter_params.estado_facturacion

                if filter_params.estado_aprobacion:
                    query["estado_aprobacion"] = filter_params.estado_aprobacion

                if filter_params.numero_factura:
                    query["numero_factura"] = safe_regex(filter_params.numero_factura)

                if filter_params.usuario_registro:
                    query["usuario_registro"] = safe_regex(filter_params.usuario_registro)

                # Filtros de fecha
                if filter_params.fecha_inicio or filter_params.fecha_fin:
                    fecha_query = {}
                    if filter_params.fecha_inicio:
                        fecha_query["$gte"] = filter_params.fecha_inicio
                    if filter_params.fecha_fin:
                        fecha_query["$lte"] = filter_params.fecha_fin
                    if fecha_query:
                        query["fecha_gasto"] = fecha_query

            # Limpiar filtros nulos
            query = {k: v for k, v in query.items() if v is not None}

            total = self.collection.count_documents(query)
            skip = (page - 1) * page_size

            gastos = list(
                self.collection.find(query)
                .sort("fecha_gasto", -1)  # Más recientes primero
                .skip(skip)
                .limit(page_size)
            )

            for gasto in gastos:
                gasto["id"] = str(gasto["_id"])
                del gasto["_id"]
                
                # Asegurar que fecha_registro exista
                if "fecha_registro" not in gasto:
                    gasto["fecha_registro"] = datetime.now()
                
                # Asegurar que fecha_registro exista
                if "fecha_registro" not in gasto:
                    gasto["fecha_registro"] = datetime.now()
                
                # Asegurar que fecha_registro exista
                if "fecha_registro" not in gasto:
                    gasto["fecha_registro"] = datetime.now()

            total_pages = ceil(total / page_size) if page_size > 0 else 0

            return {
                "items": gastos,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }

        except Exception as e:
            logger.error(f"Error al obtener gastos: {str(e)}")
            raise
    
    def update_gasto(self, gasto_id: str, update_data: dict) -> Optional[dict]:
        """Actualizar gasto"""
        try:
            if not ObjectId.is_valid(gasto_id):
                return None
            
            # Filtrar campos None
            update_dict = {k: v for k, v in update_data.items() if v is not None}
            
            if not update_dict:
                return self.get_gasto_by_id(gasto_id)
            
            # Lógica: si cambia se_factura_cliente, actualizar estado_facturacion
            if "se_factura_cliente" in update_dict:
                if update_dict["se_factura_cliente"]:
                    if "estado_facturacion" not in update_dict:
                        update_dict["estado_facturacion"] = "Pendiente"
                else:
                    update_dict["estado_facturacion"] = "N/A"
                    update_dict["numero_factura"] = None
            
            # Actualizar en base de datos
            self.collection.update_one(
                {"_id": ObjectId(gasto_id)},
                {"$set": update_dict}
            )
            
            return self.get_gasto_by_id(gasto_id)
            
        except Exception as e:
            logger.error(f"Error al actualizar gasto: {str(e)}")
            raise
    
    def delete_gasto(self, gasto_id: str) -> bool:
        """Eliminar gasto"""
        try:
            if not ObjectId.is_valid(gasto_id):
                return False
            
            result = self.collection.delete_one({"_id": ObjectId(gasto_id)})
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error al eliminar gasto: {str(e)}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de gastos adicionales"""
        try:
            total = self.collection.count_documents({})
            
            # Contadores por estado de aprobación
            pendientes = self.collection.count_documents({"estado_aprobacion": "pendiente"})
            aprobados = self.collection.count_documents({"estado_aprobacion": "aprobado"})
            rechazados = self.collection.count_documents({"estado_aprobacion": "rechazado"})
            
            # Contadores de facturación
            se_factura = self.collection.count_documents({"se_factura_cliente": True})
            no_factura = self.collection.count_documents({"se_factura_cliente": False})
            
            # Estados de facturación
            facturados = self.collection.count_documents({"estado_facturacion": "Facturado"})
            pendiente_facturacion = self.collection.count_documents({"estado_facturacion": "Pendiente"})
            
            # Agrupar por tipo de gasto
            pipeline_tipo = [
                {"$group": {"_id": "$tipo_gasto", "count": {"$sum": 1}, "total_valor": {"$sum": "$valor"}}}
            ]
            
            tipos = {}
            for result in self.collection.aggregate(pipeline_tipo):
                tipos[result["_id"]] = {
                    "cantidad": result["count"],
                    "total": round(result["total_valor"], 2)
                }
            
            # Calcular totales monetarios
            pipeline_totales = [
                {
                    "$group": {
                        "_id": None,
                        "total_general": {"$sum": "$valor"},
                        "total_facturable": {
                            "$sum": {
                                "$cond": [{"$eq": ["$se_factura_cliente", True]}, "$valor", 0]
                            }
                        },
                        "total_no_facturable": {
                            "$sum": {
                                "$cond": [{"$eq": ["$se_factura_cliente", False]}, "$valor", 0]
                            }
                        },
                        "total_facturado": {
                            "$sum": {
                                "$cond": [{"$eq": ["$estado_facturacion", "Facturado"]}, "$valor", 0]
                            }
                        }
                    }
                }
            ]
            
            totales_result = list(self.collection.aggregate(pipeline_totales))
            totales = totales_result[0] if totales_result else {
                "total_general": 0,
                "total_facturable": 0,
                "total_no_facturable": 0,
                "total_facturado": 0
            }
            
            # Agrupar por usuario
            pipeline_usuarios = [
                {"$group": {"_id": "$usuario_registro", "count": {"$sum": 1}}}
            ]
            
            usuarios = {}
            for result in self.collection.aggregate(pipeline_usuarios):
                usuarios[result["_id"]] = result["count"]
            
            return {
                "total": total,
                "por_estado_aprobacion": {
                    "pendientes": pendientes,
                    "aprobados": aprobados,
                    "rechazados": rechazados
                },
                "facturacion": {
                    "se_factura": se_factura,
                    "no_factura": no_factura,
                    "facturados": facturados,
                    "pendiente_facturacion": pendiente_facturacion
                },
                "totales_monetarios": {
                    "total_general": round(totales.get("total_general", 0), 2),
                    "total_facturable": round(totales.get("total_facturable", 0), 2),
                    "total_no_facturable": round(totales.get("total_no_facturable", 0), 2),
                    "total_facturado": round(totales.get("total_facturado", 0), 2),
                    "pendiente_facturar": round(
                        totales.get("total_facturable", 0) - totales.get("total_facturado", 0), 
                        2
                    )
                },
                "por_tipo_gasto": tipos,
                "por_usuario": usuarios
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {str(e)}")
            return {}
    
    def get_all_gastos_sin_paginacion(
        self,
        filter_params: Optional[GastoAdicionalFilter] = None
    ) -> List[dict]:
        """Obtener TODOS los gastos sin paginación (para exportación)"""
        try:
            query = {}

            if filter_params:
                if filter_params.id_flete:
                    query["id_flete"] = safe_regex(filter_params.id_flete)

                if filter_params.codigo_gasto:
                    query["codigo_gasto"] = safe_regex(filter_params.codigo_gasto)

                if filter_params.tipo_gasto:
                    query["tipo_gasto"] = safe_regex(filter_params.tipo_gasto)

                if filter_params.se_factura_cliente is not None:
                    query["se_factura_cliente"] = filter_params.se_factura_cliente

                if filter_params.estado_facturacion:
                    query["estado_facturacion"] = filter_params.estado_facturacion

                if filter_params.estado_aprobacion:
                    query["estado_aprobacion"] = filter_params.estado_aprobacion

                if filter_params.usuario_registro:
                    query["usuario_registro"] = safe_regex(filter_params.usuario_registro)

                if filter_params.fecha_inicio or filter_params.fecha_fin:
                    fecha_query = {}
                    if filter_params.fecha_inicio:
                        fecha_query["$gte"] = filter_params.fecha_inicio
                    if filter_params.fecha_fin:
                        fecha_query["$lte"] = filter_params.fecha_fin
                    if fecha_query:
                        query["fecha_gasto"] = fecha_query

            query = {k: v for k, v in query.items() if v is not None}

            gastos = list(
                self.collection
                .find(query)
                .sort("fecha_gasto", -1)
            )

            for gasto in gastos:
                gasto["id"] = str(gasto["_id"])
                del gasto["_id"]

            return gastos

        except Exception as e:
            logger.error(f"Error al obtener gastos (sin paginación): {str(e)}")
            raise
    
    def export_to_excel(self, filter_params: Optional[GastoAdicionalFilter] = None) -> BytesIO:
        """Exportar gastos a Excel"""
        try:
            gastos = self.get_all_gastos_sin_paginacion(filter_params)
            
            if not gastos:
                df = pd.DataFrame(columns=[
                    "ID", "Código Gasto", "ID Flete", "Fecha Gasto",
                    "Tipo Gasto", "Descripción", "Valor",
                    "Se Factura Cliente", "Estado Facturación", "Número Factura",
                    "Estado Aprobación", "Usuario Registro", "Fecha Registro"
                ])
            else:
                excel_data = []
                for gasto in gastos:
                    excel_data.append({
                        "ID": gasto.get("id", ""),
                        "Código Gasto": gasto.get("codigo_gasto", ""),
                        "ID Flete": gasto.get("id_flete", ""),
                        "Fecha Gasto": gasto.get("fecha_gasto", "").strftime("%Y-%m-%d %H:%M:%S") if gasto.get("fecha_gasto") else "",
                        "Tipo Gasto": gasto.get("tipo_gasto", ""),
                        "Descripción": gasto.get("descripcion", ""),
                        "Valor": gasto.get("valor", 0),
                        "Se Factura Cliente": "SÍ" if gasto.get("se_factura_cliente") else "NO",
                        "Estado Facturación": gasto.get("estado_facturacion", ""),
                        "Número Factura": gasto.get("numero_factura", "---"),
                        "Estado Aprobación": gasto.get("estado_aprobacion", ""),
                        "Usuario Registro": gasto.get("usuario_registro", ""),
                        "Fecha Registro": gasto.get("fecha_registro", "").strftime("%Y-%m-%d %H:%M:%S") if gasto.get("fecha_registro") else ""
                    })
                
                df = pd.DataFrame(excel_data)
            
            # Crear Excel en memoria
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Gastos Adicionales')
                
                # Formato opcional
                workbook = writer.book
                worksheet = writer.sheets['Gastos Adicionales']
                
                from openpyxl.styles import Font, PatternFill, Alignment
                
                header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
                header_font = Font(color="FFFFFF", bold=True)
                
                for cell in worksheet[1]:
                    cell.fill = header_fill
                    cell.font = header_font
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                
                # Ajustar anchos
                column_widths = {
                    'A': 25, 'B': 15, 'C': 15, 'D': 20,
                    'E': 18, 'F': 40, 'G': 12, 'H': 18,
                    'I': 20, 'J': 18, 'K': 18, 'L': 20, 'M': 20
                }
                
                for col, width in column_widths.items():
                    worksheet.column_dimensions[col].width = width
            
            output.seek(0)
            return output
            
        except Exception as e:
            logger.error(f"Error al exportar a Excel: {str(e)}")
            raise
    
    def get_gastos_by_flete(self, id_flete: str) -> Dict[str, Any]:
        """Obtener resumen de gastos por flete"""
        try:
            gastos = list(self.collection.find({"id_flete": id_flete}))
            
            for gasto in gastos:
                gasto["id"] = str(gasto["_id"])
                del gasto["_id"]
            
            total_gastos = sum(g.get("valor", 0) for g in gastos)
            total_recuperable = sum(
                g.get("valor", 0) for g in gastos 
                if g.get("se_factura_cliente") == True
            )
            total_costo_operativo = sum(
                g.get("valor", 0) for g in gastos 
                if g.get("se_factura_cliente") == False
            )
            
            return {
                "id_flete": id_flete,
                "total_gastos": round(total_gastos, 2),
                "total_recuperable_cliente": round(total_recuperable, 2),
                "total_costo_operativo": round(total_costo_operativo, 2),
                "cantidad_gastos": len(gastos),
                "gastos": gastos
            }
            
        except Exception as e:
            logger.error(f"Error al obtener gastos por flete: {str(e)}")
            return {}

    def get_gastos_by_code_flete(self, id_flete: str) -> Dict[str, Any]:
        """Obtener resumen de gastos por flete"""
        try:
            # print(id_flete)
            flete = self.fletes_collection.find_one({
                    "codigo_flete": id_flete
                })
            # print(flete)

            if not flete:
                raise ValueError("Flete no encontrado")

            # 3️⃣ Obtener ID REAL del flete
            id_flete = str(flete["_id"])

            # 4️⃣ Buscar gastos asociados
            gastos = list(self.collection.find({
                "id_flete": id_flete
            }))
            
            for gasto in gastos:
                gasto["id"] = str(gasto["_id"])
                del gasto["_id"]
            
            total_gastos = sum(g.get("valor", 0) for g in gastos)
            total_recuperable = sum(
                g.get("valor", 0) for g in gastos 
                if g.get("se_factura_cliente") == True
            )
            total_costo_operativo = sum(
                g.get("valor", 0) for g in gastos 
                if g.get("se_factura_cliente") == False
            )
            
            return {
                "id_flete": id_flete,
                "total_gastos": round(total_gastos, 2),
                "total_recuperable_cliente": round(total_recuperable, 2),
                "total_costo_operativo": round(total_costo_operativo, 2),
                "cantidad_gastos": len(gastos),
                "gastos": gastos
            }
            
        except Exception as e:
            logger.error(f"Error al obtener gastos por flete: {str(e)}")
            return {}


def safe_regex(value: str):
    """Función auxiliar para crear regex seguro"""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    return {"$regex": re.escape(value), "$options": "i"}