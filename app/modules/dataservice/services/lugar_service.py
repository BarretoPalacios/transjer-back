from typing import List, Optional, Dict, Any
from bson import ObjectId
from app.modules.utils.core.code_generator.code_generator import generate_sequential_code
from app.core.database import get_database
from app.modules.dataservice.models.lugar import Lugar
from app.modules.dataservice.schemas.lugar_schema import LugarCreate, LugarUpdate, LugarFilter
from datetime import datetime
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

class LugarService:
    def __init__(self, db):
        self.db = db 
        self.collection = db["lugares"]
    
    def create_lugar(self, lugar_data: dict) -> dict:
        """Crear un nuevo lugar"""
        try:
            # 1️⃣ Generar código automáticamente
            codigo_lugar = generate_sequential_code(
                counters_collection=self.db["counters"],
                target_collection=self.collection,
                sequence_name="lugares",
                field_name="codigo_lugar",
                prefix="LUG-",
                length=10
            )

            lugar_data["codigo_lugar"] = codigo_lugar

            # 2️⃣ Crear modelo
            lugar_model = Lugar(**lugar_data)

            # 3️⃣ Si es principal, desmarcar otros del mismo tipo
            if lugar_model.es_principal:
                self.collection.update_many(
                    {"tipo_lugar": lugar_model.tipo_lugar},
                    {"$set": {"es_principal": False}}
                )

            # 4️⃣ Insertar en base de datos
            result = self.collection.insert_one(
                lugar_model.model_dump(by_alias=True)
            )

            # 5️⃣ Retornar lugar creado
            created_lugar = self.collection.find_one(
                {"_id": result.inserted_id}
            )
            created_lugar["id"] = str(created_lugar["_id"])
            del created_lugar["_id"]

            return created_lugar

        except Exception as e:
            logger.error(f"Error al crear lugar: {str(e)}")
            raise

    
    def get_lugar_by_id(self, lugar_id: str) -> Optional[dict]:
        """Obtener lugar por ID"""
        try:
            if not ObjectId.is_valid(lugar_id):
                return None
            
            lugar = self.collection.find_one({"_id": ObjectId(lugar_id)})
            if lugar:
                lugar["id"] = str(lugar["_id"])
                del lugar["_id"]
            return lugar
            
        except Exception as e:
            logger.error(f"Error al obtener lugar: {str(e)}")
            return None
    
    def get_lugar_by_codigo(self, codigo_lugar: str) -> Optional[dict]:
        """Obtener lugar por código"""
        try:
            lugar = self.collection.find_one({"codigo_lugar": codigo_lugar})
            if lugar:
                lugar["id"] = str(lugar["_id"])
                del lugar["_id"]
            return lugar
            
        except Exception as e:
            logger.error(f"Error al obtener lugar por código: {str(e)}")
            return None
    
    def get_lugares_by_tipo(self, tipo_lugar: str) -> List[dict]:
        """Obtener lugares por tipo"""
        try:
            lugares = list(self.collection.find({"tipo_lugar": tipo_lugar}).sort("nombre", 1))
            
            for lugar in lugares:
                lugar["id"] = str(lugar["_id"])
                del lugar["_id"]
            
            return lugares
            
        except Exception as e:
            logger.error(f"Error al obtener lugares por tipo: {str(e)}")
            return []
    
    def get_lugar_principal_by_tipo(self, tipo_lugar: str) -> Optional[dict]:
        """Obtener el lugar principal por tipo"""
        try:
            lugar = self.collection.find_one({
                "tipo_lugar": tipo_lugar,
                "es_principal": True,
                "estado": "activo"
            })
            
            if lugar:
                lugar["id"] = str(lugar["_id"])
                del lugar["_id"]
            
            return lugar
            
        except Exception as e:
            logger.error(f"Error al obtener lugar principal: {str(e)}")
            return None
    
    def get_all_lugares(self, filter_params: Optional[LugarFilter] = None) -> List[dict]:
        """Obtener todos los lugares con filtros opcionales"""
        try:
            query = {}
            if filter_params:
                if filter_params.codigo_lugar:
                    query["codigo_lugar"] = {"$regex": filter_params.codigo_lugar, "$options": "i"}
                if filter_params.nombre:
                    query["nombre"] = {"$regex": filter_params.nombre, "$options": "i"}
                if filter_params.tipo_lugar:
                    query["tipo_lugar"] = filter_params.tipo_lugar
                if filter_params.distrito:
                    query["distrito"] = {"$regex": filter_params.distrito, "$options": "i"}
                if filter_params.provincia:
                    query["provincia"] = {"$regex": filter_params.provincia, "$options": "i"}
                if filter_params.estado:
                    query["estado"] = filter_params.estado
                if filter_params.es_principal is not None:
                    query["es_principal"] = filter_params.es_principal
            
            lugares = list(self.collection.find(query).sort("codigo_lugar", -1))
            
            # Convertir ObjectId a string
            for lugar in lugares:
                lugar["id"] = str(lugar["_id"])
                del lugar["_id"]
            
            return lugares
            
        except Exception as e:
            logger.error(f"Error al obtener lugares: {str(e)}")
            return []
    
    def update_lugar(self, lugar_id: str, update_data: dict) -> Optional[dict]:
        """Actualizar lugar"""
        try:
            if not ObjectId.is_valid(lugar_id):
                return None
            
            # Filtrar campos None
            update_dict = {k: v for k, v in update_data.items() if v is not None}
            
            if not update_dict:
                return self.get_lugar_by_id(lugar_id)
            
            # Si se marca como principal, desmarcar otros del mismo tipo
            if update_dict.get("es_principal") == True:
                lugar_actual = self.get_lugar_by_id(lugar_id)
                tipo_lugar = lugar_actual.get("tipo_lugar")
                
                self.collection.update_many(
                    {
                        "tipo_lugar": tipo_lugar,
                        "_id": {"$ne": ObjectId(lugar_id)}
                    },
                    {"$set": {"es_principal": False}}
                )
            
            # Actualizar en base de datos
            self.collection.update_one(
                {"_id": ObjectId(lugar_id)},
                {"$set": update_dict}
            )
            
            return self.get_lugar_by_id(lugar_id)
            
        except Exception as e:
            logger.error(f"Error al actualizar lugar: {str(e)}")
            raise
    
    def delete_lugar(self, lugar_id: str) -> bool:
        """Eliminar lugar"""
        try:
            if not ObjectId.is_valid(lugar_id):
                return False
            
            # Verificar si es el principal antes de eliminar
            lugar = self.get_lugar_by_id(lugar_id)
            if lugar and lugar.get("es_principal"):
                # Buscar otro lugar del mismo tipo para hacerlo principal
                mismo_tipo = list(self.collection.find({
                    "tipo_lugar": lugar["tipo_lugar"],
                    "_id": {"$ne": ObjectId(lugar_id)}
                }).limit(1))
                
                if mismo_tipo:
                    self.collection.update_one(
                        {"_id": mismo_tipo[0]["_id"]},
                        {"$set": {"es_principal": True}}
                    )
            
            result = self.collection.delete_one({"_id": ObjectId(lugar_id)})
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error al eliminar lugar: {str(e)}")
            return False
    
    def export_to_excel(self, filter_params: Optional[LugarFilter] = None) -> BytesIO:
        """Exportar lugares a Excel"""
        try:
            lugares = self.get_all_lugares(filter_params)
            
            if not lugares:
                # Crear DataFrame vacío
                df = pd.DataFrame(columns=[
                    "ID", "Código Lugar", "Nombre", "Tipo Lugar", 
                    "Dirección", "Distrito", "Provincia", "Departamento",
                    "Coordenadas", "Contacto", "Teléfono", "Horario Atención",
                    "Capacidad Estacionamiento", "Servicios Disponibles",
                    "Estado", "Es Principal", "Observaciones"
                ])
            else:
                # Preparar datos para Excel
                excel_data = []
                for lugar in lugares:
                    excel_data.append({
                        "ID": lugar.get("id", ""),
                        "Código Lugar": lugar.get("codigo_lugar", ""),
                        "Nombre": lugar.get("nombre", ""),
                        "Tipo Lugar": lugar.get("tipo_lugar", ""),
                        "Dirección": lugar.get("direccion", ""),
                        "Distrito": lugar.get("distrito", ""),
                        "Provincia": lugar.get("provincia", ""),
                        "Departamento": lugar.get("departamento", ""),
                        "Coordenadas": str(lugar.get("coordenadas", "")) if lugar.get("coordenadas") else "",
                        "Contacto": lugar.get("contacto", ""),
                        "Teléfono": lugar.get("telefono", ""),
                        "Horario Atención": lugar.get("horario_atencion", ""),
                        "Capacidad Estacionamiento": lugar.get("capacidad_estacionamiento", ""),
                        "Servicios Disponibles": ", ".join(lugar.get("servicios_disponibles", [])),
                        "Estado": lugar.get("estado", ""),
                        "Es Principal": "Sí" if lugar.get("es_principal") else "No",
                        "Observaciones": lugar.get("observaciones", "")
                    })
                
                df = pd.DataFrame(excel_data)
            
            # Crear Excel en memoria
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Lugares')
            
            output.seek(0)
            return output
            
        except Exception as e:
            logger.error(f"Error al exportar a Excel: {str(e)}")
            raise
    
    def import_from_excel(self, file_content: bytes) -> Dict[str, Any]:
        """Importar lugares desde Excel"""
        try:
            import io
            
            df = pd.read_excel(io.BytesIO(file_content))
            
            created = 0
            updated = 0
            errors = []
            
            for index, row in df.iterrows():
                try:
                    lugar_data = {
                        "codigo_lugar": str(row.get("Código Lugar", "")).strip(),
                        "nombre": str(row.get("Nombre", "")).strip(),
                        "tipo_lugar": str(row.get("Tipo Lugar", "")).strip(),
                        "direccion": str(row.get("Dirección", "")).strip(),
                        "distrito": str(row.get("Distrito", "")).strip(),
                        "provincia": str(row.get("Provincia", "")).strip(),
                        "departamento": str(row.get("Departamento", "")).strip(),
                        "coordenadas": eval(row.get("Coordenadas", "{}")) if pd.notna(row.get("Coordenadas")) and row.get("Coordenadas") else None,
                        "contacto": str(row.get("Contacto", "")).strip() if pd.notna(row.get("Contacto")) else None,
                        "telefono": str(row.get("Teléfono", "")).strip() if pd.notna(row.get("Teléfono")) else None,
                        "horario_atencion": str(row.get("Horario Atención", "")).strip() if pd.notna(row.get("Horario Atención")) else None,
                        "capacidad_estacionamiento": int(row.get("Capacidad Estacionamiento")) if pd.notna(row.get("Capacidad Estacionamiento")) else None,
                        "servicios_disponibles": [s.strip() for s in str(row.get("Servicios Disponibles", "")).split(",") if s.strip()] if pd.notna(row.get("Servicios Disponibles")) else [],
                        "estado": str(row.get("Estado", "activo")).strip(),
                        "es_principal": bool(row.get("Es Principal", False)),
                        "observaciones": str(row.get("Observaciones", "")).strip() if pd.notna(row.get("Observaciones")) else None
                    }
                    
                    # Validar campos obligatorios
                    if not lugar_data["codigo_lugar"]:
                        errors.append(f"Fila {index + 2}: Código de lugar es requerido")
                        continue
                    
                    if not lugar_data["nombre"]:
                        errors.append(f"Fila {index + 2}: Nombre es requerido")
                        continue
                    
                    if not lugar_data["tipo_lugar"]:
                        errors.append(f"Fila {index + 2}: Tipo de lugar es requerido")
                        continue
                    
                    if not lugar_data["direccion"]:
                        errors.append(f"Fila {index + 2}: Dirección es requerida")
                        continue
                    
                    if not lugar_data["distrito"]:
                        errors.append(f"Fila {index + 2}: Distrito es requerido")
                        continue
                    
                    if not lugar_data["provincia"]:
                        errors.append(f"Fila {index + 2}: Provincia es requerida")
                        continue
                    
                    if not lugar_data["departamento"]:
                        errors.append(f"Fila {index + 2}: Departamento es requerido")
                        continue
                    
                    # Verificar tipo de lugar válido
                    tipos_validos = ["origen", "destino", "almacen", "taller", "oficina"]
                    if lugar_data["tipo_lugar"] not in tipos_validos:
                        errors.append(f"Fila {index + 2}: Tipo de lugar inválido. Debe ser: {', '.join(tipos_validos)}")
                        continue
                    
                    # Verificar si ya existe por código
                    existing = self.collection.find_one({"codigo_lugar": lugar_data["codigo_lugar"]})
                    if existing:
                        # Si se marca como principal, desmarcar otros
                        if lugar_data.get("es_principal"):
                            self.collection.update_many(
                                {
                                    "tipo_lugar": lugar_data["tipo_lugar"],
                                    "_id": {"$ne": existing["_id"]}
                                },
                                {"$set": {"es_principal": False}}
                            )
                        
                        # Actualizar lugar existente
                        self.collection.update_one(
                            {"codigo_lugar": lugar_data["codigo_lugar"]},
                            {"$set": lugar_data}
                        )
                        updated += 1
                    else:
                        # Crear nuevo lugar
                        self.create_lugar(lugar_data)
                        created += 1
                        
                except Exception as e:
                    errors.append(f"Fila {index + 2}: Error - {str(e)}")
                    continue
            
            return {
                "total_rows": len(df),
                "created": created,
                "updated": updated,
                "errors": errors,
                "has_errors": len(errors) > 0
            }
            
        except Exception as e:
            logger.error(f"Error al importar desde Excel: {str(e)}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de lugares"""
        try:
            total = self.collection.count_documents({})
            activos = self.collection.count_documents({"estado": "activo"})
            inactivos = self.collection.count_documents({"estado": "inactivo"})
            principales = self.collection.count_documents({"es_principal": True})
            
            # Agrupar por tipo de lugar
            pipeline_tipo = [
                {"$group": {"_id": "$tipo_lugar", "count": {"$sum": 1}}}
            ]
            
            tipos = {}
            for result in self.collection.aggregate(pipeline_tipo):
                tipos[result["_id"]] = result["count"]
            
            # Agrupar por departamento
            pipeline_depto = [
                {"$group": {"_id": "$departamento", "count": {"$sum": 1}}}
            ]
            
            departamentos = {}
            for result in self.collection.aggregate(pipeline_depto):
                departamentos[result["_id"]] = result["count"]
            
            # Agrupar por provincia
            pipeline_prov = [
                {"$group": {"_id": "$provincia", "count": {"$sum": 1}}}
            ]
            
            provincias = {}
            for result in self.collection.aggregate(pipeline_prov):
                provincias[result["_id"]] = result["count"]
            
            return {
                "total": total,
                "activos": activos,
                "inactivos": inactivos,
                "principales": principales,
                "por_tipo_lugar": tipos,
                "por_departamento": departamentos,
                "por_provincia": provincias
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {str(e)}")
            return {}