from typing import List, Optional, Dict, Any
from bson import ObjectId
from app.modules.utils.core.code_generator.code_generator import generate_sequential_code
from app.core.database import get_database
from app.modules.dataservice.models.cuenta import Cuenta
from app.modules.dataservice.schemas.cuenta_schema import CuentaCreate, CuentaUpdate, CuentaFilter
from datetime import datetime
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

class CuentaService:
    def __init__(self, db):
        self.db = db 
        self.collection = db["cuentas"]
        self.clientes_collection = db["clientes"]
    
    def _validate_cliente(self, codigo_cliente: str) -> bool:
        """Validar que el cliente existe y está activo"""
        cliente = self.clientes_collection.find_one({
            "codigo_cliente": codigo_cliente,
            "estado": "activo"
        })
        if not cliente:
            raise ValueError(f"El cliente {codigo_cliente} no existe o no está activo")
        return True
    
    def create_cuenta(self, cuenta_data: dict) -> dict:
        try:
            # 1️⃣ Validar que el cliente existe
            self._validate_cliente(cuenta_data["codigo_cliente"])
            
            # 2️⃣ Verificar RUC único (regla de negocio)
            existing_ruc = self.collection.find_one({
                "ruc": cuenta_data["ruc"]
            })
            if existing_ruc:
                raise ValueError(
                    f"El RUC {cuenta_data['ruc']} ya está registrado"
                )

            # 3️⃣ Generar código automáticamente
            codigo_cuenta = generate_sequential_code(
                counters_collection=self.db["counters"],
                target_collection=self.collection,
                sequence_name="cuentas",
                field_name="codigo_cuenta",
                prefix="CTA-",
                length=10
            )

            cuenta_data["codigo_cuenta"] = codigo_cuenta

            # 4️⃣ Crear modelo
            cuenta_model = Cuenta(**cuenta_data)

            # 5️⃣ Insertar
            result = self.collection.insert_one(
                cuenta_model.model_dump(by_alias=True)
            )

            # 6️⃣ Retornar creado
            created_cuenta = self.collection.find_one(
                {"_id": result.inserted_id}
            )
            created_cuenta["id"] = str(created_cuenta["_id"])
            del created_cuenta["_id"]

            return created_cuenta

        except Exception as e:
            logger.error(f"Error al crear cuenta: {str(e)}")
            raise
    
    def get_cuenta_by_id(self, cuenta_id: str) -> Optional[dict]:
        """Obtener cuenta por ID"""
        try:
            if not ObjectId.is_valid(cuenta_id):
                return None
            
            cuenta = self.collection.find_one({"_id": ObjectId(cuenta_id)})
            if cuenta:
                cuenta["id"] = str(cuenta["_id"])
                del cuenta["_id"]
            return cuenta
            
        except Exception as e:
            logger.error(f"Error al obtener cuenta: {str(e)}")
            return None
    
    def get_cuenta_by_codigo(self, codigo_cuenta: str) -> Optional[dict]:
        """Obtener cuenta por código"""
        try:
            cuenta = self.collection.find_one({"codigo_cuenta": codigo_cuenta})
            if cuenta:
                cuenta["id"] = str(cuenta["_id"])
                del cuenta["_id"]
            return cuenta
            
        except Exception as e:
            logger.error(f"Error al obtener cuenta por código: {str(e)}")
            return None
    
    def get_cuenta_by_ruc(self, ruc: str) -> Optional[dict]:
        """Obtener cuenta por RUC"""
        try:
            cuenta = self.collection.find_one({"ruc": ruc})
            if cuenta:
                cuenta["id"] = str(cuenta["_id"])
                del cuenta["_id"]
            return cuenta
            
        except Exception as e:
            logger.error(f"Error al obtener cuenta por RUC: {str(e)}")
            return None
    
    def get_cuentas_by_cliente(self, codigo_cliente: str) -> List[dict]:
        """Obtener todas las cuentas de un cliente"""
        try:
            cuentas = list(self.collection.find({"codigo_cliente": codigo_cliente}).sort("nombre", 1))
            
            # Convertir ObjectId a string
            for cuenta in cuentas:
                cuenta["id"] = str(cuenta["_id"])
                del cuenta["_id"]
            
            return cuentas
            
        except Exception as e:
            logger.error(f"Error al obtener cuentas por cliente: {str(e)}")
            return []
    
    def get_all_cuentas(self, filter_params: Optional[CuentaFilter] = None) -> List[dict]:
        """Obtener todas las cuentas con filtros opcionales"""
        try:
            query = {}
            if filter_params:
                if filter_params.codigo_cuenta:
                    query["codigo_cuenta"] = {"$regex": filter_params.codigo_cuenta, "$options": "i"}
                if filter_params.codigo_cliente:
                    query["codigo_cliente"] = {"$regex": filter_params.codigo_cliente, "$options": "i"}
                if filter_params.nombre:
                    query["nombre"] = {"$regex": filter_params.nombre, "$options": "i"}
                if filter_params.ruc:
                    query["ruc"] = {"$regex": filter_params.ruc, "$options": "i"}
                if filter_params.contacto:
                    query["contacto"] = {"$regex": filter_params.contacto, "$options": "i"}
                if filter_params.tipo_cliente:
                    query["tipo_cliente"] = filter_params.tipo_cliente
                if filter_params.estado:
                    query["estado"] = filter_params.estado
            
            cuentas = list(self.collection.find(query).sort("nombre", 1))
            
            # Convertir ObjectId a string
            for cuenta in cuentas:
                cuenta["id"] = str(cuenta["_id"])
                del cuenta["_id"]
            
            return cuentas
            
        except Exception as e:
            logger.error(f"Error al obtener cuentas: {str(e)}")
            return []
    
    def update_cuenta(self, cuenta_id: str, update_data: dict) -> Optional[dict]:
        """Actualizar cuenta"""
        try:
            if not ObjectId.is_valid(cuenta_id):
                return None
            
            # Filtrar campos None
            update_dict = {k: v for k, v in update_data.items() if v is not None}
            
            if not update_dict:
                return self.get_cuenta_by_id(cuenta_id)
            
            # Si se actualiza el codigo_cliente, validar que existe
            if "codigo_cliente" in update_dict:
                self._validate_cliente(update_dict["codigo_cliente"])
            
            # Si se actualiza el RUC, verificar que no exista otra cuenta con el mismo
            if "ruc" in update_dict:
                existing = self.collection.find_one({
                    "ruc": update_dict["ruc"],
                    "_id": {"$ne": ObjectId(cuenta_id)}
                }) 
                
                if existing:
                    raise ValueError(f"Ya existe una cuenta con el RUC {update_dict['ruc']}")
            
            # Actualizar en base de datos
            self.collection.update_one(
                {"_id": ObjectId(cuenta_id)},
                {"$set": update_dict}
            )
            
            return self.get_cuenta_by_id(cuenta_id)
            
        except Exception as e:
            logger.error(f"Error al actualizar cuenta: {str(e)}")
            raise
    
    def delete_cuenta(self, cuenta_id: str) -> bool:
        """Eliminar cuenta"""
        try:
            if not ObjectId.is_valid(cuenta_id):
                return False
            
            result = self.collection.delete_one({"_id": ObjectId(cuenta_id)})
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error al eliminar cuenta: {str(e)}")
            return False
    
    def export_to_excel(self, filter_params: Optional[CuentaFilter] = None) -> BytesIO:
        """Exportar cuentas a Excel"""
        try:
            cuentas = self.get_all_cuentas(filter_params)
            
            if not cuentas:
                # Crear DataFrame vacío
                df = pd.DataFrame(columns=[
                    "ID", "Código Cuenta", "Código Cliente", "Nombre", "RUC",
                    "Dirección", "Teléfono", "Email", "Contacto",
                    "Tipo Cliente", "Límite Crédito", "Estado",
                    "Fecha Registro", "Notas"
                ])
            else:
                # Preparar datos para Excel
                excel_data = []
                for cuenta in cuentas:
                    excel_data.append({
                        "ID": cuenta.get("id", ""),
                        "Código Cuenta": cuenta.get("codigo_cuenta", ""),
                        "Código Cliente": cuenta.get("codigo_cliente", ""),
                        "Nombre": cuenta.get("nombre", ""),
                        "RUC": cuenta.get("ruc", ""),
                        "Dirección": cuenta.get("direccion", ""),
                        "Teléfono": cuenta.get("telefono", ""),
                        "Email": cuenta.get("email", ""),
                        "Contacto": cuenta.get("contacto", ""),
                        "Tipo Cliente": cuenta.get("tipo_cliente", ""),
                        "Límite Crédito": cuenta.get("limite_credito", 0.0),
                        "Estado": cuenta.get("estado", ""),
                        "Fecha Registro": cuenta.get("fecha_registro", "").strftime("%Y-%m-%d %H:%M:%S") if cuenta.get("fecha_registro") else "",
                        "Notas": cuenta.get("notas", "")
                    })
                
                df = pd.DataFrame(excel_data)
            
            # Crear Excel en memoria
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Cuentas')
            
            output.seek(0)
            return output
            
        except Exception as e:
            logger.error(f"Error al exportar a Excel: {str(e)}")
            raise
    
    def import_from_excel(self, file_content: bytes) -> Dict[str, Any]:
        """Importar cuentas desde Excel"""
        try:
            import io
            
            df = pd.read_excel(io.BytesIO(file_content))
            
            created = 0
            updated = 0
            errors = []
            
            for index, row in df.iterrows():
                try:
                    cuenta_data = {
                        "codigo_cuenta": str(row.get("Código Cuenta", "")).strip(),
                        "codigo_cliente": str(row.get("Código Cliente", "")).strip(),
                        "nombre": str(row.get("Nombre", "")).strip(),
                        "ruc": str(row.get("RUC", "")).strip(),
                        "direccion": str(row.get("Dirección", "")).strip(),
                        "telefono": str(row.get("Teléfono", "")).strip(),
                        "email": str(row.get("Email", "")).strip() if pd.notna(row.get("Email")) else None,
                        "contacto": str(row.get("Contacto", "")).strip() if pd.notna(row.get("Contacto")) else None,
                        "tipo_cliente": str(row.get("Tipo Cliente", "Regular")).strip(),
                        "limite_credito": float(row.get("Límite Crédito", 0.0)) if pd.notna(row.get("Límite Crédito")) else 0.0,
                        "estado": str(row.get("Estado", "activo")).strip(),
                        "notas": str(row.get("Notas", "")).strip() if pd.notna(row.get("Notas")) else None,
                        "fecha_registro": datetime.now()
                    }
                    
                    # Validar campos obligatorios
                    if not cuenta_data["codigo_cuenta"]:
                        errors.append(f"Fila {index + 2}: Código de cuenta es requerido")
                        continue
                    
                    if not cuenta_data["codigo_cliente"]:
                        errors.append(f"Fila {index + 2}: Código de cliente es requerido")
                        continue
                    
                    # Validar que el cliente existe
                    try:
                        self._validate_cliente(cuenta_data["codigo_cliente"])
                    except ValueError as ve:
                        errors.append(f"Fila {index + 2}: {str(ve)}")
                        continue
                    
                    if not cuenta_data["nombre"]:
                        errors.append(f"Fila {index + 2}: Nombre es requerido")
                        continue
                    
                    if not cuenta_data["ruc"]:
                        errors.append(f"Fila {index + 2}: RUC es requerido")
                        continue
                    
                    if not cuenta_data["direccion"]:
                        errors.append(f"Fila {index + 2}: Dirección es requerida")
                        continue
                    
                    if not cuenta_data["telefono"]:
                        errors.append(f"Fila {index + 2}: Teléfono es requerido")
                        continue
                    
                    # Verificar si ya existe por código
                    existing = self.collection.find_one({"codigo_cuenta": cuenta_data["codigo_cuenta"]})
                    if existing:
                        # Actualizar cuenta existente
                        self.collection.update_one(
                            {"codigo_cuenta": cuenta_data["codigo_cuenta"]},
                            {"$set": cuenta_data}
                        )
                        updated += 1
                    else:
                        # Verificar si existe por RUC
                        existing_ruc = self.collection.find_one({
                            "ruc": cuenta_data["ruc"]
                        })
                        
                        if existing_ruc:
                            errors.append(f"Fila {index + 2}: Ya existe una cuenta con el RUC {cuenta_data['ruc']}")
                            continue
                        
                        # Crear nueva cuenta
                        self.collection.insert_one(cuenta_data)
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
        """Obtener estadísticas de cuentas"""
        try:
            total = self.collection.count_documents({})
            activas = self.collection.count_documents({"estado": "activo"})
            inactivas = self.collection.count_documents({"estado": "inactivo"})
            
            # Agrupar por tipo de cliente
            pipeline_tipo = [
                {"$group": {"_id": "$tipo_cliente", "count": {"$sum": 1}}}
            ]
            
            tipos = {}
            for result in self.collection.aggregate(pipeline_tipo):
                tipos[result["_id"] if result["_id"] else "Sin especificar"] = result["count"]
            
            # Calcular límite de crédito total
            pipeline_credito = [
                {"$group": {"_id": None, "total_limite": {"$sum": "$limite_credito"}}}
            ]
            
            total_limite = 0
            for result in self.collection.aggregate(pipeline_credito):
                total_limite = result["total_limite"]
            
            # Cuentas por cliente (top 10)
            pipeline_clientes = [
                {"$group": {"_id": "$codigo_cliente", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            
            por_cliente = {}
            for result in self.collection.aggregate(pipeline_clientes):
                por_cliente[result["_id"]] = result["count"]
            
            return {
                "total": total,
                "activas": activas,
                "inactivas": inactivas,
                "por_tipo_cliente": tipos,
                "total_limite_credito": total_limite,
                "top_clientes": por_cliente
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {str(e)}")
            return {}