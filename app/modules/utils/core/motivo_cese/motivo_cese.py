from datetime import date, datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from bson import ObjectId
from io import BytesIO
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# =====================================================
# MODELOS
# =====================================================

class CesePersonal(BaseModel):
    """Modelo de Cese Personal - almacena datos completos del trabajador"""
    personal: Dict[str, Any] = Field(..., description="Datos completos del trabajador")
    fecha_cese: date = Field(..., description="Fecha de cese del trabajador")
    motivo_cese: str = Field(
        ...,
        description="Motivo del cese: Renuncia, Fin de contrato, Despido arbitrario, Despido por falta grave, Mutuo acuerdo, Abandono, Fallecimiento"
    )
    detalle: Optional[str] = Field(None, description="Detalle u observaciones del cese")
    indemnizacion: Optional[float] = Field(0, ge=0, description="Monto de indemnización si aplica")
    registrado_por: Optional[str] = Field(None, description="Usuario que registró el cese")
    fecha_registro: datetime = Field(default_factory=datetime.now)

    class Config:
        json_schema_extra = {
            "example": {
                "personal": {
                    "id": "64fd9c8a",
                    "codigo_personal": "PER-001",
                    "nombres": "Juan",
                    "apellidos": "Pérez García",
                    "tipo_documento": "DNI",
                    "numero_documento": "12345678",
                    "cargo": "Operario",
                    "area": "Producción"
                },
                "fecha_cese": "2025-01-15",
                "motivo_cese": "Despido arbitrario",
                "detalle": "Reducción de personal",
                "indemnizacion": 5000.00
            }
        }


# =====================================================
# SCHEMAS
# =====================================================

class CesePersonalCreate(BaseModel):
    """Schema para crear un nuevo cese"""
    personal_id: str = Field(..., description="ID del trabajador a cesar")
    fecha_cese: date = Field(..., description="Fecha de cese")
    motivo_cese: str = Field(..., description="Motivo del cese")
    detalle: Optional[str] = None
    indemnizacion: Optional[float] = Field(0, ge=0)
    registrado_por: Optional[str] = None


class CesePersonalUpdate(BaseModel):
    """Schema para actualizar un cese existente"""
    fecha_cese: Optional[date] = None
    motivo_cese: Optional[str] = None
    detalle: Optional[str] = None
    indemnizacion: Optional[float] = Field(None, ge=0)


class CesePersonalResponse(BaseModel):
    """Schema de respuesta con ID"""
    id: str
    personal: Dict[str, Any]
    fecha_cese: date
    motivo_cese: str
    detalle: Optional[str] = None
    indemnizacion: float
    registrado_por: Optional[str] = None
    fecha_registro: datetime

    class Config:
        from_attributes = True


class CesePersonalFilter(BaseModel):
    """Schema para filtros de búsqueda"""
    motivo_cese: Optional[str] = None
    fecha_cese_desde: Optional[date] = None
    fecha_cese_hasta: Optional[date] = None
    codigo_personal: Optional[str] = None
    numero_documento: Optional[str] = None
    area: Optional[str] = None
    cargo: Optional[str] = None


class ExcelImportResponse(BaseModel):
    """Respuesta de importación Excel"""
    message: str
    result: dict


# =====================================================
# SERVICE
# =====================================================

class CesePersonalService:
    """Servicio para gestión de Ceses de Personal"""
    
    def __init__(self, db):
        self.db = db
        self.collection = db["ceses_personal"]
        self.personal_collection = db["personal"]
    
    def create_cese(self, cese_data: dict) -> dict:
        """Crear un nuevo cese de personal"""
        try:
            personal_id = cese_data.pop("personal_id")
            
            # Obtener datos completos del personal
            if not ObjectId.is_valid(personal_id):
                raise ValueError("ID de personal no válido")
            
            personal = self.personal_collection.find_one({"_id": ObjectId(personal_id)})
            if not personal:
                raise ValueError(f"Personal con ID {personal_id} no encontrado")
            
            # Verificar si ya existe un cese para este personal
            existing_cese = self.collection.find_one({"personal.id": str(personal["_id"])})
            if existing_cese:
                raise ValueError(
                    f"Ya existe un registro de cese para {personal.get('nombres')} {personal.get('apellidos')}"
                )
            
            # Convertir ObjectId a string para guardar
            personal["id"] = str(personal["_id"])
            del personal["_id"]
            
            # Agregar datos del personal al cese
            cese_data["personal"] = personal
            cese_data["fecha_registro"] = datetime.now()
            
            # Crear modelo
            cese_model = CesePersonal(**cese_data)
            
            # Insertar
            result = self.collection.insert_one(cese_model.model_dump())
            
            # Retornar creado
            created_cese = self.collection.find_one({"_id": result.inserted_id})
            created_cese["id"] = str(created_cese["_id"])
            del created_cese["_id"]
            
            # Actualizar estado del personal a "cesado"
            self.personal_collection.update_one(
                {"_id": ObjectId(personal_id)},
                {"$set": {"estado": "cesado", "fecha_cese": cese_data["fecha_cese"]}}
            )
            
            return created_cese
            
        except Exception as e:
            logger.error(f"Error al crear cese: {str(e)}")
            raise
    
    def get_cese_by_id(self, cese_id: str) -> Optional[dict]:
        """Obtener cese por ID"""
        try:
            if not ObjectId.is_valid(cese_id):
                return None
            
            cese = self.collection.find_one({"_id": ObjectId(cese_id)})
            if cese:
                cese["id"] = str(cese["_id"])
                del cese["_id"]
            return cese
            
        except Exception as e:
            logger.error(f"Error al obtener cese: {str(e)}")
            return None
    
    def get_cese_by_personal_id(self, personal_id: str) -> Optional[dict]:
        """Obtener cese por ID del personal"""
        try:
            cese = self.collection.find_one({"personal.id": personal_id})
            if cese:
                cese["id"] = str(cese["_id"])
                del cese["_id"]
            return cese
            
        except Exception as e:
            logger.error(f"Error al obtener cese por personal: {str(e)}")
            return None
    
    def get_all_ceses(self, filter_params: Optional[CesePersonalFilter] = None) -> List[dict]:
        """Obtener todos los ceses con filtros opcionales"""
        try:
            query = {}
            
            if filter_params:
                if filter_params.motivo_cese:
                    query["motivo_cese"] = filter_params.motivo_cese
                
                if filter_params.fecha_cese_desde or filter_params.fecha_cese_hasta:
                    query["fecha_cese"] = {}
                    if filter_params.fecha_cese_desde:
                        query["fecha_cese"]["$gte"] = filter_params.fecha_cese_desde
                    if filter_params.fecha_cese_hasta:
                        query["fecha_cese"]["$lte"] = filter_params.fecha_cese_hasta
                
                if filter_params.codigo_personal:
                    query["personal.codigo_personal"] = {
                        "$regex": filter_params.codigo_personal, 
                        "$options": "i"
                    }
                
                if filter_params.numero_documento:
                    query["personal.numero_documento"] = {
                        "$regex": filter_params.numero_documento,
                        "$options": "i"
                    }
                
                if filter_params.area:
                    query["personal.area"] = {"$regex": filter_params.area, "$options": "i"}
                
                if filter_params.cargo:
                    query["personal.cargo"] = {"$regex": filter_params.cargo, "$options": "i"}
            
            ceses = list(self.collection.find(query).sort("fecha_cese", -1))
            
            # Convertir ObjectId a string
            for cese in ceses:
                cese["id"] = str(cese["_id"])
                del cese["_id"]
            
            return ceses
            
        except Exception as e:
            logger.error(f"Error al obtener ceses: {str(e)}")
            return []
    
    def update_cese(self, cese_id: str, update_data: dict) -> Optional[dict]:
        """Actualizar un cese existente"""
        try:
            if not ObjectId.is_valid(cese_id):
                return None
            
            # Filtrar campos None
            update_dict = {k: v for k, v in update_data.items() if v is not None}
            
            if not update_dict:
                return self.get_cese_by_id(cese_id)
            
            # Actualizar
            self.collection.update_one(
                {"_id": ObjectId(cese_id)},
                {"$set": update_dict}
            )
            
            return self.get_cese_by_id(cese_id)
            
        except Exception as e:
            logger.error(f"Error al actualizar cese: {str(e)}")
            raise
    
    def delete_cese(self, cese_id: str) -> bool:
        """Eliminar un cese y reactivar al personal"""
        try:
            if not ObjectId.is_valid(cese_id):
                return False
            
            # Obtener el cese antes de eliminarlo
            cese = self.get_cese_by_id(cese_id)
            if not cese:
                return False
            
            # Eliminar cese
            result = self.collection.delete_one({"_id": ObjectId(cese_id)})
            
            if result.deleted_count > 0:
                # Reactivar personal
                personal_id = cese["personal"]["id"]
                self.personal_collection.update_one(
                    {"_id": ObjectId(personal_id)},
                    {"$set": {"estado": "activo"}, "$unset": {"fecha_cese": ""}}
                )
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error al eliminar cese: {str(e)}")
            return False
    
    def export_to_excel(self, filter_params: Optional[CesePersonalFilter] = None) -> BytesIO:
        """Exportar ceses a Excel"""
        try:
            ceses = self.get_all_ceses(filter_params)
            
            if not ceses:
                df = pd.DataFrame(columns=[
                    "ID", "Código Personal", "Nombres", "Apellidos", 
                    "Tipo Documento", "Número Documento", "Cargo", "Área",
                    "Fecha Cese", "Motivo Cese", "Detalle", "Indemnización",
                    "Registrado Por", "Fecha Registro"
                ])
            else:
                excel_data = []
                for cese in ceses:
                    personal = cese.get("personal", {})
                    excel_data.append({
                        "ID": cese.get("id", ""),
                        "Código Personal": personal.get("codigo_personal", ""),
                        "Nombres": personal.get("nombres", ""),
                        "Apellidos": personal.get("apellidos", ""),
                        "Tipo Documento": personal.get("tipo_documento", ""),
                        "Número Documento": personal.get("numero_documento", ""),
                        "Cargo": personal.get("cargo", ""),
                        "Área": personal.get("area", ""),
                        "Fecha Cese": cese.get("fecha_cese", ""),
                        "Motivo Cese": cese.get("motivo_cese", ""),
                        "Detalle": cese.get("detalle", ""),
                        "Indemnización": cese.get("indemnizacion", 0),
                        "Registrado Por": cese.get("registrado_por", ""),
                        "Fecha Registro": cese.get("fecha_registro", "").strftime("%Y-%m-%d %H:%M:%S") 
                                         if cese.get("fecha_registro") else ""
                    })
                
                df = pd.DataFrame(excel_data)
            
            # Crear Excel en memoria
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Ceses Personal')
            
            output.seek(0)
            return output
            
        except Exception as e:
            logger.error(f"Error al exportar a Excel: {str(e)}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de ceses"""
        try:
            total = self.collection.count_documents({})
            
            # Agrupar por motivo de cese
            pipeline_motivo = [
                {"$group": {"_id": "$motivo_cese", "count": {"$sum": 1}}}
            ]
            
            motivos = {}
            for result in self.collection.aggregate(pipeline_motivo):
                motivos[result["_id"]] = result["count"]
            
            # Agrupar por área
            pipeline_area = [
                {"$group": {"_id": "$personal.area", "count": {"$sum": 1}}}
            ]
            
            areas = {}
            for result in self.collection.aggregate(pipeline_area):
                areas[result["_id"] if result["_id"] else "Sin especificar"] = result["count"]
            
            # Calcular indemnización total
            pipeline_indem = [
                {"$group": {"_id": None, "total": {"$sum": "$indemnizacion"}}}
            ]
            
            indemnizacion_total = 0
            for result in self.collection.aggregate(pipeline_indem):
                indemnizacion_total = result.get("total", 0)
            
            # Ceses por mes (últimos 12 meses)
            pipeline_mes = [
                {
                    "$group": {
                        "_id": {
                            "$dateToString": {"format": "%Y-%m", "date": "$fecha_cese"}
                        },
                        "count": {"$sum": 1}
                    }
                },
                {"$sort": {"_id": 1}}
            ]
            
            ceses_por_mes = {}
            for result in self.collection.aggregate(pipeline_mes):
                ceses_por_mes[result["_id"]] = result["count"]
            
            return {
                "total": total,
                "por_motivo": motivos,
                "por_area": areas,
                "indemnizacion_total": round(indemnizacion_total, 2),
                "por_mes": ceses_por_mes
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {str(e)}")
            return {}