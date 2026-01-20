from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum

class TipoGastoEnum(str, Enum):
    COMBUSTIBLE = "Combustible"
    PEAJE = "Peaje"
    MANTENIMIENTO = "Mantenimiento"
    REPARACION = "Reparación"
    ESTACIONAMIENTO = "Estacionamiento"
    LAVADO = "Lavado"
    SEGURO = "Seguro"
    MULTA = "Multa"
    LUBRICANTES = "Lubricantes"
    LLANTAS = "Llantas"
    PERSONALIZADO = "Personalizado"

class EstadoGastoEnum(str, Enum):
    PENDIENTE = "pendiente"
    APROBADO = "aprobado"
    RECHAZADO = "rechazado"
    PAGADO = "pagado"

class AmbitoGastoEnum(str, Enum):
    LOCAL = "local"
    NACIONAL = "nacional"

class DetalleGasto(BaseModel):
    tipo_gasto: str = Field(..., description="Tipo de gasto (usar TipoGastoEnum o personalizado)")
    tipo_gasto_personalizado: Optional[str] = Field(None, description="Descripción si tipo_gasto es 'Personalizado'")
    valor: float = Field(..., gt=0, description="Valor del gasto en soles")
    observacion: Optional[str] = Field(None, max_length=500, description="Observaciones adicionales del gasto")
    
    class Config:
        json_schema_extra = {
            "example": {
                "tipo_gasto": "Combustible",
                "tipo_gasto_personalizado": None,
                "valor": 150.50,
                "observacion": "Tanque lleno en Grifo Primax"
            }
        }

class Gasto(BaseModel):
    placa: str = Field(..., min_length=6, max_length=7, description="Placa del vehículo (ej: ABC-123)")
    ambito: str = Field(..., description="local o nacional - determina si permite múltiples gastos")
    fecha_gasto: datetime = Field(default_factory=datetime.now, description="Fecha del gasto (automática pero modificable)")
    detalles_gastos: List[DetalleGasto] = Field(..., min_items=1, description="Lista de detalles de gastos asociados")
    estado: str = Field(default="pendiente", description="Estado del gasto (pendiente, aprobado, rechazado, pagado)")
    id_gasto: Optional[str] = Field(None, description="ID único del gasto (generado automáticamente)")
    fecha_registro: datetime = Field(default_factory=datetime.now, description="Fecha de registro en el sistema")
    usuario_registro: Optional[str] = Field(None, description="Usuario que registró el gasto")
    
    @property
    def total(self) -> float:
        return sum(detalle.valor for detalle in self.detalles_gastos)
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "examples": [
                {
                    "description": "Ejemplo de gasto LOCAL (un solo detalle)",
                    "value": {
                        "placa": "ABC-123",
                        "ambito": "local",
                        "fecha_gasto": "2025-01-19T08:30:00",
                        "detalles_gastos": [
                            {
                                "tipo_gasto": "Combustible",
                                "tipo_gasto_personalizado": None,
                                "valor": 150.50,
                                "observacion": "Tanque lleno - Grifo Primax Av. Javier Prado"
                            }
                        ],
                        "estado": "pendiente"
                    }
                },
                {
                    "description": "Ejemplo de gasto NACIONAL (múltiples detalles)",
                    "value": {
                        "placa": "XYZ-789",
                        "ambito": "nacional",
                        "fecha_gasto": "2025-01-18T14:20:00",
                        "detalles_gastos": [
                            {
                                "tipo_gasto": "Combustible",
                                "tipo_gasto_personalizado": None,
                                "valor": 200.00,
                                "observacion": "Combustible en ruta a Arequipa"
                            },
                            {
                                "tipo_gasto": "Peaje",
                                "tipo_gasto_personalizado": None,
                                "valor": 45.50,
                                "observacion": "Peaje Serpentín de Pasamayo"
                            },
                            {
                                "tipo_gasto": "Personalizado",
                                "tipo_gasto_personalizado": "Refrigerio conductor",
                                "valor": 25.00,
                                "observacion": "Almuerzo en paradero Km 180"
                            },
                            {
                                "tipo_gasto": "Estacionamiento",
                                "tipo_gasto_personalizado": None,
                                "valor": 15.00,
                                "observacion": "Estacionamiento en centro de Arequipa"
                            }
                        ],
                        "estado": "aprobado",
                        "usuario_registro": "admin_logistica"
                    }
                }
            ]
        }