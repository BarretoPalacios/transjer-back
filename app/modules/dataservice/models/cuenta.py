from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, EmailStr
from bson import ObjectId

class Cuenta(BaseModel):
    codigo_cuenta: str = Field(..., min_length=3, max_length=20, description="Código único de la cuenta")
    codigo_cliente: str = Field(..., description="Código del cliente (foreign key)")
    nombre: str = Field(..., min_length=3, max_length=200, description="Nombre de la cuenta")
    ruc: str = Field(..., min_length=11, max_length=20, description="RUC de la cuenta")
    direccion: str = Field(..., min_length=5, max_length=300, description="Dirección de la cuenta")
    telefono: str = Field(..., description="Teléfono de contacto")
    email: Optional[EmailStr] = Field(None, description="Email de contacto")
    contacto: Optional[str] = Field(None, max_length=150, description="Persona de contacto")
    tipo_cliente: str = Field(default="Regular", description="Tipo de cliente: Regular, VIP, etc.")
    limite_credito:  Optional[float]  = Field(None, description="Límite de crédito")
    estado: str = Field(default="activo", description="activo, inactivo, suspendido")
    fecha_registro: datetime = Field(default_factory=datetime.now, description="Fecha de registro de la cuenta")
    notas: Optional[str] = Field(None, description="Notas adicionales")

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "codigo_cuenta": "CTA001",
                "codigo_cliente": "CL001",
                "nombre": "Constructora Lima S.A.C.",
                "ruc": "20123456789",
                "direccion": "Av. Javier Prado Este 456, San Isidro, Lima",
                "telefono": "987654321",
                "email": "contacto@constructoralima.com",
                "contacto": "María García López",
                "tipo_cliente": "VIP",
                "limite_credito": 50000.00,
                "estado": "activo",
                "fecha_registro": "2025-01-10T10:30:00",
                "notas": "Cliente preferencial"
            }
        }