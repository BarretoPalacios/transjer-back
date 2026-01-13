from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field, EmailStr, field_serializer, field_validator
import re

class Personal(BaseModel):
    dni: str = Field(..., min_length=8, max_length=15, description="Número de documento de identidad")
    nombres_completos: str = Field(..., min_length=2, max_length=200, description="Nombres y apellidos completos del trabajador")
    tipo: str = Field(..., description="Tipo de personal: Conductor, Auxiliar, Operario, Administrativo, Supervisor, Mecánico, Almacenero")
    
    # Campos opcionales
    estado: Optional[str] = Field(None, description="Estado del trabajador: Activo, Inactivo, Licencia, Vacaciones")
    fecha_ingreso: Optional[date] = Field(None, description="Fecha de ingreso a la empresa")
    fecha_nacimiento: Optional[date] = Field(None, description="Fecha de nacimiento")
    telefono: Optional[str] = Field(None, description="Teléfono de contacto")
    email: Optional[EmailStr] = Field(None, description="Email corporativo o personal")
    direccion: Optional[str] = Field(None, description="Dirección de domicilio")
    licencia_conducir: Optional[str] = Field(None, description="Número de licencia de conducir (para conductores)")
    categoria_licencia: Optional[str] = Field(None, description="Categoría de licencia: A-I, A-II-a, A-II-b, A-III-a, A-III-b, A-III-c")
    fecha_venc_licencia: Optional[date] = Field(None, description="Fecha de vencimiento de licencia")
    turno: Optional[str] = Field(None, description="Turno de trabajo: Día, Noche, Rotativo")
    salario: Optional[float] = Field(None, ge=0, description="Salario mensual")
    banco: Optional[str] = Field(None, description="Banco para pago de salario")
    numero_cuenta: Optional[str] = Field(None, description="Número de cuenta bancaria")
    contacto_emergencia: Optional[str] = Field(None, description="Nombre de contacto de emergencia")
    telefono_emergencia: Optional[str] = Field(None, description="Teléfono de contacto de emergencia")
    observaciones: Optional[str] = Field(None, description="Observaciones adicionales")
    fecha_registro: datetime = Field(default_factory=datetime.now, description="Fecha de registro en el sistema")

    @field_serializer('fecha_ingreso', 'fecha_nacimiento', 'fecha_venc_licencia')
    def serialize_date(self, value: Optional[date], _info):
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, datetime.min.time())
        return value

    @field_validator('dni')
    @classmethod
    def validate_dni(cls, v):
        # Validación básica de DNI (solo números)
        if not re.match(r'^\d+$', v):
            raise ValueError('El DNI debe contener solo números')
        return v

   

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "dni": "12345678",
                "nombres_completos": "Juan Carlos Pérez García",
                "tipo": "Conductor",
                "estado": "Activo",
                "fecha_ingreso": "2022-03-15",
                "fecha_nacimiento": "1985-07-20",
                "telefono": "987654321",
                "email": "juan.perez@empresa.com",
                "direccion": "Jr. Los Olivos 456, San Juan de Lurigancho, Lima",
                "licencia_conducir": "Q12345678",
                "categoria_licencia": "A-III-b",
                "fecha_venc_licencia": "2026-07-20",
                "turno": "Día",
                "salario": 2500.00,
                "banco": "Banco de Crédito del Perú",
                "numero_cuenta": "19312345678901",
                "contacto_emergencia": "María Pérez",
                "telefono_emergencia": "965432187",
                "observaciones": "Conductor responsable, experiencia en rutas interprovinciales"
            }
        }