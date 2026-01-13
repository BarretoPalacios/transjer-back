from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, EmailStr

class Contacto(BaseModel):
    tipo: str = Field(
        ...,
        description="comercial, contable, operaciones, soporte, gerencia"
    )
    nombre: str = Field(..., min_length=3, max_length=150)
    telefono: Optional[str] = Field(None, max_length=20)
    email: Optional[EmailStr] = None

    class Config:
        json_schema_extra = {
            "example": {
                "tipo": "operaciones",
                "nombre": "Carlos Rodríguez",
                "telefono": "987654321",
                "email": "carlos@proveedor.com"
            }
        }


class CuentaPagoProveedor(BaseModel):
    nombre_cuenta: str = Field(..., max_length=100)
    tipo_pago: str = Field(
        ...,
        description="Contado, Crédito, Leasing, Factoring"
    )
    dias_credito: int = Field(default=0, ge=0)
    limite_credito: float = Field(default=0.0, ge=0)
    estado: str = Field(
        default="activa",
        description="activa, suspendida"
    )
    es_principal: bool = Field(default=False)

    class Config:
        json_schema_extra = {
            "example": {
                "nombre_cuenta": "Cuenta Principal",
                "tipo_pago": "Crédito",
                "dias_credito": 30,
                "limite_credito": 50000,
                "estado": "activa",
                "es_principal": True
            }
        }


class Proveedor(BaseModel):
    # 🔹 Identificación
    tipo_documento: str = Field(
        ...,
        description="RUC, DNI, CE"
    )
    numero_documento: str = Field(
        ...,
        min_length=8,
        max_length=20,
        description="Número de documento"
    )
    razon_social: str = Field(
        ...,
        min_length=3,
        max_length=200
    )
    estado: str = Field(
        ...,
        description="activo, inactivo, suspendido"
    )

    codigo_proveedor: Optional[str] = Field(None, min_length=3, max_length=20, description="Código único del proveedor (generado automáticamente)")

    # 🔹 Clasificación
    rubro_proveedor: str = Field(
        ...,
        description="transportista, logistica, seguridad, mantenimiento, tecnologia, seguros, servicios, otros"
    )

    # 🔹 Servicios (lista simple de strings)
    servicios: Optional[List[str]] = Field(
        None,
        description="Lista de servicios que ofrece el proveedor"
    )

    # 🔹 Contactos
    contacto_principal: Optional[str] = Field(
        None,
        description="Nombre del contacto principal"
    )
    contactos: Optional[List[Contacto]] = None

    # 🔹 Cuentas de pago
    cuentas_pago: Optional[List[CuentaPagoProveedor]] = None

    # 🔹 Información general
    telefono: Optional[str] = Field(None, max_length=20)
    email: Optional[EmailStr] = None
    direccion: Optional[str] = None
    website: Optional[str] = None

    # 🔹 Observaciones
    observaciones: Optional[str] = Field(None, max_length=500)

    # 🔹 Metadata
    fecha_registro: datetime = Field(
        default_factory=datetime.now,
        description="Fecha de registro"
    )

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "tipo_documento": "RUC",
                "numero_documento": "20123456788",
                "razon_social": "Transportes del Norte SAC",
                "estado": "activo",
                "rubro_proveedor": "transportista",
                "servicios": [
                    "Transporte de carga pesada",
                    "Transporte refrigerado",
                    "Mudanzas empresariales"
                ],
                "contacto_principal": "Carlos Rodríguez",
                "contactos": [
                    {
                        "tipo": "operaciones",
                        "nombre": "Carlos Rodríguez",
                        "telefono": "987654321",
                        "email": "carlos@transnorte.com"
                    },
                    {
                        "tipo": "contable",
                        "nombre": "Ana Torres",
                        "telefono": "987654322"
                    }
                ],
                "cuentas_pago": [
                    {
                        "nombre_cuenta": "Cuenta Principal",
                        "tipo_pago": "Crédito",
                        "dias_credito": 30,
                        "limite_credito": 50000,
                        "estado": "activa",
                        "es_principal": True
                    }
                ],
                "telefono": "987654321",
                "email": "contacto@transnorte.com",
                "direccion": "Av. Los Transportistas 456, Lima",
                "website": "www.transnorte.com",
                "observaciones": "Proveedor confiable y puntual",
                "fecha_registro": "2025-01-10T10:30:00"
            }
        }