from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, EmailStr

class Contacto(BaseModel):
    tipo: str = Field(..., description="comercial, contable, logistica, administrativo, gerencia")
    nombre: str = Field(..., description="Nombre del contacto")
    telefono: str = Field(..., description="Teléfono del contacto")

    class Config:
        json_schema_extra = {
            "example": {
                "tipo": "comercial",
                "nombre": "Juan Pérez",
                "telefono": "987654321"
            }
        }

class CuentaFacturacion(BaseModel):
    nombre_cuenta: str = Field(..., description="Nombre de la cuenta de facturación")
    direccion_origen: Optional[str] = Field(None, description="Dirección de recojo o punto de partida asociada a esta cuenta")
    tipo_pago: str = Field(..., description="Contado, Crédito, Crédito Factoring")
    dias_credito: int = Field(default=0, description="Días de crédito para esta cuenta")
    limite_credito: float = Field(default=0.0, description="Límite de crédito en soles")
    estado: str = Field(default="activa", description="activa, suspendida")
    es_principal: bool = Field(default=False, description="Indica si es la cuenta principal")
    
    class Config:
        json_schema_extra = {
            "example": {
                "nombre_cuenta": "Cuenta Principal",
                "direccion_origen": "Av. Los Industriales 123, Ate, Lima",
                "tipo_pago": "Crédito",
                "dias_credito": 30,
                "limite_credito": 50000.00,
                "estado": "activa",
                "es_principal": True
            }
        }

class Cliente(BaseModel):
    # Campos obligatorios
    tipo_documento: str = Field(..., description="RUC, DNI, CE")
    numero_documento: str = Field(..., min_length=3, max_length=11, description="Número de documento de identidad")
    razon_social: str = Field(..., min_length=3, max_length=200, description="Razón social o nombre legal del cliente")
    estado: str = Field(..., description="activo, inactivo, pendiente, suspendido")
    
    # Campos opcionales
    codigo_cliente: Optional[str] = Field(None, min_length=3, max_length=20, description="Código único del cliente (generado automáticamente)")
    tipo_cliente: Optional[str] = Field(None, description="transporte, importador, exportador, distribuidor, minorista, mayorista, manufactura, servicios, construccion, otros")
    periodo_facturacion: Optional[str] = Field(None, description="semanal, quincenal, mensual, bimestral, trimestral, personalizado")
    periodo_facturacion_dias: Optional[int] = Field(None, description="Cantidad de días si el período es personalizado")
    tipo_pago: Optional[str] = Field(None, description="Contado, Crédito, Crédito Factoring (configuración general)")
    dias_credito: Optional[int] = Field(None, description="Días de crédito general")
     
    # Contactos como JSON
    contacto_principal: Optional[str] = Field(None, description="Nombre del contacto principal (primer contacto de la lista)")
    telefono: Optional[str] = Field(None, description="Teléfono del contacto principal")
    contactos: Optional[List[Contacto]] = Field(None, description="Lista de contactos del cliente")
    
    # Cuentas de facturación como JSON
    cuentas_facturacion: Optional[List[CuentaFacturacion]] = Field(None, description="Lista de cuentas de facturación")
    
    # Información adicional opcional
    email: Optional[EmailStr] = Field(None, description="Email de contacto")
    direccion: Optional[str] = Field(None, description="Dirección física del cliente")
    website: Optional[str] = Field(None, description="Sitio web del cliente")
    observaciones: Optional[str] = Field(None, description="Notas adicionales sobre el cliente")
    
    # Metadata
    fecha_registro: datetime = Field(default_factory=datetime.now, description="Fecha de registro del cliente")

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "tipo_documento": "RUC",
                "numero_documento": "20123456789",
                "razon_social": "Constructora Lima S.A.C.",
                "estado": "activo",
                "codigo_cliente": "CL001",
                "tipo_cliente": "construccion",
                "periodo_facturacion": "mensual",
                "periodo_facturacion_dias": None,
                "tipo_pago": "Crédito",
                "dias_credito": 30,
                "contacto_principal": "María García López",
                "telefono": "987654321",
                "contactos": [
                    {
                        "tipo": "comercial",
                        "nombre": "María García López",
                        "telefono": "987654321"
                    }
                ],
                "cuentas_facturacion": [
                    {
                        "nombre_cuenta": "Cuenta Principal",
                        "direccion_origen": "Av. Los Industriales 123, Ate, Lima",
                        "tipo_pago": "Crédito",
                        "dias_credito": 30,
                        "limite_credito": 50000.0,
                        "estado": "activa",
                        "es_principal": True
                    }
                ],
                "email": "contacto@constructoralima.com",
                "direccion": "Av. Javier Prado Este 456, San Isidro, Lima",
                "website": "https://www.constructoralima.com",
                "observaciones": "Cliente preferencial con descuento del 10%",
                "fecha_registro": "2025-01-10T10:30:00"
            }
        }