from datetime import datetime, date 
from typing import Optional, List
from pydantic import BaseModel, Field, field_serializer
from decimal import Decimal

class FleteRef(BaseModel):
    id: str = Field(..., description="ID del flete relacionado")

class Facturacion(BaseModel):
    codigo_factura: str = Field(
        ..., 
        min_length=3, 
        max_length=20, 
        description="Código único de la factura (PK)"
    )
    
    numero_factura: Optional[str] = Field(
        None, 
        max_length=50, 
        description="Número de factura legible. Puede ser None si es borrador"
    )
    
    fletes: List[FleteRef] = Field(
        ..., 
        description="Lista de fletes asociados a esta factura"
    )
    
    fecha_emision: Optional[date] = Field(
        None,
        description="Fecha de emisión de la factura"
    )
    
    fecha_vencimiento: Optional[date] = Field(
        None, 
        description="Fecha de vencimiento para el pago"
    )
    
    fecha_pago: Optional[date] = Field(
        None, 
        description="Fecha en que se realizó el pago"
    )
    
    estado: str = Field(
        default="Borrador",
        max_length=50,
        description="Estado: Borrador, Pendiente, Pagada, Vencida, Anulada, Parcial"
    )
    
    monto_total: Decimal = Field(
        ..., 
        ge=0, 
        description="Monto total final de la factura"
    )
    
    moneda: str = Field(
        default="PEN",
        max_length=3,
        description="Moneda: PEN, USD, EUR"
    )
    
    descripcion: Optional[str] = Field(
        None, 
        max_length=500, 
        description="Descripción o concepto de la factura"
    )
    
    es_borrador: bool = Field(
        default=True,
        description="Indica si la factura está en estado de borrador"
    )
    
    fecha_registro: datetime = Field(
        default_factory=datetime.now,
        description="Fecha de registro en el sistema"
    )
    
    fecha_actualizacion: Optional[datetime] = Field(
        None,
        description="Fecha de última actualización"
    )

    @field_serializer('fecha_emision', 'fecha_vencimiento', 'fecha_pago') 
    def serialize_date(self, value: Optional[date], _info):
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, datetime.min.time())
        return value
    
    @field_serializer('monto_total')
    def serialize_decimal(self, value: Optional[Decimal], _info):
        if value is None:
            return None
        return float(value)

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "codigo_factura": "FAC-2024-001",
                "numero_factura": "F001-00000123",
                "fletes": [
                    {"id": "695c711b9f90ed0d3484188a"},
                    {"id": "695c70f09f90ed0d34841889"}
                ],
                "fecha_emision": "2024-01-20",
                "fecha_vencimiento": "2024-02-20",
                "fecha_pago": None,
                "estado": "Pendiente",
                "monto_total": 1500.00,
                "moneda": "PEN",
                "descripcion": "Facturación por fletes de transporte de carga",
                "es_borrador": False
            }
        }