from typing import Optional, List
from pydantic import BaseModel, Field, field_serializer, model_validator
from datetime import datetime, date
from decimal import Decimal

class FleteRef(BaseModel):
    id: str = Field(..., description="ID del flete relacionado")

class FacturacionBase(BaseModel):
    numero_factura: Optional[str] = None
    fletes: List[FleteRef]
    
    fecha_emision: Optional[date] = None
    fecha_vencimiento: Optional[date] = None
    fecha_pago: Optional[date] = None
    
    estado: str = "Borrador"
    es_borrador: bool = True
    
    monto_total: Decimal
    moneda: str = "PEN"
    
    descripcion: Optional[str] = None

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

class FacturacionCreate(FacturacionBase):
    @model_validator(mode='after')
    def validar_borrador(self):
        if not self.es_borrador or self.estado != "Borrador":
            if not self.numero_factura:
                raise ValueError("numero_factura es requerido para facturas emitidas")
            if not self.fecha_emision:
                raise ValueError("fecha_emision es requerida para facturas emitidas")
            if not self.fecha_vencimiento:
                raise ValueError("fecha_vencimiento es requerida para facturas emitidas")
        return self

class FacturacionUpdate(BaseModel):
    numero_factura: Optional[str] = None
    fletes: Optional[List[FleteRef]] = None
    
    fecha_emision: Optional[date] = None 
    fecha_vencimiento: Optional[date] = None
    fecha_pago: Optional[date] = None
    
    estado: Optional[str] = None
    es_borrador: Optional[bool] = None
    
    monto_total: Optional[Decimal] = None
    moneda: Optional[str] = None
    
    descripcion: Optional[str] = None

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

class FacturacionResponse(FacturacionBase):
    codigo_factura: str
    fecha_registro: datetime
    fecha_actualizacion: Optional[datetime] = None

    class Config:
        from_attributes = True

class FacturacionFilter(BaseModel):
    numero_factura: Optional[str] = None
    estado: Optional[str] = None
    moneda: Optional[str] = None
    es_borrador: Optional[bool] = None
    
    fecha_emision: Optional[date] = None
    fecha_vencimiento: Optional[date] = None
    fecha_pago: Optional[date] = None
    
    fecha_emision_inicio: Optional[date] = None
    fecha_emision_fin: Optional[date] = None
    fecha_vencimiento_inicio: Optional[date] = None
    fecha_vencimiento_fin: Optional[date] = None
    fecha_pago_inicio: Optional[date] = None
    fecha_pago_fin: Optional[date] = None
    
    monto_total_minimo: Optional[Decimal] = None
    monto_total_maximo: Optional[Decimal] = None
    
    flete_id: Optional[str] = None
    
    periodo: Optional[str] = None
    
    nombre_cliente: Optional[str] = None

    class Config:
        from_attributes = True

class FacturacionExcelImportResponse(BaseModel):
    message: str
    result: dict