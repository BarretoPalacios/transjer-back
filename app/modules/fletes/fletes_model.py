from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class Flete(BaseModel):
    # Identificadores y Relaciones
    codigo_flete: str = Field(..., max_length=20, description="Código único del flete (Ej: FLT-2026-001)")
    servicio_id: str = Field(..., description="ID o ObjectId del servicio asociado")
    codigo_servicio: Optional[str] = Field(None, description="Código legible del servicio (Ej: SERV-1002)")
    
    # Estado y Montos (Solo en PEN)
    estado_flete: str = Field(default="PENDIENTE", description="PENDIENTE, APROBADO, FACTURADO, PAGADO, CANCELADO")
    monto_flete: float = Field(default=0.0, ge=0, description="Monto del flete en Soles (PEN)")
    
    # Trazabilidad de Facturación
    pertenece_a_factura: bool = Field(default=False, description="Indica si el flete ya está en una factura")
    factura_id: Optional[str] = Field(None, description="Referencia al ID de la factura")
    codigo_factura: Optional[str] = Field(None, description="Número correlativo de la factura")
    
    # Fechas y Auditoría
    fecha_pago: Optional[datetime] = Field(None, description="Fecha de registro del pago")
    observaciones: Optional[str] = Field(None, max_length=500, description="Notas adicionales")
    fecha_creacion: datetime = Field(default_factory=datetime.now, description="Fecha de registro automático")
    fecha_actualizacion: Optional[datetime] = Field(None, description="Fecha de última modificación")
    usuario_creador: Optional[str] = Field(None, description="Usuario que generó el registro")

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "codigo_flete": "FLT-2026-001",
                "servicio_id": "659b...",
                "codigo_servicio": "SERV-1002",
                "estado_flete": "PENDIENTE",
                "monto_flete": 0.0,
                "pertenece_a_factura": False,
                "factura_id": None,
                "codigo_factura": None,
                "fecha_creacion": "2026-01-09T19:30:00",
                "usuario_creador": "admin_user"
            }
        }