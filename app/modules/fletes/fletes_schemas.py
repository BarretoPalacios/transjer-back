from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, Field
from datetime import datetime

# Schema base: Define la estructura común
class FleteBase(BaseModel):
    codigo_flete: str
    servicio_id: str  # Cambiado a str para mayor compatibilidad con IDs de bases de datos
    codigo_servicio: Optional[str] = None
    estado_flete: str
    monto_flete: float = 0.0
    fecha_pago: Optional[datetime] = None
    observaciones: Optional[str] = None
    pertenece_a_factura: bool = False
    factura_id: Optional[str] = None
    codigo_factura: Optional[str] = None

# Schema para creación
class FleteCreate(BaseModel):
    servicio_id: str
    codigo_servicio: Optional[str] = None
    estado_flete: str = "PENDIENTE"
    monto_flete: float = Field(default=0.0, ge=0)
    observaciones: Optional[str] = Field(None, max_length=500)
    usuario_creador: Optional[str] = None

# Schema para actualización (todo opcional)
class FleteUpdate(BaseModel):
    estado_flete: Optional[str] = None
    monto_flete: Optional[float] = Field(None, ge=0)
    fecha_pago: Optional[datetime] = None
    observaciones: Optional[str] = Field(None, max_length=500)
    pertenece_a_factura: Optional[bool] = None
    factura_id: Optional[str] = None
    codigo_factura: Optional[str] = None

# Schema para respuesta (incluye id y fechas)
class FleteResponse(FleteBase):
    id: str
    fecha_creacion: datetime
    fecha_actualizacion: Optional[datetime] = None
    usuario_creador: Optional[str] = None

    class Config:
        from_attributes = True

# Schema para filtros de búsqueda (incluimos filtros de factura)
class FleteFilter(BaseModel):
    codigo_flete: Optional[str] = None
    servicio_id: Optional[str] = None
    codigo_servicio: Optional[str] = None
    estado_flete: Optional[str] = None
    pertenece_a_factura: Optional[bool] = None
    codigo_factura: Optional[str] = None
    monto_flete_min: Optional[float] = None
    monto_flete_max: Optional[float] = None
    fecha_creacion_desde: Optional[datetime] = None
    fecha_creacion_hasta: Optional[datetime] = None
    fecha_servicio_desde: Optional[datetime] = None
    fecha_servicio_hasta: Optional[datetime] = None

# Schema para importación masiva
class FleteImport(BaseModel):
    fletes: List[FleteCreate]

# Schema para respuesta de importación
class FleteImportResponse(BaseModel):
    message: str
    result: dict
    
    class Config: 
        json_schema_extra = {
            "example": {
                "message": "Importación exitosa. 45 fletes creados",
                "result": {
                    "total_rows": 50,
                    "created": 45,
                    "updated": 0,
                    "skipped": 5,
                    "errors": ["Fila 10: Servicio ID no válido"],
                    "has_errors": True,
                    "success_rate": "90.0%"
                }
            }
        }

# --- Esquemas de Utilidad ---

class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=10, ge=1, le=100)

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    items: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool

    class Config:
        from_attributes = True