from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, Field
from datetime import datetime

# ==========================================
# Schema Base de Gasto Adicional
# ==========================================
class GastoAdicionalBase(BaseModel):
    id_flete: str
    fecha_gasto: datetime
    tipo_gasto: str  # Estadía, Peaje Extra, Maniobra, Reparación, etc.
    descripcion: str
    valor: float
    se_factura_cliente: bool = False
    estado_facturacion: str = "N/A"  # N/A, Pendiente, Facturado
    numero_factura: Optional[str] = None
    estado_aprobacion: str = "pendiente"  # pendiente, aprobado, rechazado
    usuario_registro: str
    codigo_gasto: Optional[str] = None

# ==========================================
# Schema para Creación (POST)
# ==========================================
class GastoAdicionalCreate(BaseModel):
    id_flete: str
    fecha_gasto: datetime = Field(default_factory=datetime.now)
    tipo_gasto: str
    descripcion: str
    valor: float
    se_factura_cliente: bool = False
    usuario_registro: str

# ==========================================
# Schema para Actualización (PATCH)
# ==========================================
class GastoAdicionalUpdate(BaseModel):
    tipo_gasto: Optional[str] = None
    descripcion: Optional[str] = None
    valor: Optional[float] = None
    se_factura_cliente: Optional[bool] = None
    estado_facturacion: Optional[str] = None
    numero_factura: Optional[str] = None
    estado_aprobacion: Optional[str] = None

# ==========================================
# Schema para Respuesta (GET)
# ==========================================
class GastoAdicionalResponse(GastoAdicionalBase):
    id: str  # ID de base de datos (UUID o similar)
    fecha_registro: datetime

    class Config:
        from_attributes = True

# ==========================================
# Schema para Filtros de Búsqueda
# ==========================================
class GastoAdicionalFilter(BaseModel):
    id_flete: Optional[str] = None
    codigo_gasto: Optional[str] = None
    tipo_gasto: Optional[str] = None
    se_factura_cliente: Optional[bool] = None
    estado_facturacion: Optional[str] = None
    estado_aprobacion: Optional[str] = None
    usuario_registro: Optional[str] = None
    fecha_inicio: Optional[datetime] = None
    fecha_fin: Optional[datetime] = None
    numero_factura: Optional[str] = None

# ==========================================
# Paginación y Respuestas Masivas
# ==========================================
T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    items: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool

# Schema para reporte resumido por Flete
class ResumenGastosFlete(BaseModel):
    id_flete: str
    total_gastos: float
    total_recuperable_cliente: float
    total_costo_operativo: float
    gastos: List[GastoAdicionalResponse]