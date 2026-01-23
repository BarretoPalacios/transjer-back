from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, Field
from datetime import datetime, date

# Schema base
class FlotaBase(BaseModel):
    codigo_flota: Optional[str] = Field(..., min_length=1, max_length=20, description="Código único de la flota")
    placa: str = Field(..., min_length=6, max_length=10, description="Placa del vehículo")
    marca: str = Field(..., min_length=2, max_length=50, description="Marca del vehículo")
    modelo: str = Field(..., min_length=1, max_length=50, description="Modelo del vehículo")
    anio: int = Field(..., ge=1990, le=2050, description="Año de fabricación")
    tn: float = Field(..., ge=0, description="Capacidad de carga en toneladas")
    m3: float = Field(..., ge=0, description="Capacidad volumétrica en metros cúbicos")
    tipo_vehiculo: str = Field(..., description="Volquete, Furgón, Plataforma, Tanque, Cisterna, etc.")
    tipo_combustible: Optional[str] = Field(default="Diesel", description="Diesel, Gasolina, GNV, Eléctrico")
    
    # Datos del conductor
    nombre_conductor: Optional[str] = Field(None, min_length=2, max_length=100, description="Nombre completo del conductor")
    numero_licencia: Optional[str] = Field(None, min_length=5, max_length=20, description="Número de licencia de conducir")
    
    revision_tecnica_emision: Optional[date] = Field(None, description="Fecha de emisión de la revisión técnica")
    revision_tecnica_vencimiento: Optional[date] = Field(None, description="Fecha de vencimiento de la revisión técnica")
    soat_vigencia_inicio: Optional[date] = Field(None, description="Inicio de vigencia del SOAT")
    soat_vigencia_fin: Optional[date] = Field(None, description="Fin de vigencia del SOAT")
    mtc_numero: Optional[str] = Field(None, description="Número de autorización o registro MTC")
    extintor_vencimiento: Optional[date] = Field(None, description="Fecha de vencimiento del extintor")
    cantidad_parihuelas: int = Field(0, ge=0, description="Cantidad de parihuelas transportadas")
    dias_alerta_revision_tecnica: int = Field(default=30, ge=1, le=365, description="Días de anticipación para alertar vencimiento de revisión técnica")
    dias_alerta_soat: int = Field(default=30, ge=1, le=365, description="Días de anticipación para alertar vencimiento del SOAT")
    dias_alerta_extintor: int = Field(default=15, ge=1, le=365, description="Días de anticipación para alertar vencimiento del extintor")
    observaciones: Optional[str] = Field(None, description="Observaciones adicionales")
    activo: bool = Field(default=True, description="Indica si el vehículo está activo")
 
# Schema para creación (sin id que se genera automáticamente)
class FlotaCreate(BaseModel):
    codigo_flota: Optional[str] = Field(None, min_length=1, max_length=20, description="Código único de la flota")
    placa: str = Field(..., min_length=6, max_length=10, description="Placa del vehículo")
    marca: str = Field(..., min_length=2, max_length=50, description="Marca del vehículo")
    modelo: str = Field(..., min_length=1, max_length=50, description="Modelo del vehículo")
    anio: int = Field(..., ge=1990, le=2050, description="Año de fabricación")
    tn: float = Field(..., ge=0, description="Capacidad de carga en toneladas")
    m3: float = Field(..., ge=0, description="Capacidad volumétrica en metros cúbicos")
    tipo_vehiculo: str = Field(..., description="Volquete, Furgón, Plataforma, Tanque, Cisterna, etc.")
    tipo_combustible: Optional[str] = Field(default="Diesel", description="Diesel, Gasolina, GNV, Eléctrico")
    
    # Datos del conductor
    nombre_conductor: Optional[str] = Field(None, min_length=2, max_length=100, description="Nombre completo del conductor")
    numero_licencia: Optional[str] = Field(None, min_length=5, max_length=20, description="Número de licencia de conducir")
    
    revision_tecnica_emision: Optional[date] = None
    revision_tecnica_vencimiento: Optional[date] = None
    soat_vigencia_inicio: Optional[date] = None
    soat_vigencia_fin: Optional[date] = None
    mtc_numero: Optional[str] = None
    extintor_vencimiento: Optional[date] = None
    cantidad_parihuelas: int = Field(default=0, ge=0)
    dias_alerta_revision_tecnica: int = Field(default=30, ge=1, le=365)
    dias_alerta_soat: int = Field(default=30, ge=1, le=365)
    dias_alerta_extintor: int = Field(default=15, ge=1, le=365)
    observaciones: Optional[str] = None
    activo: bool = Field(default=True)

# Schema para actualización (todo opcional)
class FlotaUpdate(BaseModel):
    placa: Optional[str] = Field(None, min_length=6, max_length=10)
    marca: Optional[str] = Field(None, min_length=2, max_length=50)
    modelo: Optional[str] = Field(None, min_length=1, max_length=50)
    anio: Optional[int] = Field(None, ge=1990, le=2050)
    tn: Optional[float] = Field(None, ge=0)
    m3: Optional[float] = Field(None, ge=0)
    tipo_vehiculo: Optional[str] = None
    tipo_combustible: Optional[str] = None
    
    # Datos del conductor
    nombre_conductor: Optional[str] = Field(None, min_length=2, max_length=100)
    numero_licencia: Optional[str] = Field(None, min_length=5, max_length=20)
    
    revision_tecnica_emision: Optional[date] = None
    revision_tecnica_vencimiento: Optional[date] = None
    soat_vigencia_inicio: Optional[date] = None
    soat_vigencia_fin: Optional[date] = None
    mtc_numero: Optional[str] = None
    extintor_vencimiento: Optional[date] = None
    cantidad_parihuelas: Optional[int] = Field(None, ge=0)
    dias_alerta_revision_tecnica: Optional[int] = Field(None, ge=1, le=365)
    dias_alerta_soat: Optional[int] = Field(None, ge=1, le=365)
    dias_alerta_extintor: Optional[int] = Field(None, ge=1, le=365)
    observaciones: Optional[str] = None
    activo: Optional[bool] = None

# Schema para respuesta (incluye id y fecha_registro)
class FlotaResponse(FlotaBase):
    id: str
    fecha_registro: datetime

    class Config:
        from_attributes = True

# Schema para filtros de búsqueda
class FlotaFilter(BaseModel):
    codigo_flota: Optional[str] = None
    placa: Optional[str] = None
    marca: Optional[str] = None
    modelo: Optional[str] = None
    anio: Optional[int] = None
    tipo_vehiculo: Optional[str] = None
    tipo_combustible: Optional[str] = None
    nombre_conductor: Optional[str] = None
    numero_licencia: Optional[str] = None
    mtc_numero: Optional[str] = None
    activo: Optional[bool] = None

# Schema para alertas
class AlertaResponse(BaseModel):
    revision_tecnica: bool
    soat: bool
    extintor: bool
    mensajes: List[str]

# Schema para importación masiva
class FlotaImport(BaseModel):
    vehiculos: List[FlotaCreate]

# Schema para respuesta de importación
class ExcelImportResponse(BaseModel):
    message: str
    result: dict
    
    class Config:
        json_schema_extra = {
            "example": {
                "message": "Importación exitosa. 25 vehículos creados, 5 actualizados",
                "result": {
                    "total_rows": 32,
                    "created": 25,
                    "updated": 5,
                    "skipped": 2,
                    "errors": [
                        "Fila 10: Vehículo duplicado - Placa ABC-123 ya existe"
                    ],
                    "has_errors": True,
                    "success_rate": "93.8%"
                }
            }
        }

# Schema para parámetros de paginación
class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1, description="Número de página")
    page_size: int = Field(default=10, ge=1, le=100, description="Elementos por página")

# Schema genérico para respuesta paginada
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