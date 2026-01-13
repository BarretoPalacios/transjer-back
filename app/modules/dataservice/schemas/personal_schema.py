from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, EmailStr, Field
from datetime import datetime, date
import re

# Modelo base con validaciones
class PersonalBase(BaseModel):
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

    @classmethod
    def validate_dni(cls, v):
        # Validación básica de DNI (solo números)
        if not re.match(r'^\d+$', v):
            raise ValueError('El DNI debe contener solo números')
        return v

# Schema para creación
class PersonalCreate(PersonalBase):
    pass

# Schema para actualización (todo opcional)
class PersonalUpdate(BaseModel):
    dni: Optional[str] = Field(None, min_length=8, max_length=15, description="Número de documento de identidad")
    nombres_completos: Optional[str] = Field(None, min_length=2, max_length=200, description="Nombres y apellidos completos del trabajador")
    tipo: Optional[str] = Field(None, description="Tipo de personal: Conductor, Auxiliar, Operario, Administrativo, Supervisor, Mecánico, Almacenero")
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

# Schema para respuesta (incluye id y fecha_registro)
class PersonalResponse(PersonalBase):
    id: str
    fecha_registro: datetime

    class Config:
        from_attributes = True

# Schema para filtros de búsqueda
class PersonalFilter(BaseModel):
    dni: Optional[str] = Field(None, min_length=8, max_length=15, description="Número de documento de identidad")
    nombres_completos: Optional[str] = Field(None, min_length=2, max_length=200, description="Nombres y apellidos completos del trabajador")
    tipo: Optional[str] = Field(None, description="Tipo de personal: Conductor, Auxiliar, Operario, Administrativo, Supervisor, Mecánico, Almacenero")
    estado: Optional[str] = Field(None, description="Estado del trabajador: Activo, Inactivo, Licencia, Vacaciones")
    licencia_conducir: Optional[str] = Field(None, description="Número de licencia de conducir (para conductores)")
    categoria_licencia: Optional[str] = Field(None, description="Categoría de licencia: A-I, A-II-a, A-II-b, A-III-a, A-III-b, A-III-c")
    turno: Optional[str] = Field(None, description="Turno de trabajo: Día, Noche, Rotativo")
    fecha_ingreso_desde: Optional[date] = Field(None, description="Fecha de ingreso desde")
    fecha_ingreso_hasta: Optional[date] = Field(None, description="Fecha de ingreso hasta")
    fecha_nacimiento_desde: Optional[date] = Field(None, description="Fecha de nacimiento desde")
    fecha_nacimiento_hasta: Optional[date] = Field(None, description="Fecha de nacimiento hasta")
    fecha_venc_licencia_desde: Optional[date] = Field(None, description="Fecha de vencimiento de licencia desde")
    fecha_venc_licencia_hasta: Optional[date] = Field(None, description="Fecha de vencimiento de licencia hasta")
    salario_min: Optional[float] = Field(None, ge=0, description="Salario mínimo")
    salario_max: Optional[float] = Field(None, ge=0, description="Salario máximo")
    banco: Optional[str] = Field(None, description="Banco para pago de salario")
    telefono: Optional[str] = Field(None, description="Teléfono de contacto")
    email: Optional[str] = Field(None, description="Email corporativo o personal")
    contacto_emergencia: Optional[str] = Field(None, description="Nombre de contacto de emergencia")

# Schema para importación masiva
class PersonalImport(BaseModel):
    items: List[PersonalCreate]

# Schema para respuesta de importación (igual que cliente)
class ExcelImportResponse(BaseModel):
    message: str
    result: dict
    
    class Config:
        json_schema_extra = {
            "example": {
                "message": "Importación exitosa. 25 registros creados, 5 actualizados",
                "result": {
                    "total_rows": 30,
                    "created": 25,
                    "updated": 5,
                    "skipped": 0,
                    "errors": [],
                    "has_errors": False,
                    "success_rate": "100.0%"
                }
            }
        }

# Schema para parámetros de paginación (igual que cliente)
class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1, description="Número de página")
    page_size: int = Field(default=10, ge=1, le=100, description="Elementos por página")
    sort_by: Optional[str] = Field(default="fecha_registro", description="Campo por el que ordenar")
    sort_order: Optional[str] = Field(default="desc", description="Orden: asc o desc")

# Schema genérico para respuesta paginada (igual que cliente)
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

# Schema específico para respuesta paginada de personal
class PersonalPaginatedResponse(PaginatedResponse[PersonalResponse]):
    pass

# Schema para estadísticas/resumen
class PersonalStatsResponse(BaseModel):
    total_personal: int
    total_activos: int
    total_inactivos: int
    por_tipo: dict[str, int]  # Ej: {"Conductor": 10, "Auxiliar": 5}
    por_estado: dict[str, int]  # Ej: {"Activo": 12, "Licencia": 3}
    por_turno: dict[str, int]  # Ej: {"Día": 8, "Noche": 4, "Rotativo": 3}
    promedio_salario: float
    licencias_por_vencer: int
    personal_reciente: int  # Personal ingresado en los últimos 30 días

# Schema para exportación
class PersonalExportParams(BaseModel):
    format: str = Field(default="excel", description="Formato: excel, csv, pdf")
    include_fields: Optional[List[str]] = Field(None, description="Campos a incluir en la exportación")
    filter: Optional[PersonalFilter] = Field(None, description="Filtros a aplicar")

# Schema para cambio de estado masivo
class BulkStatusUpdate(BaseModel):
    personal_ids: List[str]
    nuevo_estado: str
    motivo: Optional[str] = Field(None, description="Motivo del cambio de estado")
    fecha_efectiva: Optional[date] = Field(None, description="Fecha efectiva del cambio")