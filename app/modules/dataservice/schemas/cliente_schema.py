from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, EmailStr, Field
from datetime import datetime

# Modelos auxiliares para estructuras anidadas
class Contacto(BaseModel):
    tipo: str  
    nombre: str
    telefono: str

class CuentaFacturacion(BaseModel):
    nombre_cuenta: str
    direccion_origen: Optional[str] = None  
    tipo_pago: str  # Contado, Crédito, Crédito Factoring
    dias_credito: int = 0
    limite_credito: float = 0.0
    estado: str = "activa"  # activa, suspendida
    es_principal: bool = False

# Schema base
class ClienteBase(BaseModel):
    tipo_documento: str
    numero_documento: str
    razon_social: str
    estado: str
    codigo_cliente: Optional[str] = None
    tipo_cliente: Optional[str] = None  # rubro comercial
    periodo_facturacion: Optional[str] = None
    periodo_facturacion_dias: Optional[int] = None
    tipo_pago: Optional[str] = None
    dias_credito: Optional[int] = None
    contacto_principal: Optional[str] = None
    telefono: Optional[str] = None
    contactos: Optional[List[Contacto]] = None
    cuentas_facturacion: Optional[List[CuentaFacturacion]] = None
    email: Optional[EmailStr] = None
    direccion: Optional[str] = None
    website: Optional[str] = None
    observaciones: Optional[str] = None

# Schema para creación (sin codigo_cliente que se genera automáticamente)
class ClienteCreate(BaseModel):
    tipo_documento: str
    numero_documento: str
    razon_social: str
    estado: str = "activo"
    tipo_cliente: Optional[str] = None
    periodo_facturacion: Optional[str] = None
    periodo_facturacion_dias: Optional[int] = None
    tipo_pago: Optional[str] = None
    dias_credito: Optional[int] = None
    contacto_principal: Optional[str] = None
    telefono: Optional[str] = None
    contactos: Optional[List[Contacto]] = None
    cuentas_facturacion: Optional[List[CuentaFacturacion]] = None
    email: Optional[EmailStr] = None
    direccion: Optional[str] = None
    website: Optional[str] = None
    observaciones: Optional[str] = None

# Schema para actualización (todo opcional excepto lo que se quiera cambiar)
class ClienteUpdate(BaseModel):
    tipo_documento: Optional[str] = None
    numero_documento: Optional[str] = None
    razon_social: Optional[str] = None
    estado: Optional[str] = None
    tipo_cliente: Optional[str] = None
    periodo_facturacion: Optional[str] = None
    periodo_facturacion_dias: Optional[int] = None
    tipo_pago: Optional[str] = None
    dias_credito: Optional[int] = None
    contacto_principal: Optional[str] = None
    telefono: Optional[str] = None
    contactos: Optional[List[Contacto]] = None
    cuentas_facturacion: Optional[List[CuentaFacturacion]] = None
    email: Optional[EmailStr] = None
    direccion: Optional[str] = None
    website: Optional[str] = None
    observaciones: Optional[str] = None

# Schema para respuesta (incluye id y fecha_registro)
class ClienteResponse(ClienteBase):
    id: str
    fecha_registro: datetime

    class Config:
        from_attributes = True

# Schema para filtros de búsqueda
class ClienteFilter(BaseModel):
    codigo_cliente: Optional[str] = None
    tipo_documento: Optional[str] = None
    numero_documento: Optional[str] = None
    razon_social: Optional[str] = None
    tipo_cliente: Optional[str] = None
    tipo_pago: Optional[str] = None
    dias_credito: Optional[int] = None
    contacto_principal: Optional[str] = None
    telefono: Optional[str] = None
    estado: Optional[str] = None
    periodo_facturacion: Optional[str] = None

# Schema para importación masiva
class ClienteImport(BaseModel):
    clientes: List[ClienteCreate]

# Schema para respuesta de importación
class ExcelImportResponse(BaseModel):
    message: str
    result: dict
    
    class Config: 
        json_schema_extra = {
            "example": {
                "message": "Importación exitosa. 45 clientes creados, 3 actualizados",
                "result": {
                    "total_rows": 50,
                    "created": 45,
                    "updated": 3,
                    "skipped": 2,
                    "errors": [
                        "Fila 15: Cliente duplicado - RUC 20123456789 ya existe"
                    ],
                    "has_errors": True,
                    "success_rate": "96.0%"
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