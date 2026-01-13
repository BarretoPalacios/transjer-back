from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, EmailStr, Field
from datetime import datetime

# Modelos auxiliares para estructuras anidadas
class ContactoProveedor(BaseModel):
    tipo: str  # comercial, contable, operaciones, soporte, gerencia
    nombre: str
    telefono: Optional[str] = None
    email: Optional[EmailStr] = None

class CuentaPagoProveedor(BaseModel):
    nombre_cuenta: str
    tipo_pago: str  # Contado, Crédito, Leasing, Factoring
    dias_credito: int = 0
    limite_credito: float = 0.0
    estado: str = "activa"  # activa, suspendida
    es_principal: bool = False

# Schema base
class ProveedorBase(BaseModel):
    tipo_documento: str
    numero_documento: str
    razon_social: str
    estado: str
    codigo_proveedor: Optional[str] = None
    rubro_proveedor: Optional[str] = None  # transportista, logistica, seguridad, mantenimiento, tecnologia, seguros, servicios, otros
    servicios: Optional[List[str]] = None  # Lista simple de servicios
    contacto_principal: Optional[str] = None
    telefono: Optional[str] = None
    contactos: Optional[List[ContactoProveedor]] = None
    cuentas_pago: Optional[List[CuentaPagoProveedor]] = None
    email: Optional[EmailStr] = None
    direccion: Optional[str] = None
    website: Optional[str] = None
    observaciones: Optional[str] = None

# Schema para creación (sin codigo_proveedor que se genera automáticamente)
class ProveedorCreate(BaseModel):
    tipo_documento: str
    numero_documento: str
    razon_social: str
    estado: str = "activo"
    rubro_proveedor: Optional[str] = None
    servicios: Optional[List[str]] = None
    contacto_principal: Optional[str] = None
    telefono: Optional[str] = None
    contactos: Optional[List[ContactoProveedor]] = None
    cuentas_pago: Optional[List[CuentaPagoProveedor]] = None
    email: Optional[EmailStr] = None
    direccion: Optional[str] = None
    website: Optional[str] = None
    observaciones: Optional[str] = None

# Schema para actualización (todo opcional excepto lo que se quiera cambiar)
class ProveedorUpdate(BaseModel):
    tipo_documento: Optional[str] = None
    numero_documento: Optional[str] = None
    razon_social: Optional[str] = None
    estado: Optional[str] = None
    rubro_proveedor: Optional[str] = None
    servicios: Optional[List[str]] = None
    contacto_principal: Optional[str] = None
    telefono: Optional[str] = None
    contactos: Optional[List[ContactoProveedor]] = None
    cuentas_pago: Optional[List[CuentaPagoProveedor]] = None
    email: Optional[EmailStr] = None
    direccion: Optional[str] = None
    website: Optional[str] = None
    observaciones: Optional[str] = None

# Schema para respuesta (incluye id y fecha_registro)
class ProveedorResponse(ProveedorBase):
    id: str
    fecha_registro: datetime

    class Config:
        from_attributes = True

# Schema para filtros de búsqueda
class ProveedorFilter(BaseModel):
    codigo_proveedor: Optional[str] = None
    tipo_documento: Optional[str] = None
    numero_documento: Optional[str] = None
    razon_social: Optional[str] = None
    rubro_proveedor: Optional[str] = None
    contacto_principal: Optional[str] = None
    telefono: Optional[str] = None
    estado: Optional[str] = None
    servicio: Optional[str] = None  # Para buscar por un servicio específico

# Schema para importación masiva
class ProveedorImport(BaseModel):
    proveedores: List[ProveedorCreate]

# Schema para respuesta de importación
class ExcelImportResponseProveedor(BaseModel):
    message: str
    result: dict
    
    class Config:
        json_schema_extra = {
            "example": {
                "message": "Importación exitosa. 30 proveedores creados, 2 actualizados",
                "result": {
                    "total_rows": 35,
                    "created": 30,
                    "updated": 2,
                    "skipped": 3,
                    "errors": [
                        "Fila 10: Proveedor duplicado - RUC 20987654321 ya existe"
                    ],
                    "has_errors": True,
                    "success_rate": "91.4%"
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