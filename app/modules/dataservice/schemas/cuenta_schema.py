from typing import Optional, List
from pydantic import BaseModel, EmailStr
from datetime import datetime

class CuentaBase(BaseModel):
    codigo_cuenta: Optional[str] = None
    codigo_cliente: str
    nombre: str
    ruc: str
    direccion: str
    telefono: str
    email: Optional[EmailStr] = None
    contacto: Optional[str] = None
    tipo_cliente: str = "Regular"
    limite_credito: float = 0.0
    estado: str = "activo"
    notas: Optional[str] = None

class CuentaCreate(BaseModel):
    codigo_cliente: str
    nombre: str
    ruc: str
    direccion: str
    telefono: str
    email: Optional[EmailStr] = None
    contacto: Optional[str] = None
    tipo_cliente: str = "Regular"
    limite_credito: float = 0.0
    estado: str = "activo"
    notas: Optional[str] = None

class CuentaUpdate(BaseModel):
    codigo_cliente: Optional[str] = None
    nombre: Optional[str] = None
    ruc: Optional[str] = None
    direccion: Optional[str] = None
    telefono: Optional[str] = None
    email: Optional[EmailStr] = None
    contacto: Optional[str] = None
    tipo_cliente: Optional[str] = None
    limite_credito: Optional[float] = None
    estado: Optional[str] = None
    notas: Optional[str] = None

class CuentaResponse(CuentaBase):
    id: str
    fecha_registro: datetime

    class Config:
        from_attributes = True

class CuentaFilter(BaseModel):
    codigo_cuenta: Optional[str] = None
    codigo_cliente: Optional[str] = None
    nombre: Optional[str] = None
    ruc: Optional[str] = None
    contacto: Optional[str] = None
    tipo_cliente: Optional[str] = None
    estado: Optional[str] = None

class CuentaImport(BaseModel):
    cuentas: List[CuentaCreate]

class ExcelImportResponse(BaseModel):
    message: str
    result: dict