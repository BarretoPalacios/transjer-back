from typing import Optional, List
from pydantic import BaseModel

class LugarBase(BaseModel):
    codigo_lugar: Optional[str] = None
    nombre: str
    tipo_lugar: str
    direccion: str
    distrito: str
    provincia: str
    departamento: str
    coordenadas: Optional[dict] = None
    contacto: Optional[str] = None
    telefono: Optional[str] = None
    horario_atencion: Optional[str] = None
    capacidad_estacionamiento: Optional[int] = None
    servicios_disponibles: List[str] = []
    estado: str = "activo"
    es_principal: bool = False
    observaciones: Optional[str] = None

class LugarCreate(BaseModel):
    nombre: str
    tipo_lugar: str
    direccion: str
    distrito: str
    provincia: str
    departamento: str
    coordenadas: Optional[dict] = None
    contacto: Optional[str] = None
    telefono: Optional[str] = None
    horario_atencion: Optional[str] = None
    capacidad_estacionamiento: Optional[int] = None
    servicios_disponibles: List[str] = []
    estado: str = "activo"
    es_principal: bool = False
    observaciones: Optional[str] = None

class LugarUpdate(BaseModel):
    nombre: Optional[str] = None
    tipo_lugar: Optional[str] = None
    distrito: Optional[str] = None
    provincia: Optional[str] = None
    departamento: Optional[str] = None
    direccion: Optional[str] = None
    contacto: Optional[str] = None
    telefono: Optional[str] = None
    horario_atencion: Optional[str] = None
    coordenadas: Optional[dict] = None
    horario_atencion: Optional[str] = None
    capacidad_estacionamiento: Optional[int] = None
    servicios_disponibles: Optional[List[str]] = None
    estado: Optional[str] = None
    es_principal: Optional[bool] = None
    observaciones: Optional[str] = None

class LugarResponse(LugarBase):
    id: str

    class Config:
        from_attributes = True

class LugarFilter(BaseModel):
    codigo_lugar: Optional[str] = None
    nombre: Optional[str] = None
    tipo_lugar: Optional[str] = None
    distrito: Optional[str] = None
    provincia: Optional[str] = None
    estado: Optional[str] = None
    es_principal: Optional[bool] = None


class ExcelImportResponse(BaseModel):
    message: str
    result: dict