from typing import Optional
from pydantic import BaseModel, Field
from bson import ObjectId

class Lugar(BaseModel):
    codigo_lugar: Optional[str] = Field(..., min_length=3, max_length=20)
    nombre: str = Field(..., min_length=3, max_length=100)
    tipo_lugar: str = Field(..., description="origen, destino, almacen, taller, oficina")
    direccion: str
    distrito: str
    provincia: str 
    departamento: str
    coordenadas: Optional[dict] = None
    contacto: Optional[str] = None
    telefono: Optional[str] = None
    horario_atencion: Optional[str] = None
    capacidad_estacionamiento: Optional[int] = None
    servicios_disponibles: list[str] = Field(default_factory=list)
    estado: str = Field(default="activo", description="activo, inactivo")
    es_principal: bool = Field(default=False)
    observaciones: Optional[str] = None

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "codigo_lugar": "LUG-001",
                "nombre": "Planta de Arena Lima Norte",
                "tipo_lugar": "origen",
                "direccion": "Km 22.5 Panamericana Norte",
                "distrito": "Carabayllo",
                "provincia": "Lima",
                "departamento": "Lima",
                "coordenadas": {"lat": -11.8500, "lng": -77.0500},
                "contacto": "Sr. Rodríguez",
                "telefono": "987654323",
                "horario_atencion": "06:00 - 18:00",
                "capacidad_estacionamiento": 10,
                "servicios_disponibles": ["Carga", "Almacenamiento"],
                "estado": "activo",
                "es_principal": True,
                "observaciones": "Lugar principal para carga de materiales"
            }
        }