# app/models/servicio.py - Modelos Pydantic para servicios

from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime
from bson import ObjectId


class PyObjectId(ObjectId):
    """ObjectId personalizado para Pydantic"""
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):
        field_schema.update(type="string")


class CambioHistorial(BaseModel):
    """Historial de cambios en un servicio"""
    campo: str
    valor_anterior: Optional[str] = None
    valor_nuevo: Optional[str] = None
    fecha: datetime = Field(default_factory=datetime.utcnow)
    usuario: Optional[str] = None


class Factura(BaseModel):
    """Información de facturación del servicio"""
    numero: Optional[str] = None
    fecha_emision: Optional[datetime] = None
    estado: Literal[
        "PENDIENTE",
        "EMITIDA", 
        "FACTURADO",
        "POR_COBRAR",
        "OBSERVADO",
        "ANULADO"
    ] = "PENDIENTE"
    monto: Optional[float] = None
    moneda: Optional[str] = "PEN"


class Metadata(BaseModel):
    """Metadata de carga y procesamiento"""
    archivo_origen: Optional[str] = None
    fecha_carga: datetime = Field(default_factory=datetime.utcnow)
    usuario_carga: Optional[str] = None
    fila_original: Optional[int] = None
    observaciones: Optional[str] = None


class Servicio(BaseModel):
    """Modelo principal de servicio de transporte"""
    
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    
    # Información básica
    cuenta: Optional[str] = None
    cliente: Optional[str] = None
    m3_tn: Optional[float] = None
    aux: Optional[int] = None
    zona: Optional[str] = None
    mes: Optional[str] = None
    proveedor: Optional[str] = None
    
    # Solicitud y tipo
    solicitud: Optional[str] = None
    tipo_servicio: Optional[str] = None
    
    # Fechas y horarios
    fecha_servicio: Optional[datetime] = None
    fecha_salida: Optional[datetime] = None
    hora_cita: Optional[str] = None
    
    # Transporte
    placa: Optional[str] = None
    tipo_camion: Optional[str] = None
    capacidad_m3: Optional[float] = None
    capacidad_tn: Optional[float] = None
    
    # Campo SERVICIO (nuevo)
    servicio: Optional[str] = None
    
    # Personal
    conductor: Optional[str] = None
    auxiliar: Optional[str] = None
    
    # Ubicaciones
    origen: Optional[str] = None
    destino: Optional[str] = None
    
    # CLIENTE (nuevo - según el ejemplo parece ser un segundo campo cliente)
    cliente_destino: Optional[str] = None
    
    # Campo GRTE (nuevo)
    grte: Optional[str] = None
    
    # Factura (CRÍTICO)
    factura: Factura = Field(default_factory=Factura)
    
    # Estado del servicio
    estado_servicio: Literal[
        "PROGRAMADO", 
        "EN_CURSO", 
        "COMPLETADO",
        "CANCELADO",
        "PENDIENTE_FACTURACION"
    ] = "PROGRAMADO"
    
    # Metadata
    metadata: Metadata = Field(default_factory=Metadata)
    
    # Historial
    historial_cambios: List[CambioHistorial] = []
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        json_schema_extra = {
            "example": {
                "cuenta": "SONEPAR",
                "cliente": "SONEPAR",
                "m3_tn": 10.0,
                "aux": 1,
                "mes": "Agosto",
                "proveedor": "Transporte Transjer",
                "solicitud": "Dia",
                "tipo_servicio": "FLETE",
                "fecha_servicio": "2025-08-07T00:00:00",
                "fecha_salida": "2025-08-07T00:00:00",
                "hora_cita": "6:00 AM",
                "placa": "BZH-921",
                "tipo_camion": "FURGON",
                "capacidad_m3": 10.0,
                "capacidad_tn": 2.0,
                "servicio": "LIMA",
                "conductor": "JOHN",
                "auxiliar": "BARAHONA",
                "origen": "CD",
                "destino": "HUACHO",
                "cliente_destino": "",
                "grte": "VT01-2824",
                "factura": {
                    "numero": "F001-468",
                    "fecha_emision": "2025-08-14T00:00:00",
                    "estado": "FACTURADO"
                }
            }
        }


# ========== SCHEMAS PARA RESPUESTAS ==========

class ErrorDetalle(BaseModel):
    """Detalle de un error o advertencia durante la carga"""
    fila: int
    campo: Optional[str] = None
    mensaje: str
    tipo: Literal["ERROR", "ADVERTENCIA"] = "ERROR"


class ResultadoCarga(BaseModel):
    """Resultado de la carga de Excel"""
    total_registros: int
    insertados: int
    errores: int
    advertencias: int
    tiempo_procesamiento: float
    detalles_errores: List[ErrorDetalle] = []
    detalles_advertencias: List[ErrorDetalle] = []


class ProgresoResponse(BaseModel):
    """Respuesta de progreso en tiempo real"""
    progreso: float  # 0-100
    mensaje: str
    registros_procesados: int
    total_registros: int


class ActualizarEstadoRequest(BaseModel):
    """Request para actualizar estado de factura"""
    nuevo_estado: Literal[
        "PENDIENTE",
        "EMITIDA", 
        "FACTURADO",
        "POR_COBRAR",
        "OBSERVADO",
        "ANULADO"
    ]
    usuario: Optional[str] = "sistema"


class FiltrosServiciosRequest(BaseModel):
    """Filtros para búsqueda de servicios"""
    cliente: Optional[str] = None
    estado_factura: Optional[str] = None
    estado_servicio: Optional[str] = None
    fecha_desde: Optional[str] = None
    fecha_hasta: Optional[str] = None
    skip: int = 0
    limit: int = 100