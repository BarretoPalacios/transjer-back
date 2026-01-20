from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime, date, time
from enum import Enum


class EstadoServicio(str, Enum):
    PROGRAMADO = "Programado"
    COMPLETADO = "Completado"
    CANCELADO = "Cancelado"
    REPROGRAMADO = "Reprogramado"


class TurnoServicio(str, Enum):
    DIA = "Dia"
    TARDE = "Tarde"
    NOCHE = "Noche"


class HistorialCambioEstado(BaseModel):
    estado_anterior: str
    estado_nuevo: str
    justificacion: str = Field(..., min_length=10, description="Justificación del cambio")
    usuario: Optional[str] = Field(None, description="Usuario que realizó el cambio")
    fecha_cambio: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_schema_extra = {
            "example": {
                "estado_anterior": "Programado",
                "estado_nuevo": "Cancelado",
                "justificacion": "Cliente canceló el servicio por problemas internos",
                "usuario": "admin@empresa.com",
                "fecha_cambio": "2024-01-15T10:30:00"
            }
        }


class ServicioPrincipalBase(BaseModel):
    cuenta: Dict[str, Any]
    cliente: Dict[str, Any]
    proveedor: Dict[str, Any]
    flota: Optional[Dict[str, Any]] = None
    conductor: Optional[List[Dict[str, Any]]] = None
    auxiliar: Optional[List[Dict[str, Any]]] = None

    m3: Optional[str] = None
    tn: Optional[str] = None
    mes: str
    solicitud: Optional[TurnoServicio] = None
    tipo_servicio: str
    modalidad_servicio: str

    zona: str

    fecha_servicio: date
    fecha_salida: Optional[date] = None
    hora_cita: Optional[time] = None

    gia_rr: Optional[str] = None
    gia_rt: Optional[str] = None

    descripcion: Optional[str] = None

    origen: str
    destino: str
    cliente_destino: Optional[str] = None

    responsable: Optional[str] = None


class ServicioPrincipalCreate(ServicioPrincipalBase):
    estado: Optional[EstadoServicio] = Field(
        default=EstadoServicio.PROGRAMADO,
        description="Estado inicial del servicio"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "cuenta": {"numero": "CT001", "tipo": "Corriente"},
                "cliente": {"nombre": "Cliente S.A.", "ruc": "12345678901"},
                "proveedor": {"nombre": "Proveedor S.A.", "ruc": "98765432109"},
                "flota": {"placa": "ABC123", "marca": "Volvo"},
                "conductor": [
                    {"nombre": "Juan Pérez", "licencia": "A12345"},
                    {"nombre": "Carlos López", "licencia": "A67890"}
                ],
                "auxiliar": [
                    {"nombre": "Pedro Gómez", "dni": "12345678"},
                    {"nombre": "Luis Torres", "dni": "87654321"}
                ],
                "m3": "25",
                "tn": "18",
                "mes": "Enero",
                "solicitud": "Dia",
                "tipo_servicio": "Transporte de Carga",
                "modalidad_servicio": "Carga Completa",
                "zona": "Provincia",
                "fecha_servicio": "2024-01-15",
                "fecha_salida": "2024-01-15",
                "hora_cita": "08:00:00",
                "gia_rr": "GRR001",
                "gia_rt": "GRT001",
                "descripcion": "Transporte de mercancía general",
                "origen": "Av. Principal 123, Lima",
                "destino": "Av. Industrial 456, Arequipa",
                "cliente_destino": "Consignatario S.A.",
                "responsable": "Gerente de Operaciones"
            }
        }


class ServicioPrincipalUpdate(BaseModel):
    cuenta: Optional[Dict[str, Any]] = None
    cliente: Optional[Dict[str, Any]] = None
    proveedor: Optional[Dict[str, Any]] = None
    flota: Optional[Dict[str, Any]] = None
    conductor: Optional[List[Dict[str, Any]]] = None
    auxiliar: Optional[List[Dict[str, Any]]] = None

    m3: Optional[str] = None
    tn: Optional[str] = None
    mes: Optional[str] = None
    solicitud: Optional[TurnoServicio] = None
    tipo_servicio: Optional[str] = None
    modalidad_servicio: Optional[str] = None
    zona: Optional[str] = None

    fecha_servicio: Optional[date] = None
    fecha_salida: Optional[date] = None
    hora_cita: Optional[time] = None

    gia_rr: Optional[str] = None
    gia_rt: Optional[str] = None

    descripcion: Optional[str] = None

    origen: Optional[str] = None
    destino: Optional[str] = None
    cliente_destino: Optional[str] = None

    responsable: Optional[str] = None


class CambioEstadoRequest(BaseModel):
    nuevo_estado: EstadoServicio = Field(..., description="Nuevo estado del servicio")
    justificacion: str = Field(
        ...,
        min_length=10,
        description="Justificación del cambio (mínimo 10 caracteres)"
    ) 
    usuario: Optional[str] = Field(None, description="Usuario que realiza el cambio")
    
    class Config:
        json_schema_extra = {
            "example": {
                "nuevo_estado": "Cancelado",
                "justificacion": "Cliente solicitó cancelación por cambio de cronograma interno",
                "usuario": "admin@empresa.com"
            }
        }


class CierreServicioRequest(BaseModel):
    usuario: Optional[str] = Field(None, description="Usuario que realiza el cierre")
    
    class Config:
        json_schema_extra = {
            "example": {
                "usuario": "contador@empresa.com"
            }
        }


class ServicioPrincipalResponse(ServicioPrincipalBase):
    id: str
    codigo_servicio_principal: Optional[str] = None
    
    estado: EstadoServicio
    historial_estados: List[HistorialCambioEstado] = Field(default_factory=list)
    
    es_editable: bool = Field(default=True)
    es_eliminable: bool = Field(default=True)
    
    servicio_cerrado: bool = Field(default=False)
    fecha_cierre: Optional[datetime] = None
    
    pertenece_a_factura: bool = Field(default=False)
    
    fecha_registro: datetime
    fecha_ultima_modificacion: Optional[datetime] = None
    fecha_completado: Optional[datetime] = None
    
    periodo: Optional[str] = None

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "507f1f77bcf86cd799439011",
                "codigo_servicio_principal": "SRV-2024-001",
                "cuenta": {"numero": "CT001", "tipo": "Corriente"},
                "cliente": {"nombre": "Cliente S.A.", "ruc": "12345678901"},
                "proveedor": {"nombre": "Proveedor S.A.", "ruc": "98765432109"},
                "flota": {"placa": "ABC123", "marca": "Volvo"},
                "conductor": [
                    {"nombre": "Juan Pérez", "licencia": "A12345"}
                ],
                "auxiliar": [
                    {"nombre": "Pedro Gómez", "dni": "12345678"}
                ],
                "m3": "25",
                "tn": "18",
                "mes": "Enero",
                "solicitud": "Dia",
                "tipo_servicio": "Transporte de Carga",
                "modalidad_servicio": "Carga Completa",
                "zona": "Lima",
                "fecha_servicio": "2024-01-15",
                "origen": "Lima",
                "destino": "Arequipa",
                "estado": "Programado",
                "es_editable": True,
                "es_eliminable": True,
                "servicio_cerrado": False,
                "pertenece_a_factura": False,
                "fecha_registro": "2024-01-15T10:00:00"
            }
        }


class ServicioPrincipalConPermisos(BaseModel):
    servicio: ServicioPrincipalResponse
    permisos: Dict[str, Any] = Field(
        description="Permisos actuales del servicio"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "servicio": {
                    "id": "507f1f77bcf86cd799439011",
                    "estado": "Completado",
                    "es_editable": False,
                    "es_eliminable": False,
                    "pertenece_a_factura": False
                },
                "permisos": {
                    "puede_editar": False,
                    "mensaje_editar": "No se puede editar un servicio en estado Completado",
                    "puede_eliminar": False,
                    "mensaje_eliminar": "No se puede eliminar un servicio en estado Completado",
                    "puede_cambiar_estado": False,
                    "esta_cerrado": False
                }
            }
        }


class ServicioPrincipalFilter(BaseModel):
    codigo_servicio_principal: Optional[str] = None
    mes: Optional[str] = None
    tipo_servicio: Optional[str] = None
    modalidad_servicio: Optional[str] = None
    zona: Optional[str] = None
    solicitud: Optional[TurnoServicio] = None
    
    fecha_servicio: Optional[date] = None
    
    fecha_inicio: Optional[date] = None
    fecha_fin: Optional[date] = None
    
    cliente_nombre: Optional[str] = None
    proveedor_nombre: Optional[str] = None
    flota_placa: Optional[str] = None
    conductor_nombre: Optional[str] = None
    cuenta_nombre: Optional[str] = None
    
    origen: Optional[str] = None
    destino: Optional[str] = None
    responsable: Optional[str] = None
    gia_rr: Optional[str] = None
    gia_rt: Optional[str] = None
    
    periodo: Optional[str] = None

    estado: Optional[EstadoServicio] = None
    
    servicio_cerrado: Optional[bool] = None
    es_editable: Optional[bool] = None
    es_eliminable: Optional[bool] = None
    pertenece_a_factura: Optional[bool] = None
    
    class Config:
        from_attributes = True


class ServicioPrincipalExcelImportResponse(BaseModel):
    message: str
    result: dict
    
    class Config:
        json_schema_extra = {
            "example": {
                "message": "Importación completada exitosamente",
                "result": {
                    "total_procesados": 50,
                    "exitosos": 48,
                    "fallidos": 2,
                    "errores": [
                        {"fila": 15, "error": "Fecha inválida"},
                        {"fila": 32, "error": "Cliente no encontrado"}
                    ]
                }
            }
        }


class HistorialServicioResponse(BaseModel):
    codigo_servicio: str
    estado_actual: EstadoServicio
    historial_estados: List[HistorialCambioEstado]
    total_cambios: int
    servicio_cerrado: bool
    
    class Config:
        json_schema_extra = {
            "example": {
                "codigo_servicio": "SRV-2024-001",
                "estado_actual": "Completado",
                "historial_estados": [
                    {
                        "estado_anterior": "Programado",
                        "estado_nuevo": "Completado",
                        "justificacion": "Servicio finalizado exitosamente",
                        "usuario": "operador@empresa.com",
                        "fecha_cambio": "2024-01-15T18:30:00"
                    }
                ],
                "total_cambios": 1,
                "servicio_cerrado": False
            }
        }