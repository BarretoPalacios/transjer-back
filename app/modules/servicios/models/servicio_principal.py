from datetime import datetime, date, time
from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel, Field, field_validator, field_serializer, model_validator

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
    justificacion: str = Field(..., min_length=10, description="Justificación del cambio (mínimo 10 caracteres)")
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

class ServicioPrincipal(BaseModel):
    codigo_servicio_principal: Optional[str] = Field(..., min_length=3, max_length=20, description="Código único del servicio principal")
    
    cuenta: Dict[str, Any] = Field(..., description="Información de la cuenta (dict)")
    cliente: Dict[str, Any] = Field(..., description="Información del cliente (dict)")
    proveedor: Dict[str, Any] = Field(..., description="Información del proveedor (dict)")
    flota: Optional[Dict[str, Any]] = Field(None, description="Información de la flota (dict)")
    conductor: Optional[List[Dict[str, Any]]] = Field(None, description="Lista opcional de conductores (list of dict)")
    auxiliar: Optional[List[Dict[str, Any]]] = Field(None, description="Lista opcional de auxiliares (list of dict)")
    
    m3: Optional[str] = Field(None, max_length=50, description="Medida en M3")
    tn: Optional[str] = Field(None, max_length=50, description="Medida en TN")
    
    mes: str = Field(..., max_length=50, description="Mes del servicio")
    solicitud: Optional[TurnoServicio] = Field(None, description="Turno de solicitud: Dia, Tarde o Noche")
    tipo_servicio: str = Field(..., max_length=100, description="Tipo de servicio")
    modalidad_servicio: str = Field(..., max_length=100, description="Modalidad de servicio")
    zona: str = Field(..., max_length=50, description="Zona del servicio: Lima, Provincia o Extranjero")
    
    fecha_servicio: date = Field(..., description="Fecha del servicio")
    fecha_salida: Optional[date] = Field(None, description="Fecha de salida")
    hora_cita: Optional[time] = Field(None, description="Hora de cita")
    
    gia_rr: Optional[str] = Field(None, max_length=100, description="Guía de Remisión Remitente")
    gia_rt: Optional[str] = Field(None, max_length=100, description="Guía de Remisión Transportista")
    
    descripcion: Optional[str] = Field(None, max_length=200, description="Descripción del servicio")
    
    origen: str = Field(..., max_length=300, description="Dirección de origen")
    destino: str = Field(..., max_length=300, description="Dirección de destino")
    cliente_destino: Optional[str] = Field(None, max_length=200, description="Cliente en destino")
    
    responsable: Optional[str] = Field(None, max_length=100, description="Gerente o responsable")
    
    estado: EstadoServicio = Field(default=EstadoServicio.PROGRAMADO, description="Estado del servicio")
    
    historial_estados: List[HistorialCambioEstado] = Field(
        default_factory=list,
        description="Historial de cambios de estado con justificaciones"
    )
    
    es_editable: bool = Field(
        default=True,
        description="Indica si el servicio puede ser editado"
    )
    es_eliminable: bool = Field(
        default=True,
        description="Indica si el servicio puede ser eliminado"
    )
    servicio_cerrado: bool = Field(
        default=False,
        description="Indica si el servicio está en un período cerrado (para cierre contable)"
    )
    fecha_cierre: Optional[datetime] = Field(
        None,
        description="Fecha en que se cerró el servicio (cierre contable)"
    )
    
    pertenece_a_factura: bool = Field(
        default=False,
        description="Indica si el servicio pertenece a una factura"
    )
    
    fecha_registro: datetime = Field(
        default_factory=datetime.now,
        description="Fecha de registro"
    )
    fecha_ultima_modificacion: Optional[datetime] = Field(
        None,
        description="Fecha de última modificación" 
    )
    fecha_completado: Optional[datetime] = Field(
        None,
        description="Fecha en que se completó el servicio"
    )
    
    @model_validator(mode='after')
    def actualizar_permisos_segun_estado(self):
        if self.servicio_cerrado:
            self.es_editable = False
            self.es_eliminable = False
            if not self.fecha_cierre:
                self.fecha_cierre = datetime.now()
        elif self.estado == EstadoServicio.COMPLETADO:
            self.es_editable = False
            self.es_eliminable = False
            if not self.fecha_completado:
                self.fecha_completado = datetime.now()
        elif self.estado in [EstadoServicio.CANCELADO, EstadoServicio.REPROGRAMADO]:
            self.es_editable = False
            self.es_eliminable = False
        else:
            self.es_editable = True
            self.es_eliminable = False
            
        return self

    @field_serializer('fecha_servicio', 'fecha_salida')
    def serialize_date(self, value: Optional[date], _info):
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, datetime.min.time())
        return value

    @field_serializer('hora_cita')
    def serialize_time(self, value: Optional[time], _info):
        if value is None:
            return None
        if isinstance(value, time):
            return datetime.combine(date.today(), value)
        return value
    
    def cambiar_estado(
        self, 
        nuevo_estado: EstadoServicio, 
        justificacion: str, 
        usuario: Optional[str] = None
    ) -> bool:
        if self.servicio_cerrado:
            raise ValueError(
                "No se puede cambiar el estado de un servicio cerrado"
            )
        
        if self.estado == EstadoServicio.COMPLETADO:
            raise ValueError("No se puede cambiar el estado de un servicio completado")
        
        if self.estado == nuevo_estado:
            raise ValueError(f"El servicio ya está en estado {nuevo_estado}")
        
        if nuevo_estado in [EstadoServicio.CANCELADO, EstadoServicio.REPROGRAMADO]:
            if not justificacion or len(justificacion.strip()) < 10:
                raise ValueError(
                    f"Se requiere una justificación de al menos 10 caracteres para cambiar a estado {nuevo_estado}"
                )
        
        cambio = HistorialCambioEstado(
            estado_anterior=self.estado,
            estado_nuevo=nuevo_estado,
            justificacion=justificacion,
            usuario=usuario
        )
        self.historial_estados.append(cambio)
        
        estado_anterior = self.estado
        self.estado = nuevo_estado
        self.fecha_ultima_modificacion = datetime.now()
        
        if nuevo_estado == EstadoServicio.COMPLETADO:
            self.es_editable = True
            self.es_eliminable = False
            self.fecha_completado = datetime.now()
        elif nuevo_estado == EstadoServicio.CANCELADO:
            self.es_editable = False
            self.es_eliminable = False
        elif nuevo_estado == EstadoServicio.REPROGRAMADO:
            self.es_editable = True
            self.es_eliminable = False
        
        return True
    
    def puede_editar(self) -> tuple[bool, str]:
        if self.servicio_cerrado:
            return False, "No se puede editar un servicio cerrado"
        if not self.es_editable:
            return False, f"No se puede editar un servicio en estado {self.estado}"
        return True, "El servicio puede ser editado"
    
    def puede_eliminar(self) -> tuple[bool, str]:
        if self.servicio_cerrado:
            return False, "No se puede eliminar un servicio cerrado"
        if not self.es_eliminable:
            return False, f"No se puede eliminar un servicio en estado {self.estado}"
        return True, "El servicio puede ser eliminado"
    
    def cerrar_servicio(self, usuario: Optional[str] = None) -> bool:
        if self.servicio_cerrado:
            raise ValueError("El servicio ya está cerrado")
        
        self.servicio_cerrado = True
        self.fecha_cierre = datetime.now()
        self.es_editable = False
        self.es_eliminable = False
        self.fecha_ultima_modificacion = datetime.now()
        
        return True

    class Config:
        populate_by_name = True
        use_enum_values = True
        json_schema_extra = {
            "example": {
                "codigo_servicio_principal": "SRV-2024-001",
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
                "responsable": "Gerente de Operaciones",
                "estado": "Programado",
                "pertenece_a_factura": False
            }
        }