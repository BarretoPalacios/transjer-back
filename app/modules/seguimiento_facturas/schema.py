from datetime import date, datetime
from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, Field
from decimal import Decimal
from enum import Enum

# --- ENUMS ---
class EstadoPagoNeto(str, Enum):
    PENDIENTE = "Pendiente"
    PROGRAMADO = "Programado"
    PAGADO_PARCIAL = "Pagado Parcial"
    PAGADO = "Pagado"
    VENCIDO = "Vencido"
    DISPUTA = "En Disputa"
    ANULADO = "Anulado"

class EstadoDetraccion(str, Enum):
    NO_APLICA = "No Aplica"
    PENDIENTE = "Pendiente"
    PAGADO = "Pagado"

class PrioridadPago(str, Enum):
    BAJA = "Baja"
    MEDIA = "Media"
    ALTA = "Alta"
    URGENTE = "Urgente"

# --- SNAPSHOTS ---
class ServicioSnapshot(BaseModel):
    codigo_servicio: Optional[str] = None
    nombre_cliente: Optional[str] = None
    nombre_cuenta: Optional[str] = None
    nombre_proveedor: Optional[str] = None
    placa_flota: Optional[str] = None
    nombre_conductor: Optional[str] = None
    nombre_auxiliar: Optional[str] = None
    m3: Optional[str] = None
    tn: Optional[str] = None
    tipo_servicio: Optional[str] = None
    modalidad: Optional[str] = None
    zona: Optional[str] = None
    fecha_servicio: date
    fecha_salida: date
    gia_rr: Optional[str] = None
    gia_rt: Optional[str] = None
    origen: Optional[str] = None
    destino: Optional[str] = None

class FleteSnapshot(BaseModel):
    codigo_flete: str
    monto_flete: Decimal
    servicio: ServicioSnapshot

class FacturaSnapshot(BaseModel):
    numero_factura: str
    fecha_emision: date
    fecha_vencimiento: date
    monto_total: Decimal
    fletes: List[FleteSnapshot]

# --- SCHEMA BASE ---
class FacturacionGestionBase(BaseModel):
    codigo_factura: str
    datos_completos: Optional[FacturaSnapshot] = None
    
    estado_detraccion: EstadoDetraccion = EstadoDetraccion.PENDIENTE
    tasa_detraccion: Decimal = Decimal("4.0")
    monto_detraccion: Decimal = Decimal("0.0")
    nro_constancia_detraccion: Optional[str] = Field(None, max_length=50)
    fecha_pago_detraccion: Optional[date] = None
    
    estado_pago_neto: EstadoPagoNeto = EstadoPagoNeto.PENDIENTE
    monto_neto: Decimal
    monto_pagado_acumulado: Decimal = Decimal("0.0")
    
    banco_destino: Optional[str] = None
    cuenta_bancaria_destino: Optional[str] = None
    nro_operacion_pago_neto: Optional[str] = None
    
    fecha_probable_pago: Optional[date] = None
    prioridad: PrioridadPago = PrioridadPago.MEDIA
    
    centro_costo: Optional[str] = None
    responsable_gestion: Optional[str] = None
    observaciones_admin: Optional[str] = Field(None, max_length=500)

# --- SCHEMA PARA CREACIÓN ---
class FacturacionGestionCreate(FacturacionGestionBase):
    pass

# --- SCHEMA PARA ACTUALIZACIÓN (Todo Opcional) ---
class FacturacionGestionUpdate(BaseModel):
    datos_completos: Optional[FacturaSnapshot] = None
    estado_detraccion: Optional[EstadoDetraccion] = None
    tasa_detraccion: Optional[Decimal] = None
    monto_detraccion: Optional[Decimal] = None
    nro_constancia_detraccion: Optional[str] = None
    fecha_pago_detraccion: Optional[date] = None
    estado_pago_neto: Optional[EstadoPagoNeto] = None
    monto_neto: Optional[Decimal] = None
    monto_pagado_acumulado: Optional[Decimal] = None
    banco_destino: Optional[str] = None
    cuenta_bancaria_destino: Optional[str] = None
    nro_operacion_pago_neto: Optional[str] = None
    fecha_probable_pago: Optional[date] = None
    prioridad: Optional[PrioridadPago] = None
    centro_costo: Optional[str] = None
    responsable_gestion: Optional[str] = None
    observaciones_admin: Optional[str] = None

# --- SCHEMA PARA RESPUESTA ---
class FacturacionGestionResponse(FacturacionGestionBase):
    id: str
    ultima_actualizacion: datetime
    saldo_pendiente: Decimal

    class Config:
        from_attributes = True
        use_enum_values = True

# --- SCHEMA PARA FILTROS ---
class FacturacionGestionFilter(BaseModel):
    # Filtros básicos
    codigo_factura: Optional[str] = None
    numero_factura: Optional[str] = None
    
    # Estados y prioridad
    estado_detraccion: Optional[EstadoDetraccion] = None
    estado_pago_neto: Optional[EstadoPagoNeto] = None
    prioridad: Optional[PrioridadPago] = None
    
    # Gestión administrativa
    centro_costo: Optional[str] = None
    responsable_gestion: Optional[str] = None
    
    # Filtros de fecha - Probable pago
    fecha_probable_inicio: Optional[date] = None
    fecha_probable_fin: Optional[date] = None
    
    # Filtros de fecha - Emisión factura
    fecha_emision_inicio: Optional[date] = None
    fecha_emision_fin: Optional[date] = None
    
    # Filtros de fecha - Vencimiento factura
    fecha_vencimiento_inicio: Optional[date] = None
    fecha_vencimiento_fin: Optional[date] = None
    
    # Filtros de fecha - Servicio
    fecha_servicio_inicio: Optional[date] = None
    fecha_servicio_fin: Optional[date] = None
    
    # Filtros de fecha - Pago detracción
    fecha_pago_detraccion_inicio: Optional[date] = None
    fecha_pago_detraccion_fin: Optional[date] = None
    
    # Filtros basados en snapshots - Datos principales
    nombre_cliente: Optional[str] = None
    nombre_cuenta: Optional[str] = None
    nombre_proveedor: Optional[str] = None
    
    # Filtros de flota y personal
    placa_flota: Optional[str] = None
    nombre_conductor: Optional[str] = None
    nombre_auxiliar: Optional[str] = None
    
    # Filtros de servicio
    tipo_servicio: Optional[str] = None
    modalidad: Optional[str] = None
    zona: Optional[str] = None
    origen: Optional[str] = None
    destino: Optional[str] = None
    
    # Filtros de montos
    monto_total_min: Optional[Decimal] = None
    monto_total_max: Optional[Decimal] = None
    monto_neto_min: Optional[Decimal] = None
    monto_neto_max: Optional[Decimal] = None
    monto_detraccion_min: Optional[Decimal] = None
    monto_detraccion_max: Optional[Decimal] = None
    
    # Filtros de saldo
    tiene_saldo_pendiente: Optional[bool] = None
    saldo_pendiente_min: Optional[Decimal] = None
    saldo_pendiente_max: Optional[Decimal] = None
    
    # Filtros de GIA
    gia_rr: Optional[str] = None
    gia_rt: Optional[str] = None
    
    # Filtros de búsqueda flexible
    search: Optional[str] = None  # Búsqueda general en múltiples campos

# --- PAGINACIÓN GENÉRICA ---
T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    items: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool