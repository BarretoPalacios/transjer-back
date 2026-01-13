from datetime import date, datetime
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from decimal import Decimal
from enum import Enum

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

class ServicioSnapshot(BaseModel):
    codigo_servicio: str
    nombre_cliente: str
    nombre_cuenta: str
    nombre_proveedor: str
    placa_flota: str
    nombre_conductor: str
    nombre_auxiliar: str
    m3: str
    tn: str
    tipo_servicio: str
    modalidad: str
    zona: str
    fecha_servicio: date
    fecha_salida: date
    gia_rr: str
    gia_rt: str
    origen: str
    destino: str

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

class FacturacionGestion(BaseModel):
    codigo_factura: str = Field(...)
    
    datos_completos: Optional[FacturaSnapshot] = None
    
    estado_detraccion: EstadoDetraccion = Field(default=EstadoDetraccion.PENDIENTE)
    tasa_detraccion: Decimal = Field(default=Decimal("4.0"))
    monto_detraccion: Decimal = Field(default=Decimal("0.0"))
    nro_constancia_detraccion: Optional[str] = Field(None, max_length=50)
    fecha_pago_detraccion: Optional[date] = None
    
    estado_pago_neto: EstadoPagoNeto = Field(default=EstadoPagoNeto.PENDIENTE)
    monto_neto: Decimal = Field(...)
    monto_pagado_acumulado: Decimal = Field(default=Decimal("0.0"))
    
    banco_destino: Optional[str] = Field(None)
    cuenta_bancaria_destino: Optional[str] = Field(None)
    nro_operacion_pago_neto: Optional[str] = Field(None)
    
    fecha_probable_pago: Optional[date] = Field(None)
    prioridad: PrioridadPago = Field(default=PrioridadPago.MEDIA)
    
    centro_costo: Optional[str] = Field(None)
    responsable_gestion: Optional[str] = Field(None)
    observaciones_admin: Optional[str] = Field(None, max_length=500)

    ultima_actualizacion: datetime = Field(default_factory=datetime.now)

    @field_validator('monto_detraccion', 'monto_neto')
    def check_positivo(cls, v):
        if v < 0:
            raise ValueError('El monto no puede ser negativo')
        return v

    @property
    def saldo_pendiente(self) -> Decimal:
        return self.monto_neto - self.monto_pagado_acumulado

    class Config:
        use_enum_values = True
        json_schema_extra = {
            "example": {
                "codigo_factura": "F-202601719",
                "datos_completos": {
                    "numero_factura": "F-202601445",
                    "fecha_emision": "2026-01-11",
                    "fecha_vencimiento": "2026-02-11",
                    "monto_total": 1000.0,
                    "fletes": [
                        {
                            "codigo_flete": "FLT-0000000015",
                            "monto_flete": 1000.0,
                            "servicio": {
                                "codigo_servicio": "SRV-0000000018",
                                "nombre_cliente": "ALICORP",
                                "nombre_cuenta": "Cuenta ALICORP",
                                "nombre_proveedor": "TRANSPORTES EJEMPLO SAC",
                                "placa_flota": "A4M-831",
                                "nombre_conductor": "Dvid Barrto",
                                "nombre_auxiliar": "MARLON ESTRELLA GLICERO",
                                "m3": "10",
                                "tn": "5",
                                "tipo_servicio": "Local",
                                "modalidad": "TRASLADO",
                                "zona": "lima",
                                "fecha_servicio": "2026-01-09",
                                "fecha_salida": "2026-01-09",
                                "gia_rr": "1111",
                                "gia_rt": "22222",
                                "origen": "origen ALICORP",
                                "destino": "El Agustino"
                            }
                        }
                    ]
                },
                "estado_detraccion": "Pagado",
                "monto_detraccion": 40.0,
                "monto_neto": 960.0,
                "estado_pago_neto": "Pendiente",
                "prioridad": "Media"
            }
        }