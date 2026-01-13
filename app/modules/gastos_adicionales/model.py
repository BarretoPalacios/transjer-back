# ID Gasto	ID Viaje	Fecha	Tipo de Gasto	Valor	¿Se Factura?	Estado Facturación	Nº Factura	Estado Aprobación	Usuario
# 1	TRK-102	13-ene	Estadía	$150.00	SÍ	Facturado	FAC-2026-001	Aprobado	J. Pérez
# 2	TRK-102	13-ene	Reparación	$45.00	NO	N/A	---	Aprobado	A. Gómez
# 3	TRK-105	14-ene	Peaje Extra	$20.00	SÍ	Pendiente	---	Aprobado	J. Pérez
# 4	TRK-108	15-ene	Maniobras	$90.00	SÍ	Facturado	FAC-2026-005	Aprobado	J. Pérez
# 5	TRK-110	15-ene	Multa	$210.00	NO	N/A	---	Rechazado	Sistema

# # 


from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator

class GastoAdicional(BaseModel):
    # Identificadores ajustados
    codigo_gasto: Optional[str] = Field(None, description="Código único del gasto (ej. G-001)")
    id_flete: str = Field(..., description="ID del flete o viaje asociado")
    
    # Información del Gasto
    fecha_gasto: datetime = Field(default_factory=datetime.now, description="Fecha del evento")
    tipo_gasto: str = Field(..., description="Estadía, Peaje Extra, Maniobra, Reparación, Viático, Multa")
    descripcion: str = Field(..., min_length=5, description="Detalle de la incidencia")
    valor: float = Field(..., gt=0, description="Monto del gasto extra")
    
    # Facturación y Cobro
    se_factura_cliente: bool = Field(default=False, description="¿Se le cobrará este monto al cliente?")
    estado_facturacion: str = Field(default="N/A", description="N/A, Pendiente, Facturado")
    numero_factura: Optional[str] = Field(None, description="Número de la factura emitida al cliente")
    
    # Control de Gestión
    estado_aprobacion: str = Field(default="pendiente", description="pendiente, aprobado, rechazado")
    usuario_registro: str = Field(..., description="Usuario que reporta el gasto")

    @field_validator('estado_facturacion')
    @classmethod
    def validar_estado_facturacion(cls, v, info):
        # Lógica: Si se factura al cliente, el estado no puede ser N/A
        if info.data.get('se_factura_cliente') and v == "N/A":
            return "Pendiente"
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "codigo_gasto": "G-9923",
                "id_flete": "FLT-550",
                "fecha_gasto": "2026-01-13T09:00:00",
                "tipo_gasto": "Estadía",
                "descripcion": "Unidad retenida en planta por falta de documentación del cliente",
                "valor": 120.00,
                "se_factura_cliente": True,
                "estado_facturacion": "Pendiente",
                "numero_factura": None,
                "estado_aprobacion": "pendiente",
                "usuario_registro": "control_trafico_01"
            }
        }