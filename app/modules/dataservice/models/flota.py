from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field, field_serializer 

class Flota(BaseModel):
    codigo_flota: Optional[str]  = Field(..., min_length=1, max_length=20, description="Código único de la flota")
    placa: str = Field(..., description="Placa del vehículo")
    marca: Optional[str]  = Field(None,  description="Marca del vehículo")
    modelo: Optional[str]  = Field(None,  description="Modelo del vehículo")
    anio: int = Field(..., description="Año de fabricación")
    tn: float = Field(..., ge=0, description="Capacidad de carga en toneladas")
    m3: float = Field(..., ge=0, description="Capacidad volumétrica en metros cúbicos")
    tipo_vehiculo: str = Field(..., description="Volquete, Furgón, Plataforma, Tanque, Cisterna, etc.")
    tipo_combustible: Optional[str] = Field(default="Diesel", description="Diesel, Gasolina, GNV, Eléctrico")
    
    # Datos del conductor
    nombre_conductor: Optional[str] = Field(None,  description="Nombre completo del conductor")
    numero_licencia: Optional[str] = Field(None,  description="Número de licencia de conducir")
    
    revision_tecnica_emision: Optional[date] = Field(None, description="Fecha de emisión de la revisión técnica")
    revision_tecnica_vencimiento: Optional[date] = Field(None, description="Fecha de vencimiento de la revisión técnica")
    soat_vigencia_inicio: Optional[date] = Field(None, description="Inicio de vigencia del SOAT")
    soat_vigencia_fin: Optional[date] = Field(None, description="Fin de vigencia del SOAT")
    mtc_numero: Optional[str] = Field(None, description="Número de autorización o registro MTC")
    extintor_vencimiento: Optional[date] = Field(None, description="Fecha de vencimiento del extintor")
    cantidad_parihuelas: int = Field(0, ge=0, description="Cantidad de parihuelas transportadas")

    dias_alerta_revision_tecnica: int = Field(default=30, ge=1, le=365, description="Días de anticipación para alertar vencimiento de revisión técnica")
    dias_alerta_soat: int = Field(default=30, ge=1, le=365, description="Días de anticipación para alertar vencimiento del SOAT")
    dias_alerta_extintor: int = Field(default=15, ge=1, le=365, description="Días de anticipación para alertar vencimiento del extintor")

    observaciones: Optional[str] = Field(None, description="Observaciones adicionales")
    fecha_registro: datetime = Field(default_factory=datetime.now, description="Fecha de registro en el sistema")
    activo: bool = Field(default=True, description="Indica si el vehículo está activo")
    

    @field_serializer('revision_tecnica_emision', 'revision_tecnica_vencimiento', 'soat_vigencia_inicio', 'soat_vigencia_fin', 'extintor_vencimiento')
    def serialize_date(self, value: Optional[date], _info):
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, datetime.min.time())
        return value

    def verificar_alertas(self) -> dict:
        hoy = date.today()
        alertas = {"revision_tecnica": False, "soat": False, "extintor": False, "mensajes": []}

        if self.revision_tecnica_vencimiento:
            dias_faltantes = (self.revision_tecnica_vencimiento - hoy).days
            if 0 <= dias_faltantes <= self.dias_alerta_revision_tecnica:
                alertas["revision_tecnica"] = True
                alertas["mensajes"].append(f"Revisión técnica vence en {dias_faltantes} días ({self.revision_tecnica_vencimiento})")

        if self.soat_vigencia_fin:
            dias_faltantes = (self.soat_vigencia_fin - hoy).days
            if 0 <= dias_faltantes <= self.dias_alerta_soat:
                alertas["soat"] = True
                alertas["mensajes"].append(f"SOAT vence en {dias_faltantes} días ({self.soat_vigencia_fin})")

        if self.extintor_vencimiento:
            dias_faltantes = (self.extintor_vencimiento - hoy).days
            if 0 <= dias_faltantes <= self.dias_alerta_extintor:
                alertas["extintor"] = True
                alertas["mensajes"].append(f"Extintor vence en {dias_faltantes} días ({self.extintor_vencimiento})")

        return alertas

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "codigo_flota": "FL-001",
                "placa": "ABC-123",
                "marca": "Volvo",
                "modelo": "FH16",
                "anio": 2022,
                "tn": 20.0,
                "m3": 15.0,
                "tipo_vehiculo": "Volquete",
                "tipo_combustible": "Diesel",
                "nombre_conductor": "Juan Pérez García",
                "numero_licencia": "Q12345678",
                "revision_tecnica_emision": "2024-01-10",
                "revision_tecnica_vencimiento": "2025-01-10",
                "soat_vigencia_inicio": "2024-02-01",
                "soat_vigencia_fin": "2025-02-01",
                "mtc_numero": "MTC-458712",
                "extintor_vencimiento": "2025-06-30",
                "cantidad_parihuelas": 12,
                "dias_alerta_revision_tecnica": 30,
                "dias_alerta_soat": 30,
                "dias_alerta_extintor": 15,
                "observaciones": "Unidad operativa y documentada",
                "activo": True
            }
        }