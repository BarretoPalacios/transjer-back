# models/historico_servicio.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class HistoricoServicio(BaseModel):
    servicio_id: str  # Referencia al servicio original
    codigo_servicio: str  # Para búsquedas rápidas
    tipo: str  # "cancelado" o "completado"
    estado_final: str  # Estado final del servicio
    fecha_registro: datetime  # Fecha en que se registró en histórico
    usuario: Optional[str]  # Usuario que cambió el estado
    justificacion: Optional[str] = None  # Razón del cambio
    # periodo: str  # YYYY-MM del cierre
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "servicio_id": "507f1f77bcf86cd799439011",
                "codigo_servicio": "SRV-2024-001",
                "tipo": "completado",
                "estado_final": "completado",
                "fecha_registro": "2024-12-26T10:30:00",
                "usuario": "admin@empresa.com",
                "justificacion": "Servicio finalizado exitosamente",
                # "periodo": "2024-12"
            }
        }