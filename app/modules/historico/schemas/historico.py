# schemas/historico_schemas.py
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, date
from typing import Optional, List
from enum import Enum


# ============ ENUMS ============
class TipoHistorico(str, Enum):
    """Tipos de registro histórico"""
    COMPLETADO = "completado"
    CANCELADO = "cancelado"


# ============ SCHEMA DE CREACIÓN ============
class HistoricoCreate(BaseModel):
    """Schema para crear un registro histórico"""
    servicio_id: str = Field(..., description="ID del servicio")
    codigo_servicio: str = Field(..., description="Código del servicio")
    tipo: TipoHistorico = Field(..., description="Tipo de histórico")
    estado_final: str = Field(..., description="Estado final del servicio")
    usuario: str = Field(..., description="Usuario que registró")
    justificacion: Optional[str] = Field(None, description="Justificación del cambio")
    # periodo: str = Field(..., description="Periodo de cierre (YYYY-MM)")
    
    # @field_validator('periodo')
    # @classmethod
    # def validar_formato_periodo(cls, v):
    #     """Validar que el periodo tenga formato YYYY-MM"""
    #     try:
    #         datetime.strptime(v, "%Y-%m")
    #     except ValueError:
    #         raise ValueError("El periodo debe tener formato YYYY-MM")
    #     return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "servicio_id": "507f1f77bcf86cd799439011",
                "codigo_servicio": "SRV-2024-001",
                "tipo": "completado",
                "estado_final": "completado",
                "usuario": "admin@empresa.com",
                "justificacion": "Servicio finalizado exitosamente",
                # "periodo": "2024-12"
            }
        }


# ============ SCHEMA DE RESPUESTA ============
class HistoricoResponse(BaseModel):
    """Schema de respuesta para un registro histórico"""
    id: str = Field(..., description="ID del registro histórico")
    servicio_id: str = Field(..., description="ID del servicio")
    codigo_servicio: str = Field(..., description="Código del servicio")
    tipo: str = Field(..., description="Tipo de histórico")
    estado_final: str = Field(..., description="Estado final del servicio")
    fecha_registro: datetime = Field(..., description="Fecha de registro")
    usuario: str = Field(..., description="Usuario que registró")
    justificacion: Optional[str] = Field(None, description="Justificación del cambio")
    # periodo: str = Field(..., description="Periodo de cierre")
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "507f1f77bcf86cd799439011",
                "servicio_id": "507f1f77bcf86cd799439012",
                "codigo_servicio": "SRV-2024-001",
                "tipo": "completado",
                "estado_final": "completado",
                "fecha_registro": "2024-12-26T10:30:00",
                "usuario": "admin@empresa.com",
                "justificacion": "Servicio finalizado exitosamente",
                # "periodo": "2024-12"
            }
        }


# ============ SCHEMA DE RESPUESTA CON SERVICIO ============
class HistoricoConServicioResponse(BaseModel):
    """Schema de respuesta que incluye datos del servicio"""
    id: str
    servicio_id: str
    codigo_servicio: str
    tipo: str
    estado_final: str
    fecha_registro: datetime
    usuario: str
    justificacion: Optional[str] = None
    # periodo: str
    servicio: Optional[dict] = Field(None, description="Datos completos del servicio")
    
    class Config:
        from_attributes = True


# ============ SCHEMA DE FILTROS ============
class HistoricoFilter(BaseModel):
    """Schema para filtrar registros históricos"""
    
    # Filtros de texto exacto
    tipo: Optional[TipoHistorico] = Field(None, description="Filtrar por tipo")
    periodo: Optional[str] = Field(None, description="Filtrar por periodo (YYYY-MM)")
    estado_final: Optional[str] = Field(None, description="Filtrar por estado final")
    
    # Filtros de texto con regex
    codigo_servicio: Optional[str] = Field(None, description="Buscar por código de servicio")
    usuario: Optional[str] = Field(None, description="Buscar por usuario")
    servicio_id: Optional[str] = Field(None, description="Filtrar por ID de servicio")
    
    # Filtros de fecha
    fecha_inicio: Optional[date] = Field(None, description="Fecha de registro desde")
    fecha_fin: Optional[date] = Field(None, description="Fecha de registro hasta")
    
    # Filtros de periodo (para rangos de meses)
    periodo_inicio: Optional[str] = Field(None, description="Periodo desde (YYYY-MM)")
    periodo_fin: Optional[str] = Field(None, description="Periodo hasta (YYYY-MM)")
    
    # Paginación
    skip: int = Field(0, ge=0, description="Número de registros a saltar")
    limit: int = Field(20, ge=1, le=1000, description="Número máximo de registros")
    
    # Ordenamiento
    sort_by: Optional[str] = Field("fecha_registro", description="Campo para ordenar")
    sort_order: int = Field(-1, description="Orden: 1 ascendente, -1 descendente")
    
    @field_validator('periodo', 'periodo_inicio', 'periodo_fin')
    @classmethod
    def validar_formato_periodo(cls, v):
        """Validar formato de periodo si se proporciona"""
        if v is not None:
            try:
                datetime.strptime(v, "%Y-%m")
            except ValueError:
                raise ValueError("El periodo debe tener formato YYYY-MM")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "tipo": "completado",
                "periodo": "2024-12",
                "fecha_inicio": "2024-12-01",
                "fecha_fin": "2024-12-31",
                "skip": 0,
                "limit": 50,
                "sort_by": "fecha_registro",
                "sort_order": -1
            }
        }


# ============ SCHEMA DE RESPUESTA PAGINADA ============
class HistoricoListResponse(BaseModel):
    """Schema de respuesta para lista paginada de históricos"""
    total: int = Field(..., description="Total de registros")
    skip: int = Field(..., description="Registros saltados")
    limit: int = Field(..., description="Límite de registros")
    data: List[HistoricoResponse] = Field(..., description="Lista de históricos")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total": 150,
                "skip": 0,
                "limit": 50,
                "data": [
                    {
                        "id": "507f1f77bcf86cd799439011",
                        "servicio_id": "507f1f77bcf86cd799439012",
                        "codigo_servicio": "SRV-2024-001",
                        "tipo": "completado",
                        "estado_final": "completado",
                        "fecha_registro": "2024-12-26T10:30:00",
                        "usuario": "admin@empresa.com",
                        "justificacion": "Servicio finalizado exitosamente",
                        "periodo": "2024-12"
                    }
                ]
            }
        }


# ============ SCHEMA DE ESTADÍSTICAS ============
class HistoricoEstadisticas(BaseModel):
    """Schema para estadísticas de históricos"""
    total_registros: int
    total_completados: int
    total_cancelados: int
    por_periodo: List[dict] = Field(..., description="Agrupación por periodo")
    por_usuario: List[dict] = Field(..., description="Agrupación por usuario")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_registros": 150,
                "total_completados": 120,
                "total_cancelados": 30,
                "por_periodo": [
                    {"periodo": "2024-12", "count": 50},
                    {"periodo": "2024-11", "count": 45}
                ],
                "por_usuario": [
                    {"usuario": "admin@empresa.com", "count": 80},
                    {"usuario": "operador@empresa.com", "count": 70}
                ]
            }
        }


# ============ SCHEMA PARA RESTAURAR SERVICIO ============
class RestaurarServicioRequest(BaseModel):
    """Schema para solicitar restauración de un servicio desde histórico"""
    historico_id: str = Field(..., description="ID del registro histórico")
    usuario: str = Field(..., description="Usuario que restaura")
    justificacion: str = Field(..., description="Justificación de la restauración")
    nuevo_estado: str = Field("programado", description="Estado al que volverá el servicio")
    
    class Config:
        json_schema_extra = {
            "example": {
                "historico_id": "507f1f77bcf86cd799439011",
                "usuario": "admin@empresa.com",
                "justificacion": "Error en el cierre, debe reprogramarse",
                "nuevo_estado": "programado"
            }
        }