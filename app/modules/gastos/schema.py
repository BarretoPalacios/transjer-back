from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, Field
from datetime import datetime

class DetalleGasto(BaseModel):
    tipo_gasto: str
    tipo_gasto_personalizado: Optional[str] = None
    valor: float
    observacion: Optional[str] = None

class GastoBase(BaseModel):
    placa: str
    ambito: str
    fecha_gasto: datetime
    detalles_gastos: List[DetalleGasto]
    estado: str
    id_gasto: Optional[str] = None
    usuario_registro: Optional[str] = None

class GastoCreate(BaseModel):
    placa: str
    ambito: str
    fecha_gasto: Optional[datetime] = None
    detalles_gastos: List[DetalleGasto]

class GastoUpdate(BaseModel):
    placa: Optional[str] = None
    ambito: Optional[str] = None
    fecha_gasto: Optional[datetime] = None
    detalles_gastos: Optional[List[DetalleGasto]] = None
    estado: Optional[str] = None
    usuario_registro: Optional[str] = None

class GastoResponse(GastoBase):
    id: str
    fecha_registro: datetime
    total: float

    class Config:
        from_attributes = True

class GastoFilter(BaseModel):
    id_gasto: Optional[str] = None
    placa: Optional[str] = None
    ambito: Optional[str] = None
    fecha_gasto_desde: Optional[datetime] = None
    fecha_gasto_hasta: Optional[datetime] = None
    estado: Optional[str] = None
    tipo_gasto: Optional[str] = None
    valor_minimo: Optional[float] = None
    valor_maximo: Optional[float] = None

class GastoImport(BaseModel):
    gastos: List[GastoCreate]

class ExcelImportResponse(BaseModel):
    message: str
    result: dict
    
    class Config: 
        json_schema_extra = {
            "example": {
                "message": "Importación exitosa. 45 gastos creados, 3 actualizados",
                "result": {
                    "total_rows": 50,
                    "created": 45,
                    "updated": 3,
                    "skipped": 2,
                    "errors": [
                        "Fila 15: Gasto duplicado"
                    ],
                    "has_errors": True,
                    "success_rate": "96.0%"
                }
            }
        }

class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1, description="Número de página")
    page_size: int = Field(default=10, ge=1, le=100, description="Elementos por página")

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    items: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool

    class Config:
        from_attributes = True