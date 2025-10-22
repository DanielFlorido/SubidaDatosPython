from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from decimal import Decimal
from enum import Enum

class BalanceGeneralRow(BaseModel):
    nivel: str = Field(..., description="Nivel de la cuenta")
    transaccional: str = Field(..., description="Si es transaccional (Sí/No)")
    codigo_cuenta_contable: str = Field(..., description="Código de la cuenta")
    nombre_cuenta_contable: str = Field(..., description="Nombre de la cuenta")
    identificacion: Optional[str] = Field(default="", description="Identificación del tercero")
    sucursal: Optional[str] = Field(default="", description="Sucursal")
    nombre_tercero: Optional[str] = Field(default="", description="Nombre del tercero")
    saldo_inicial: Decimal = Field(..., description="Saldo inicial")
    movimiento_debito: Decimal = Field(..., description="Movimiento débito")
    movimiento_credito: Decimal = Field(..., description="Movimiento crédito")
    saldo_final: Decimal = Field(..., description="Saldo final")
    
    @field_validator('transaccional')
    @classmethod
    def validate_transaccional(cls, v):
        # Permite valores vacíos y los convierte a 'No'
        if not v or v.strip() == '' or str(v).lower() == 'nan':
            return 'No'
        if v not in ['Sí', 'No', 'Si']:
            return 'No'  # Default en lugar de error
        return 'Sí' if v == 'Si' else v
    
    @field_validator('identificacion', 'sucursal', 'nombre_tercero')
    @classmethod
    def empty_string_to_none(cls, v):
        if v is None or str(v).strip() == '' or str(v).lower() == 'nan':
            return ""
        return str(v).strip()

class ExcelUploadRequest(BaseModel):
    identificacion_cliente: str = Field(..., description="Identificación del cliente")
    fecha: str = Field(..., description="Fecha en formato YYYYMMDD")

class ExcelData(BaseModel):
    rows: List[BalanceGeneralRow]
    total_rows: int
    identificacion_cliente: str
    fecha: str

class ConfirmationRequest(BaseModel):
    data: List[BalanceGeneralRow]
    identificacion_cliente: str
    fecha: str
    confirmed: bool

class ProcessResult(BaseModel):
    status: str
    message: str
    rows_processed: int = 0
    errors: List[str] = []


class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    VALIDATING = "validating"
    SAVING = "saving"
    COMPLETED = "completed"
    FAILED = "failed"

class JobResponse(BaseModel):
    job_id: str
    status: JobStatus
    message: str
    created_at: str

class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    message: str
    progress: int  # 0-100
    total_rows: int = 0
    processed_rows: int = 0
    errors: List[str] = []
    created_at: str
    updated_at: str
    completed_at: Optional[str] = None