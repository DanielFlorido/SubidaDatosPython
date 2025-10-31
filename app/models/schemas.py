from pydantic import BaseModel, Field, field_validator, validator
from typing import List, Optional
from decimal import Decimal
from enum import Enum
from datetime import datetime

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
    created_at: Optional[str] = None

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
    
class ValidationResult(BaseModel):
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    
class TotalesGenerales(BaseModel):
    total_registros: int
    suma_saldo_inicial: Decimal
    suma_debito: Decimal
    suma_credito: Decimal
    suma_saldo_final: Decimal
    suma_movimiento_mes: Decimal

class TotalesPorClase(BaseModel):
    total_clase_1: Decimal  # Activos
    total_clase_2: Decimal  # Pasivos
    total_clase_3: Decimal  # Patrimonio
    total_clase_4: Decimal  # Ingresos
    total_clase_5: Decimal  # Gastos

class EcuacionContable(BaseModel):
    activos: Decimal
    pasivos: Decimal
    patrimonio: Decimal
    ingresos: Decimal
    gastos: Decimal
    diferencia_ecuacion_contable: Decimal

class ErrorEcuacion(BaseModel):
    id: int
    nivel: str
    codigo_cuenta: str
    nombre_cuenta: str
    identificacion: str
    nombre_tercero: str
    saldo_inicial: Decimal
    movimiento_debito: Decimal
    movimiento_credito: Decimal
    saldo_final: Decimal
    saldo_calculado: Decimal
    diferencia: Decimal
class EncabezadoFlujoCajaBase(BaseModel):
    codigo_contable: str = Field(..., description="Código contable")
    cuenta_contable: str = Field(..., description="Nombre de la cuenta contable")
    saldo_inicial: float = Field(0.0, description="Saldo inicial")
    debito: float = Field(0.0, description="Débito")
    credito: float = Field(0.0, description="Crédito")
    saldo_total_cuenta: float = Field(0.0, description="Saldo total de la cuenta")

class DetalleFlujoCajaBase(BaseModel):
    codigo_contable: str = Field(..., description="Código contable")
    cuenta_contable: str = Field(..., description="Nombre de la cuenta contable")
    comprobante: Optional[str] = Field("", description="Número de comprobante")
    secuencia: Optional[str] = Field("", description="Secuencia")
    fecha_elaboracion: Optional[str] = Field("", description="Fecha de elaboración")
    identificacion: Optional[str] = Field("", description="Identificación del tercero")
    suc: Optional[str] = Field("", description="Sucursal")
    nombre_tercero: Optional[str] = Field("", description="Nombre del tercero")
    descripcion: Optional[str] = Field("", description="Descripción")
    detalle: Optional[str] = Field("", description="Detalle adicional")
    centro_costo: Optional[str] = Field("", description="Centro de costo")
    debito: float = Field(0.0, description="Débito")
    credito: float = Field(0.0, description="Crédito")
    saldo_movimiento: float = Field(0.0, description="Saldo del movimiento")

class FlujoCajaUploadRequest(BaseModel):
    fecha_movimiento: str = Field(..., description="Fecha del movimiento (YYYY-MM-DD)")
    numero_identificacion: str = Field(..., description="Número de identificación")
    
    @validator('fecha_movimiento')
    def validate_fecha(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('La fecha debe estar en formato YYYY-MM-DD')

class FlujoCajaUploadResponse(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None

class EncabezadoFlujoCajaResponse(BaseModel):
    id: int
    codigo_contable: str
    cuenta_contable: str
    saldo_inicial: float
    debito: float
    credito: float
    saldo_total_cuenta: float
    fecha_movimiento: str
    numero_identificacion: str
    fecha_creacion: Optional[datetime]

    class Config:
        from_attributes = True

class DetalleFlujoCajaResponse(BaseModel):
    id: int
    id_encabezado: int
    codigo_contable: str
    cuenta_contable: str
    comprobante: Optional[str]
    secuencia: Optional[str]
    fecha_elaboracion: Optional[str]
    identificacion: Optional[str]
    suc: Optional[str]
    nombre_tercero: Optional[str]
    descripcion: Optional[str]
    detalle: Optional[str]
    centro_costo: Optional[str]
    debito: float
    credito: float
    saldo_movimiento: float
    fecha_creacion: Optional[datetime]

    class Config:
        from_attributes = True