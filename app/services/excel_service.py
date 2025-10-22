import pandas as pd
import uuid
from typing import List, Dict
from decimal import Decimal, InvalidOperation
from app.models.schemas import BalanceGeneralRow, ExcelData
from app.repositories.database_repository import DatabaseRepository
from app.utils.job_manager import job_manager
from app.models.schemas import JobStatus

class ExcelService:
    def __init__(self):
        self.repository = DatabaseRepository()
    
    def _is_empty_row(self, row) -> bool:
        """Verifica si una fila está vacía o contiene solo valores NaN"""
        # Verifica campos clave que siempre deben tener valor
        key_fields = ['Nivel', 'Código cuenta contable', 'Nombre cuenta contable']
        
        for field in key_fields:
            value = row.get(field)
            # Si el valor no es NaN y no está vacío, la fila no está vacía
            if pd.notna(value) and str(value).strip() != '':
                return False
        
        return True
    
    def _clean_numeric_value(self, value) -> Decimal:
        """Limpia y convierte valores numéricos, manejando NaN"""
        if pd.isna(value) or value == '' or str(value).lower() == 'nan':
            return Decimal('0')
        
        try:
            # Convierte a string, limpia espacios y comas
            clean_value = str(value).strip().replace(',', '')
            return Decimal(clean_value)
        except (InvalidOperation, ValueError):
            return Decimal('0')
    
    def _clean_string_value(self, value) -> str:
        """Limpia valores de texto, manejando NaN"""
        if pd.isna(value) or str(value).lower() == 'nan':
            return ''
        return str(value).strip()
    
    def process_excel_file(
        self, 
        file_path: str, 
        identificacion_cliente: str, 
        fecha: str
    ) -> ExcelData:
        """Lee y procesa el archivo Excel desde la fila 8 hasta que encuentre filas vacías"""
        try:
            # Lee el Excel desde la fila 8 (índice 7)
            df = pd.read_excel(file_path, skiprows=7)
            
            # Nombres de columnas esperados
            expected_columns = [
                'Nivel', 'Transaccional', 'Código cuenta contable',
                'Nombre cuenta contable', 'Identificación', 'Sucursal',
                'Nombre tercero', 'Saldo inicial', 'Movimiento débito',
                'Movimiento crédito', 'Saldo final'
            ]
            
            # Verifica columnas
            if not all(col in df.columns for col in expected_columns):
                raise ValueError(f"El Excel no tiene las columnas requeridas. Columnas encontradas: {list(df.columns)}")
            
            # Convierte a objetos Pydantic
            rows = []
            for idx, row in df.iterrows():
                # Si encontramos una fila vacía, detenemos el proceso
                if self._is_empty_row(row):
                    break
                
                try:
                    # Limpia el valor de Transaccional
                    transaccional_raw = self._clean_string_value(row['Transaccional'])
                    # Si está vacío o es nan, usa 'No' como default
                    transaccional = transaccional_raw if transaccional_raw in ['Sí', 'Si', 'No'] else 'No'
                    
                    balance_row = BalanceGeneralRow(
                        nivel=self._clean_string_value(row['Nivel']),
                        transaccional=transaccional,
                        codigo_cuenta_contable=str(int((float(row['Código cuenta contable'])))),
                        nombre_cuenta_contable=self._clean_string_value(row['Nombre cuenta contable']),
                        identificacion=self._clean_string_value(row['Identificación']),
                        sucursal=self._clean_string_value(row['Sucursal']),
                        nombre_tercero=self._clean_string_value(row['Nombre tercero']),
                        saldo_inicial=self._clean_numeric_value(row['Saldo inicial']),
                        movimiento_debito=self._clean_numeric_value(row['Movimiento débito']),
                        movimiento_credito=self._clean_numeric_value(row['Movimiento crédito']),
                        saldo_final=self._clean_numeric_value(row['Saldo final'])
                    )
                    rows.append(balance_row)
                except Exception as e:
                    raise ValueError(f"Error en fila {idx + 8}: {str(e)}")
            
            if len(rows) == 0:
                raise ValueError("No se encontraron datos válidos en el Excel")
            
            return ExcelData(
                rows=rows,
                total_rows=len(rows),
                identificacion_cliente=identificacion_cliente,
                fecha=fecha
            )
        
        except Exception as e:
            raise ValueError(f"Error procesando Excel: {str(e)}")
    
    def save_to_database(
        self, 
        rows: List[BalanceGeneralRow], 
        fecha: str, 
        identificacion_cliente: str
    ) -> Dict:
        """Guarda los datos en la base de datos"""
        return self.repository.insert_balance_general_bulk(rows, fecha, identificacion_cliente)
    
    def validate_data(
        self, 
        rows: List[BalanceGeneralRow],
        fecha: str,
        identificacion_cliente: str
    ) -> dict:
        """Valida los datos antes de guardar"""
        errors = []
        
        # Valida fecha (formato YYYYMMDD)
        if not fecha or len(fecha) != 8 or not fecha.isdigit():
            errors.append("Fecha debe estar en formato YYYYMMDD (ej: 20240630)")
        
        # Valida identificación cliente
        if not identificacion_cliente or not identificacion_cliente.strip():
            errors.append("Identificación del cliente es requerida")
        
        # Valida cada fila
        for i, row in enumerate(rows):
            row_num = i + 8  # Número real de fila en Excel
            
            if not row.nivel or row.nivel == '':
                errors.append(f"Fila {row_num}: Nivel es requerido")
            
            if not row.codigo_cuenta_contable or row.codigo_cuenta_contable == '':
                errors.append(f"Fila {row_num}: Código cuenta contable es requerido")
            
            if not row.nombre_cuenta_contable or row.nombre_cuenta_contable == '':
                errors.append(f"Fila {row_num}: Nombre cuenta contable es requerido")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "total_rows": len(rows)
        }
    def process_and_save_async(
            self,
            file_path: str,
            identificacion_cliente: str,
            fecha: str,
            job_id: str
        ):
            """Procesa y guarda el archivo de forma asíncrona"""
            try:
                # 1. Actualizar estado: Procesando
                job_manager.update_job(
                    job_id,
                    status=JobStatus.PROCESSING,
                    message="Leyendo archivo Excel...",
                    progress=10
                )
                
                # Procesar Excel
                excel_data = self.process_excel_file(
                    file_path,
                    identificacion_cliente,
                    fecha
                )
                
                total_rows = len(excel_data.rows)
                job_manager.update_job(
                    job_id,
                    message=f"Archivo procesado: {total_rows} filas encontradas",
                    progress=30,
                    total_rows=total_rows
                )
                
                # 2. Actualizar estado: Validando
                job_manager.update_job(
                    job_id,
                    status=JobStatus.VALIDATING,
                    message="Validando datos...",
                    progress=40
                )
                
                # Validar datos
                validation_result = self.validate_data(
                    excel_data.rows,
                    fecha,
                    identificacion_cliente
                )
                
                if not validation_result["valid"]:
                    # Validación falló
                    job_manager.update_job(
                        job_id,
                        status=JobStatus.FAILED,
                        message="Validación falló",
                        progress=100,
                        errors=validation_result["errors"]
                    )
                    return
                
                job_manager.update_job(
                    job_id,
                    message="Validación exitosa",
                    progress=50
                )
                
                # 3. Actualizar estado: Guardando
                job_manager.update_job(
                    job_id,
                    status=JobStatus.SAVING,
                    message="Guardando en base de datos...",
                    progress=60
                )
                
                # Guardar en base de datos
                result = self.save_to_database(
                    excel_data.rows,
                    fecha,
                    identificacion_cliente
                )
                
                # 4. Verificar resultados
                if result["failed"] > 0:
                    job_manager.update_job(
                        job_id,
                        status=JobStatus.COMPLETED,
                        message=f"Proceso completado con errores: {result['successful']} exitosos, {result['failed']} fallidos",
                        progress=100,
                        processed_rows=result["successful"],
                        errors=result["errors"]
                    )
                else:
                    job_manager.update_job(
                        job_id,
                        status=JobStatus.COMPLETED,
                        message=f"Proceso completado exitosamente: {result['successful']} filas guardadas",
                        progress=100,
                        processed_rows=result["successful"]
                    )
            
            except Exception as e:
                # Error general
                job_manager.update_job(
                    job_id,
                    status=JobStatus.FAILED,
                    message=f"Error en el procesamiento: {str(e)}",
                    progress=100,
                    errors=[str(e)]
                )