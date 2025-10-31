import pandas as pd
import uuid
import os
import time
from typing import List, Dict
from decimal import Decimal, InvalidOperation
from app.models.schemas import BalanceGeneralRow, ExcelData
from app.utils.job_manager import job_manager
from app.models.schemas import JobStatus
from app.repositories.balance_general_repository import BalanceGeneralRepository
class ExcelService:
    def __init__(self):
        self.repository = BalanceGeneralRepository()

    def _log_error(
        self,
        fecha: str,
        identificacion_cliente: str,
        archivo_origen: str,
        observaciones: str,
        nombre_cliente: str = None,
        id_cliente: str = None,
        tiempo_ejecucion: int = 0,
        total_registros: int = 0,
        totales_generales: dict = None,
        totales_clase: dict = None,
        errores_ecuacion_count: int = 0,
        diferencia_ecuacion: Decimal = None
    ):

        
        # Si no tenemos info del cliente, intentar obtenerla
        if not nombre_cliente or not id_cliente:
            cliente_info = self.repository.get_cliente_info(identificacion_cliente)
            id_cliente = cliente_info["id_cliente"]
            nombre_cliente = cliente_info["nombre_cliente"]
        
        # Valores por defecto si no hay totales
        if not totales_generales:
            totales_generales = {
                "suma_saldo_inicial": Decimal('0'),
                "suma_debito": Decimal('0'),
                "suma_credito": Decimal('0')
            }
        
        if not totales_clase:
            totales_clase = {
                "total_clase_1": Decimal('0'),
                "total_clase_2": Decimal('0'),
                "total_clase_3": Decimal('0'),
                "total_clase_4": Decimal('0'),
                "total_clase_5": Decimal('0')
            }
        
        if diferencia_ecuacion is None:
            diferencia_ecuacion = Decimal('0')
        
        try:
            self.repository.insert_log_carga(
                fecha_carga=fecha,
                id_cliente=str(id_cliente) if id_cliente else identificacion_cliente,
                nombre_cliente=nombre_cliente,
                estado='ERROR',
                total_registros=total_registros,
                total_activos=totales_clase["total_clase_1"],
                total_pasivos=totales_clase["total_clase_2"],
                total_patrimonio=totales_clase["total_clase_3"],
                total_ingresos=totales_clase["total_clase_4"],
                total_gastos=totales_clase["total_clase_5"],
                suma_saldo_inicial=totales_generales["suma_saldo_inicial"],
                suma_debito=totales_generales["suma_debito"],
                suma_credito=totales_generales["suma_credito"],
                observaciones=observaciones,
                archivo_origen=archivo_origen,
                cantidad_errores_jerarquia=errores_ecuacion_count,
                diferencia_ecuacion_contable=diferencia_ecuacion,
                tiempo_ejecucion=tiempo_ejecucion
            )
            print(f"📝 Log de ERROR guardado")
        except Exception as e:
            print(f"⚠️ No se pudo guardar log de error: {str(e)}")
    
    def _log_exitoso(
        self,
        fecha: str,
        identificacion_cliente: str,
        archivo_origen: str,
        nombre_cliente: str,
        id_cliente: str,
        tiempo_ejecucion: int,
        totales_generales: dict,
        totales_clase: dict,
        ecuacion: dict,
        errores_ecuacion_count: int,
        observaciones: str = None
    ):
        """Helper para registrar logs EXITOSOS"""
        
        if not observaciones:
            observaciones = 'Carga completada correctamente. Todas las validaciones pasaron.'
        
        try:
            self.repository.insert_log_carga(
                fecha_carga=fecha,
                id_cliente=str(id_cliente) if id_cliente else identificacion_cliente,
                nombre_cliente=nombre_cliente,
                estado='EXITOSO',
                total_registros=totales_generales["total_registros"],
                total_activos=totales_clase["total_clase_1"],
                total_pasivos=totales_clase["total_clase_2"],
                total_patrimonio=totales_clase["total_clase_3"],
                total_ingresos=totales_clase["total_clase_4"],
                total_gastos=totales_clase["total_clase_5"],
                suma_saldo_inicial=totales_generales["suma_saldo_inicial"],
                suma_debito=totales_generales["suma_debito"],
                suma_credito=totales_generales["suma_credito"],
                observaciones=observaciones,
                archivo_origen=archivo_origen,
                cantidad_errores_jerarquia=errores_ecuacion_count,
                diferencia_ecuacion_contable=ecuacion["diferencia_ecuacion_contable"],
                tiempo_ejecucion=tiempo_ejecucion
            )
            print(f"📝 Log EXITOSO guardado")
        except Exception as e:
            print(f"⚠️ No se pudo guardar log exitoso: {str(e)}")
    
    def _log_advertencia(
        self,
        fecha: str,
        identificacion_cliente: str,
        archivo_origen: str,
        nombre_cliente: str,
        id_cliente: str,
        tiempo_ejecucion: int,
        totales_generales: dict,
        totales_clase: dict,
        ecuacion: dict,
        errores_ecuacion_count: int,
        observaciones: str
    ):
        """Helper para registrar logs con ADVERTENCIA"""
        
        try:
            self.repository.insert_log_carga(
                fecha_carga=fecha,
                id_cliente=str(id_cliente) if id_cliente else identificacion_cliente,
                nombre_cliente=nombre_cliente,
                estado='ADVERTENCIA',
                total_registros=totales_generales["total_registros"],
                total_activos=totales_clase["total_clase_1"],
                total_pasivos=totales_clase["total_clase_2"],
                total_patrimonio=totales_clase["total_clase_3"],
                total_ingresos=totales_clase["total_clase_4"],
                total_gastos=totales_clase["total_clase_5"],
                suma_saldo_inicial=totales_generales["suma_saldo_inicial"],
                suma_debito=totales_generales["suma_debito"],
                suma_credito=totales_generales["suma_credito"],
                observaciones=observaciones,
                archivo_origen=archivo_origen,
                cantidad_errores_jerarquia=errores_ecuacion_count,
                diferencia_ecuacion_contable=ecuacion["diferencia_ecuacion_contable"],
                tiempo_ejecucion=tiempo_ejecucion
            )
            print(f"📝 Log de ADVERTENCIA guardado")
        except Exception as e:
            print(f"⚠️ No se pudo guardar log de advertencia: {str(e)}")

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
        """Procesa, valida y guarda el archivo con transacción completa"""
        start_time = time.time()
        archivo_origen = os.path.basename(file_path)
        
        try:
            # 1. Procesamiento del Excel
            job_manager.update_job(
                job_id,
                status=JobStatus.PROCESSING,
                message="Leyendo archivo Excel...",
                progress=10
            )
            
            excel_data = self.process_excel_file(
                file_path,
                identificacion_cliente,
                fecha
            )
            
            total_rows = len(excel_data.rows)
            job_manager.update_job(
                job_id,
                message=f"Archivo procesado: {total_rows} filas encontradas",
                progress=20,
                total_rows=total_rows
            )
            
            # 2. Validación de estructura
            job_manager.update_job(
                job_id,
                status=JobStatus.VALIDATING,
                message="Validando estructura de datos...",
                progress=30
            )
            
            validation_result = self.validate_data(
                excel_data.rows,
                fecha,
                identificacion_cliente
            )
            
            if not validation_result["valid"]:
                tiempo_ejecucion = int(time.time() - start_time)
                
                # Log de error - validación de estructura
                self._log_error(
                    fecha=fecha,
                    identificacion_cliente=identificacion_cliente,
                    archivo_origen=archivo_origen,
                    observaciones=f"Validación de estructura falló: {', '.join(validation_result['errors'][:3])}",
                    tiempo_ejecucion=tiempo_ejecucion
                )
                
                job_manager.update_job(
                    job_id,
                    status=JobStatus.FAILED,
                    message="Validación de estructura falló",
                    progress=100,
                    errors=validation_result["errors"]
                )
                return
            
            # 3. TRANSACCIÓN COMPLETA: Guardar y Validar
            job_manager.update_job(
                job_id,
                status=JobStatus.SAVING,
                message="Guardando datos en transacción...",
                progress=50
            )
            
            print(f"🔄 Iniciando transacción completa...")
            
            # Ejecutar transacción con validaciones
            result = self.repository.save_with_transaction_and_validations(
                rows=excel_data.rows,
                fecha=fecha,
                identificacion_cliente=identificacion_cliente
            )
            
            # Obtener info del cliente
            cliente_info = self.repository.get_cliente_info(identificacion_cliente)
            id_cliente = cliente_info["id_cliente"]
            nombre_cliente = cliente_info["nombre_cliente"]
            tiempo_ejecucion = int(time.time() - start_time)
            
            print(f"📋 Cliente: {nombre_cliente} (ID: {id_cliente})")
            
            # 4. Procesar resultado de la transacción
            if not result["success"]:
                # ROLLBACK ejecutado - Log de error
                print(f"❌ Transacción fallida: {result['message']}")
                
                self._log_error(
                    fecha=fecha,
                    identificacion_cliente=identificacion_cliente,
                    archivo_origen=archivo_origen,
                    observaciones=result["message"],
                    nombre_cliente=nombre_cliente,
                    id_cliente=id_cliente,
                    tiempo_ejecucion=tiempo_ejecucion,
                    total_registros=result.get("rows_inserted", 0),
                    totales_generales=result.get("totales_generales"),
                    totales_clase=result.get("totales_clase"),
                    errores_ecuacion_count=result.get("errores_ecuacion_count", 0),
                    diferencia_ecuacion=result.get("diferencia_ecuacion", Decimal('0'))
                )
                
                job_manager.update_job(
                    job_id,
                    status=JobStatus.FAILED,
                    message=f"Validaciones contables fallaron: {result['message']}",
                    progress=100,
                    errors=result.get("errors", [])
                )
                return
            
            # 5. ÉXITO - Commit ejecutado
            print(f"✅ Transacción exitosa - Datos guardados permanentemente")
            
            totales_generales = result["totales_generales"]
            totales_clase = result["totales_clase"]
            ecuacion = result["ecuacion"]
            errores_ecuacion_count = result["errores_ecuacion_count"]
            
            # Determinar estado final
            warnings = []
            if errores_ecuacion_count > 0:
                warnings.append(f"Se encontraron {errores_ecuacion_count} registros con diferencias menores en su ecuación individual")
            
            if warnings:
                # Log con ADVERTENCIA
                observaciones = '; '.join(warnings)
                self._log_advertencia(
                    fecha=fecha,
                    identificacion_cliente=identificacion_cliente,
                    archivo_origen=archivo_origen,
                    nombre_cliente=nombre_cliente,
                    id_cliente=id_cliente,
                    tiempo_ejecucion=tiempo_ejecucion,
                    totales_generales=totales_generales,
                    totales_clase=totales_clase,
                    ecuacion=ecuacion,
                    errores_ecuacion_count=errores_ecuacion_count,
                    observaciones=observaciones
                )
                estado = 'ADVERTENCIA'
            else:
                # Log EXITOSO
                self._log_exitoso(
                    fecha=fecha,
                    identificacion_cliente=identificacion_cliente,
                    archivo_origen=archivo_origen,
                    nombre_cliente=nombre_cliente,
                    id_cliente=id_cliente,
                    tiempo_ejecucion=tiempo_ejecucion,
                    totales_generales=totales_generales,
                    totales_clase=totales_clase,
                    ecuacion=ecuacion,
                    errores_ecuacion_count=errores_ecuacion_count
                )
                estado = 'EXITOSO'
            
            # Finalizar con éxito
            job_manager.update_job(
                job_id,
                status=JobStatus.COMPLETED,
                message=f"Proceso completado: {result['rows_inserted']} registros guardados. Estado: {estado}",
                progress=100,
                processed_rows=result["rows_inserted"],
                errors=warnings if warnings else []
            )
            
            print(f" Proceso finalizado exitosamente - Estado: {estado}")
        
        except Exception as e:
            # Error general
            print(f"❌ Error crítico en procesamiento: {str(e)}")
            tiempo_ejecucion = int(time.time() - start_time)
            
            self._log_error(
                fecha=fecha,
                identificacion_cliente=identificacion_cliente,
                archivo_origen=archivo_origen,
                observaciones=f"Error crítico en procesamiento: {str(e)[:200]}",
                tiempo_ejecucion=tiempo_ejecucion
            )
            
            job_manager.update_job(
                job_id,
                status=JobStatus.FAILED,
                message=f"Error crítico: {str(e)}",
                progress=100,
                errors=[str(e)]
            )