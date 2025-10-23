# database_repository.py
import pyodbc
from typing import List, Optional
from decimal import Decimal
from app.config import settings
from app.models.schemas import (
    BalanceGeneralRow,
    TotalesGenerales,  
    TotalesPorClase,
    EcuacionContable,
    ErrorEcuacion
)

class DatabaseRepository:
    def __init__(self):
        self.connection_string = (
            f"DRIVER={{{settings.db_driver}}};"
            f"SERVER={settings.db_server};"
            f"PORT=1433;"
            f"DATABASE={settings.db_database};"
            f"UID={settings.db_username};"
            f"PWD={settings.db_password};"
            f"TDS_Version=8.0;"
            f"Encrypt=yes;"
        )
    
    def get_connection(self):
        return pyodbc.connect(self.connection_string)
    
    def insert_balance_general_row(
        self, 
        row: BalanceGeneralRow, 
        fecha: str, 
        identificacion_cliente: str
    ) -> bool:
        """Inserta una fila de balance general usando el stored procedure"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Ejecuta el stored procedure
            cursor.execute("""
                EXEC [dbo].[BalanceGeneralInsertar] 
                    @Nivel = ?,
                    @Transaccional = ?,
                    @CodigoCuentaContable = ?,
                    @NombreCuentaContable = ?,
                    @Identificacion = ?,
                    @Sucursal = ?,
                    @NombreTercero = ?,
                    @SaldoInicial = ?,
                    @MovimientoDebito = ?,
                    @MovimientoCredito = ?,
                    @SaldoFinal = ?,
                    @Fecha = ?,
                    @IdentificacionCliente = ?
            """, (
                row.nivel,
                row.transaccional,
                row.codigo_cuenta_contable,
                row.nombre_cuenta_contable,
                row.identificacion or '',
                row.sucursal or '',
                row.nombre_tercero or '',
                float(row.saldo_inicial),
                float(row.movimiento_debito),
                float(row.movimiento_credito),
                float(row.saldo_final),
                fecha,
                identificacion_cliente
            ))
            
            conn.commit()
            return True
        
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    def test_connection(self):
        """Verifica la conexión a la base de datos y retorna información del error si falla"""
        try:
            conn = self.get_connection()
            conn.close()
            return {"success": True, "message": "Conexión exitosa"}
        except Exception as e:
            return {
                "success": False,
                "error": getattr(e, "args", [""])[0],
                "type": type(e).__name__,
            }
    
    # ---- Modificado: estas funciones aceptan un cursor opcional para poder ejecutarlas dentro
    # ---- de la misma transacción que realiza las inserciones.
    def get_totales_generales(self, fecha: str, identificacion_cliente: str, cursor: Optional[pyodbc.Cursor]=None) -> TotalesGenerales:
        """Obtiene los totales generales de la carga. Si se pasa 'cursor', usa ese cursor (misma transacción)."""
        own_conn = None
        close_cursor = False
        if cursor is None:
            own_conn = self.get_connection()
            cursor = own_conn.cursor()
            close_cursor = True
        
        try:
            query = """
            SELECT 
                COUNT(*) as TotalRegistros,
                ISNULL(SUM([SaldoInicial]), 0) as SumaSaldoInicial,
                ISNULL(SUM([MovimientoDebito]), 0) as SumaDebito,
                ISNULL(SUM([MovimientoCredito]), 0) as SumaCredito,
                ISNULL(SUM([SaldoFinal]), 0) as SumaSaldoFinal,
                ISNULL(SUM([MovimientoMes]), 0) as SumaMovimientoMes
            FROM [dbo].[BalanceGeneral]
            WHERE [Fecha] = ? AND [IdCliente] = ?
            """
            
            cursor.execute(query, (fecha, identificacion_cliente))
            row = cursor.fetchone()
            
            return TotalesGenerales(
                total_registros=row[0],
                suma_saldo_inicial=Decimal(str(row[1])),
                suma_debito=Decimal(str(row[2])),
                suma_credito=Decimal(str(row[3])),
                suma_saldo_final=Decimal(str(row[4])),
                suma_movimiento_mes=Decimal(str(row[5]))
            )
        finally:
            if close_cursor:
                cursor.close()
                own_conn.close()

    def get_totales_por_clase(self, fecha: str, identificacion_cliente: str, cursor: Optional[pyodbc.Cursor]=None) -> TotalesPorClase:
        """Obtiene los totales por clase contable. Usa cursor si se pasa (misma transacción)."""
        own_conn = None
        close_cursor = False
        if cursor is None:
            own_conn = self.get_connection()
            cursor = own_conn.cursor()
            close_cursor = True
        
        try:
            query = """
            SELECT 
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '1'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as TotalClase1,
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '2'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as TotalClase2,
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '3'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as TotalClase3,
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '4'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as TotalClase4,
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '5'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as TotalClase5
            FROM [dbo].[BalanceGeneral]
            WHERE [Fecha] = ? AND [IdCliente] = ?
            """
            
            cursor.execute(query, (fecha, identificacion_cliente))
            row = cursor.fetchone()
            
            return TotalesPorClase(
                total_clase_1=Decimal(str(row[0])),
                total_clase_2=Decimal(str(row[1])),
                total_clase_3=Decimal(str(row[2])),
                total_clase_4=Decimal(str(row[3])),
                total_clase_5=Decimal(str(row[4]))
            )
        finally:
            if close_cursor:
                cursor.close()
                own_conn.close()

    def get_ecuacion_contable(self, fecha: str, identificacion_cliente: str, cursor: Optional[pyodbc.Cursor]=None) -> EcuacionContable:
        """Valida la ecuación contable global. Usa cursor si se pasa (misma transacción)."""
        own_conn = None
        close_cursor = False
        if cursor is None:
            own_conn = self.get_connection()
            cursor = own_conn.cursor()
            close_cursor = True
        
        try:
            query = """
            SELECT 
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '1'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as Activos,
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '2'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as Pasivos,
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '3'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as Patrimonio,
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '4'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as Ingresos,
                ISNULL(SUM(CASE 
                    WHEN [Nivel] = 'Clase' 
                    AND [Transaccional] = 0
                    AND LEFT([CodigoCuentaConble], 1) = '5'
                    THEN ABS([SaldoFinal])
                    ELSE 0 
                END), 0) as Gastos,
                ABS(
                    ISNULL(SUM(CASE WHEN [Nivel] = 'Clase' AND [Transaccional] = 0 AND LEFT([CodigoCuentaConble], 1) = '1' THEN ABS([SaldoFinal]) ELSE 0 END), 0) -
                    (ISNULL(SUM(CASE WHEN [Nivel] = 'Clase' AND [Transaccional] = 0 AND LEFT([CodigoCuentaConble], 1) = '2' THEN ABS([SaldoFinal]) ELSE 0 END), 0) +
                    ISNULL(SUM(CASE WHEN [Nivel] = 'Clase' AND [Transaccional] = 0 AND LEFT([CodigoCuentaConble], 1) = '3' THEN ABS([SaldoFinal]) ELSE 0 END), 0))
                ) as DiferenciaEcuacionContable
            FROM [dbo].[BalanceGeneral]
            WHERE [Fecha] = ? AND [IdCliente] = ?
            """
            
            cursor.execute(query, (fecha, identificacion_cliente))
            row = cursor.fetchone()
            
            return EcuacionContable(
                activos=Decimal(str(row[0])),
                pasivos=Decimal(str(row[1])),
                patrimonio=Decimal(str(row[2])),
                ingresos=Decimal(str(row[3])),
                gastos=Decimal(str(row[4])),
                diferencia_ecuacion_contable=Decimal(str(row[5]))
            )
        finally:
            if close_cursor:
                cursor.close()
                own_conn.close()

    def get_errores_ecuacion(self, fecha: str, identificacion_cliente: str, cursor: Optional[pyodbc.Cursor]=None) -> List[ErrorEcuacion]:
        """Obtiene registros con errores en la ecuación contable individual. Usa cursor si se pasa."""
        own_conn = None
        close_cursor = False
        if cursor is None:
            own_conn = self.get_connection()
            cursor = own_conn.cursor()
            close_cursor = True
        
        try:
            query = """
            SELECT TOP 100
                [Id],
                [Nivel],
                [CodigoCuentaConble],
                [NombreCuentaConble],
                [Identificacion],
                [NombreTercero],
                [SaldoInicial],
                [MovimientoDebito],
                [MovimientoCredito],
                [SaldoFinal],
                ([SaldoInicial] + [MovimientoDebito] - [MovimientoCredito]) as SaldoCalculado,
                ABS([SaldoFinal] - ([SaldoInicial] + [MovimientoDebito] - [MovimientoCredito])) as Diferencia
            FROM [dbo].[BalanceGeneral]
            WHERE [Fecha] = ? 
            AND [IdCliente] = ?
            AND ABS([SaldoFinal] - ([SaldoInicial] + [MovimientoDebito] - [MovimientoCredito])) > 0.01
            ORDER BY Diferencia DESC
            """
            
            cursor.execute(query, (fecha, identificacion_cliente))
            rows = cursor.fetchall()
            
            errores = []
            for row in rows:
                errores.append(ErrorEcuacion(
                    id=row[0],
                    nivel=row[1],
                    codigo_cuenta=row[2],
                    nombre_cuenta=row[3],
                    identificacion=row[4] or '',
                    nombre_tercero=row[5] or '',
                    saldo_inicial=Decimal(str(row[6])),
                    movimiento_debito=Decimal(str(row[7])),
                    movimiento_credito=Decimal(str(row[8])),
                    saldo_final=Decimal(str(row[9])),
                    saldo_calculado=Decimal(str(row[10])),
                    diferencia=Decimal(str(row[11]))
                ))
            
            return errores
        finally:
            if close_cursor:
                cursor.close()
                own_conn.close()

    def insert_log_carga(
        self,
        fecha_carga: str,
        id_cliente: str,
        nombre_cliente: str,
        estado: str,
        total_registros: int,
        total_activos: Decimal,
        total_pasivos: Decimal,
        total_patrimonio: Decimal,
        total_ingresos: Decimal,
        total_gastos: Decimal,
        suma_saldo_inicial: Decimal,
        suma_debito: Decimal,
        suma_credito: Decimal,
        observaciones: str,
        archivo_origen: str,
        cantidad_errores_jerarquia: int,
        diferencia_ecuacion_contable: Decimal,
        tiempo_ejecucion:str
    ):
        """Inserta un log de carga en la base de datos"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                EXEC [dbo].[LogCargasBalanceGeneral_Insertar]
                    @FechaCarga = ?,
                    @IdCliente = ?,
                    @NombreCliente = ?,
                    @TotalRegistros = ?,
                    @TotalActivos = ?,
                    @TotalPasivos = ?,
                    @TotalPatrimonio = ?,
                    @TotalIngresos = ?,
                    @TotalGastos = ?,
                    @SumaSaldoInicial = ?,
                    @SumaDebito = ?,
                    @SumaCredito = ?,
                    @UsuarioCarga = ?,
                    @Observaciones = ?,
                    @ArchivoOrigen = ?,
                    @CantidadErroresJerarquia = ?,
                    @DiferenciaEcuacionContable = ?,
                    @Estado = ?,
                    @TiempoEjecucionSegundos = ?
            """, (
                fecha_carga,
                id_cliente,
                nombre_cliente,
                total_registros,
                float(total_activos),
                float(total_pasivos),
                float(total_patrimonio),
                float(total_ingresos),
                float(total_gastos),
                float(suma_saldo_inicial),
                float(suma_debito),
                float(suma_credito),
                'EquipoPruebas',
                observaciones,
                archivo_origen,
                cantidad_errores_jerarquia,
                float(diferencia_ecuacion_contable),
                estado,
                tiempo_ejecucion
            ))
            # 🔧 Antes no había commit aquí: ahora confirmamos el log en su propia conexión
            conn.commit()
            return True
        except Exception as e:
            # mejor manejo: rollback y propagar
            try:
                conn.rollback()
            except:
                pass
            raise e
        finally:
            cursor.close()
            conn.close()

    def get_cliente_info(self, identificacion: str) -> dict:
        """Obtiene IdCliente y RazonSocial desde la tabla Clientes"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            query = """
            SELECT TOP 1 [IdCliente], [RazonSocial]
            FROM [dbo].[Clientes]
            WHERE [NumeroDocumento] = ?
            """
            
            cursor.execute(query, (identificacion,))
            row = cursor.fetchone()
            
            if row:
                return {
                    "id_cliente": row[0],
                    "nombre_cliente": row[1]
                }
            else:
                return {
                    "id_cliente": None,
                    "nombre_cliente": "Cliente Desconocido"
                }
        except Exception as e:
            print(f"⚠️ Error obteniendo info del cliente: {str(e)}")
            return {
                "id_cliente": None,
                "nombre_cliente": "Cliente Desconocido"
            }
        finally:
            cursor.close()
            conn.close()

    def save_with_transaction_and_validations(
        self,
        rows: List[BalanceGeneralRow],
        fecha: str,
        identificacion_cliente: str
    ) -> dict:
        """
        Guarda los datos en una transacción completa.
        Ejecuta validaciones y hace ROLLBACK si no pasan.
        Solo hace COMMIT si todas las validaciones son exitosas.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Obtener id_cliente / nombre_cliente para consultas por IdCliente
            cliente_info = self.get_cliente_info(identificacion_cliente)
            id_cliente = cliente_info.get("id_cliente")
            nombre_cliente = cliente_info.get("nombre_cliente")
            
            # INICIAR TRANSACCIÓN EXPLÍCITA
            cursor.execute("BEGIN TRANSACTION")
            print(f"🔄 Transacción iniciada...")
            
            rows_inserted = 0
            errors = []
            
            # 1. INSERTAR TODOS LOS REGISTROS
            print(f"📥 Insertando {len(rows)} registros...")
            
            for idx, row in enumerate(rows):
                try:
                    
                    cursor.execute("""
                        EXEC [dbo].[BalanceGeneralInsertar] 
                            @Nivel = ?,
                            @Transaccional = ?,
                            @CodigoCuentaContable = ?,
                            @NombreCuentaContable = ?,
                            @Identificacion = ?,
                            @Sucursal = ?,
                            @NombreTercero = ?,
                            @SaldoInicial = ?,
                            @MovimientoDebito = ?,
                            @MovimientoCredito = ?,
                            @SaldoFinal = ?,
                            @Fecha = ?,
                            @IdentificacionCliente = ?
                    """, (
                        row.nivel,
                        row.transaccional,
                        row.codigo_cuenta_contable,
                        row.nombre_cuenta_contable,
                        row.identificacion or '',
                        row.sucursal or '',
                        row.nombre_tercero or '',
                        float(row.saldo_inicial),
                        float(row.movimiento_debito),
                        float(row.movimiento_credito),
                        float(row.saldo_final),
                        fecha,
                        identificacion_cliente
                    ))
                    rows_inserted += 1
                    
                    if (idx + 1) % 100 == 0:
                        print(f"   ✓ {idx + 1}/{len(rows)} registros insertados...")
                
                except Exception as e:
                    error_msg = f"Error insertando fila {idx + 8}: {str(e)}"
                    errors.append(error_msg)
                    print(f"   ❌ {error_msg}")
                    cursor.execute("ROLLBACK TRANSACTION")
                    print(f"🔙 ROLLBACK ejecutado - Error en inserción")
                    return {
                        "success": False,
                        "message": f"Error insertando datos: {error_msg}",
                        "rows_inserted": 0,
                        "errors": errors
                    }
            
            print(f"✅ {rows_inserted} registros insertados en transacción")
            
            # 2. OBTENER TOTALES PARA VALIDACIÓN (USANDO EL MISMO CURSOR para ver datos no confirmados)
            print(f"🔍 Obteniendo totales para validación (misma transacción)...")
            
            # Usamos las funciones existentes pasando el cursor para que ejecuten sus queries
            if id_cliente is None:
                # Si no encontramos id_cliente en tabla Clientes, no podemos continuar con consultas por IdCliente
                cursor.execute("ROLLBACK TRANSACTION")
                return {
                    "success": False,
                    "message": "No se encontró IdCliente para la identificación proporcionada",
                    "rows_inserted": 0,
                    "errors": ["Cliente no encontrado"]
                }
            
            # Totales Generales (retorna TotalesGenerales dataclass)
            totales_generales_obj = self.get_totales_generales(fecha, id_cliente, cursor=cursor)
            totales_generales = {
                "total_registros": totales_generales_obj.total_registros,
                "suma_saldo_inicial": totales_generales_obj.suma_saldo_inicial,
                "suma_debito": totales_generales_obj.suma_debito,
                "suma_credito": totales_generales_obj.suma_credito,
                "suma_saldo_final": totales_generales_obj.suma_saldo_final
            }
            
            print(f"   Registros: {totales_generales['total_registros']}")
            print(f"   Saldo Inicial: ${totales_generales['suma_saldo_inicial']:,.2f}")
            print(f"   Débito: ${totales_generales['suma_debito']:,.2f}")
            print(f"   Crédito: ${totales_generales['suma_credito']:,.2f}")
            
            # Totales por Clase
            totales_clase_obj = self.get_totales_por_clase(fecha, id_cliente, cursor=cursor)
            totales_clase = {
                "total_clase_1": totales_clase_obj.total_clase_1,
                "total_clase_2": totales_clase_obj.total_clase_2,
                "total_clase_3": totales_clase_obj.total_clase_3,
                "total_clase_4": totales_clase_obj.total_clase_4,
                "total_clase_5": totales_clase_obj.total_clase_5
            }
            
            print(f"   Activos: ${totales_clase['total_clase_1']:,.2f}")
            print(f"   Pasivos: ${totales_clase['total_clase_2']:,.2f}")
            print(f"   Patrimonio: ${totales_clase['total_clase_3']:,.2f}")
            
            # Ecuación Contable
            ecuacion_obj = self.get_ecuacion_contable(fecha, id_cliente, cursor=cursor)
            diferencia_ecuacion = ecuacion_obj.diferencia_ecuacion_contable
            
            ecuacion = {
                "activos": ecuacion_obj.activos,
                "pasivos": ecuacion_obj.pasivos,
                "patrimonio": ecuacion_obj.patrimonio,
                "diferencia_ecuacion_contable": diferencia_ecuacion
            }
            
            print(f"   Diferencia Ecuación Contable: ${diferencia_ecuacion:,.2f}")

            # Contar errores individuales
            errores_ecuacion_list = self.get_errores_ecuacion(fecha, id_cliente, cursor=cursor)
            errores_ecuacion_count = len(errores_ecuacion_list)
            print(f"   Errores ecuación individual: {errores_ecuacion_count}")
            
            # 3. VALIDACIONES CRÍTICAS
            print(f"🔍 Ejecutando validaciones...")
            
            validation_errors = []
            
            # Validación: Debe haber registros insertados (según conteo por IdCliente)
            if totales_generales["total_registros"] == 0:
                validation_errors.append("No se insertaron registros")
            
            # Agrega otras validaciones si es necesario...
            
            # Si hay errores de validación, hacer ROLLBACK
            if validation_errors:
                cursor.execute("ROLLBACK TRANSACTION")
                print(f"🔙 ROLLBACK ejecutado - Validaciones fallaron:")
                for error in validation_errors:
                    print(f"   ❌ {error}")
                
                return {
                    "success": False,
                    "message": "; ".join(validation_errors),
                    "rows_inserted": 0,
                    "errors": validation_errors,
                    "totales_generales": totales_generales,
                    "totales_clase": totales_clase,
                    "ecuacion": ecuacion,
                    "errores_ecuacion_count": errores_ecuacion_count,
                    "diferencia_ecuacion": diferencia_ecuacion
                }
            
            # 4. TODAS LAS VALIDACIONES PASARON - HACER COMMIT
            cursor.execute("COMMIT TRANSACTION")
            conn.commit()
            print(f" COMMIT ejecutado - Todos los datos guardados permanentemente")
            print(f" Transacción completada exitosamente")
            

            
            return {
                "success": True,
                "message": "Datos guardados y validados correctamente",
                "rows_inserted": rows_inserted,
                "totales_generales": totales_generales,
                "totales_clase": totales_clase,
                "ecuacion": ecuacion,
                "errores_ecuacion_count": errores_ecuacion_count,
                "errors": []
            }
        
        except Exception as e:
            # Error inesperado - ROLLBACK
            try:
                cursor.execute("ROLLBACK TRANSACTION")
                print(f"🔙 ROLLBACK ejecutado - Error inesperado: {str(e)}")
            except:
                pass
            print(f"🔙 ROLLBACK ejecutado - Error inesperado: {str(e)}")
            
            return {
                "success": False,
                "message": f"Error en transacción: {str(e)}",
                "rows_inserted": 0,
                "errors": [str(e)]
            }
        
        finally:
            cursor.close()
            conn.close()
