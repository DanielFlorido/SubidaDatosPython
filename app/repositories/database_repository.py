import pyodbc
from typing import List
from decimal import Decimal
from app.config import settings
from app.models.schemas import BalanceGeneralRow

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
    
    def insert_balance_general_bulk(
        self, 
        rows: List[BalanceGeneralRow], 
        fecha: str, 
        identificacion_cliente: str
    ) -> dict:
        """Inserta múltiples filas en transacción"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        successful = 0
        failed = 0
        errors = []
        
        try:
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
                    successful += 1
                
                except Exception as e:
                    failed += 1
                    errors.append(f"Fila {idx + 8}: {str(e)}")
                    # Continúa con la siguiente fila
            
            conn.commit()
            
            return {
                "successful": successful,
                "failed": failed,
                "errors": errors
            }
        
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
