import pyodbc
from app.config import settings
from app.utils.logger import app_logger, log_database_connection
from decimal import Decimal

class DatabaseRepository:
    def __init__(self):        
        self.connection_string = (
                f"DRIVER={{{settings.db_driver}}};"
                f"SERVER={settings.db_server};"
                f"PORT={settings.db_port};"
                f"DATABASE={settings.db_database};"
                f"UID={settings.db_username};"
                f"PWD={settings.db_password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
        app_logger.info(f"DatabaseRepository initialized with server: {settings.db_server}, database: {settings.db_database}")
    def get_connection(self):
        try:
            app_logger.info("Intentando conectar a la base de datos...")
            conn = pyodbc.connect(self.connection_string)
            app_logger.info("Conexión establecida exitosamente")
            return conn
        except Exception as e:
            app_logger.error(f"Error al conectar a BD: {str(e)}", exc_info=True)
            raise

    def test_connection(self):
        try:
            app_logger.info("Probando conexión a base de datos...")
            conn = self.get_connection()

            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            log_database_connection(True)
            app_logger.info(f"BD Version: {version[:100]}")
            return {"success": True, "message": "Conexión exitosa"}
        
        except pyodbc.Error as e:
            error_info = {
                "success": False,
                "error_code": e.args[0] if e.args else "Unknown",
                "error_message": e.args[1] if len(e.args) > 1 else str(e),
                "type": "DatabaseError",
                "server": settings.db_server,
                "database": settings.db_database,
                "driver": settings.db_driver
            }
            log_database_connection(False, error_info)
            app_logger.error(f"Error de pyodbc: {error_info}", exc_info=True)
            return error_info
        except Exception as e:
            error_info = {
                "success": False,
                "error": str(e),
                "type": type(e).__name__,
                "server": settings.db_server,
                "database": settings.db_database
            }
            log_database_connection(False, error_info)
            app_logger.error(f"Error general: {error_info}", exc_info=True)
            return error_info
    
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
            conn.commit()
            app_logger.info({
                "action": "insert_log_carga",
                "fecha_carga": fecha_carga,
                "id_cliente": id_cliente,
                "nombre_cliente": nombre_cliente,
                "total_registros": total_registros,
                "total_activos": float(total_activos),
                "total_pasivos": float(total_pasivos),
                "total_patrimonio": float(total_patrimonio),
                "total_ingresos": float(total_ingresos),
                "total_gastos": float(total_gastos),
                "suma_saldo_inicial": float(suma_saldo_inicial),
                "suma_debito": float(suma_debito),
                "suma_credito": float(suma_credito),
                "observaciones": observaciones,
                "archivo_origen": archivo_origen,
                "cantidad_errores_jerarquia": cantidad_errores_jerarquia,
                "diferencia_ecuacion": float(diferencia_ecuacion_contable),
                "estado": estado,
                "tiempo_ejecucion": tiempo_ejecucion
            })

            return True
        except Exception as e:
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