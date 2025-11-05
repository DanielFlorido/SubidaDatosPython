from typing import Any, Dict, Optional
from app.models.schemas import JobStatusResponse, JobStatus
import pyodbc
from app.config import settings
from app.utils.logger import app_logger, log_database_connection
from decimal import Decimal
from datetime import datetime
import json
class DatabaseRepository:
    def __init__(self):        
        self.connection_string = (
            f"DRIVER={{{settings.db_driver}}};"
            f"SERVER={settings.db_server},{settings.db_port};"
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
            app_logger.info(f"Estableciendo conexión a BD: {settings.db_server}/{settings.db_database}/{settings.db_username}/{settings.db_driver}/{settings.db_port}")
            app_logger.info(f"Connection String: {self.connection_string}")
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
            print(f" Error obteniendo info del cliente: {str(e)}")
            app_logger.error(f"Error obteniendo info del cliente: {str(e)}", exc_info=True)
            return {
                "id_cliente": None,
                "nombre_cliente": "Cliente Desconocido"
            }
        finally:
            cursor.close()
            conn.close()

    def insert_or_update_job_history(self, job_data: Dict[str, Any]) -> None:
        """
        Guarda o actualiza el log del trabajo asíncrono en la base de datos,
        usando el stored procedure [dbo].[JobHistoryInsertOrUpdate].

        :param job_data: Diccionario con los datos del job.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                EXEC [dbo].[JobHistoryInsertOrUpdate]
                    @JobId = ?,
                    @Status = ?,
                    @Message = ?,
                    @Progress = ?,
                    @TotalRows = ?,
                    @ProcessedRows = ?,
                    @Errors = ?,
                    @Result = ?,
                    @CreatedAt = ?,
                    @UpdatedAt = ?,
                    @StartedAt = ?,
                    @CompletedAt = ?;
            """, (
                job_data.get('job_id'),
                job_data.get('status'),
                job_data.get('message'),
                job_data.get('progress'),
                job_data.get('total_rows'),
                job_data.get('processed_rows'),
                str(job_data.get('errors')) if job_data.get('errors') else None,
                str(job_data.get('result')) if job_data.get('result') else None,
                job_data.get('created_at'),
                job_data.get('updated_at'),
                job_data.get('started_at'),
                job_data.get('completed_at')
            ))

            conn.commit()
            app_logger.info(f" JobHistory actualizado correctamente para JobId: {job_data.get('job_id')}")
        
        except Exception as e:
            conn.rollback()
            app_logger.error(f" Error al insertar/actualizar JobHistory: {str(e)}")
            raise e
        
        finally:
            cursor.close()
            conn.close()
    
    def get_job_history(self, job_id: str) -> Optional[JobStatusResponse]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT JobId, Status, Message, Progress, TotalRows, ProcessedRows, Errors, Result,
                    CreatedAt, UpdatedAt, StartedAt, CompletedAt
                FROM JobHistory
                WHERE JobId = ?
            """, job_id)
            
            row = cursor.fetchone()
            if not row:
                return None
            
            return JobStatusResponse(
                job_id=row[0],
                status=JobStatus(row[1]),
                message=row[2],
                progress=row[3] or 0,
                total_rows=row[4] or 0,
                processed_rows=row[5] or 0,
                errors=json.loads(row[6]) if row[6] else [],
                result=json.loads(row[7]) if row[7] else None,
                created_at=row[8].isoformat() if isinstance(row[8], datetime) else row[8],
                updated_at=row[9].isoformat() if isinstance(row[9], datetime) else row[9],
                started_at=row[10].isoformat() if isinstance(row[10], datetime) else row[10],
                completed_at=row[11].isoformat() if isinstance(row[11], datetime) else row[11]
            )
        except Exception as e:
            raise e