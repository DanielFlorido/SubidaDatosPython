import pyodbc
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from app.config import settings
from app.repositories.database_repository import DatabaseRepository
from app.utils.logger import app_logger
class FlujoCajaRepository(DatabaseRepository):
    def __init__(self):
        super().__init__()
    
    def insertar_encabezado(
        self,
        conn: pyodbc.Connection,
        codigo_contable: str,
        saldo_inicial: float,
        debito: float,
        credito: float,
        saldo_total_cuenta: float,
        fecha_movimiento: str,
        numero_identificacion: str
    ) -> int:
        """
        Inserta el encabezado del flujo de caja y retorna el ID generado.
        """
        cursor = conn.cursor()
        try:
            query = """
            EXEC [dbo].[EncabezadoFlujoCajaInsertar]
                @CodigoContable = ?,
                @FechaElaboracion = NULL,
                @SaldoInicial = ?,
                @Debito = ?,
                @Credito = ?,
                @SaldoTotalCuenta = ?,
                @FechaMovimiento = ?,
                @NumeroIdentificacion = ?;
            """
            
            cursor.execute(query, (
                codigo_contable,
                saldo_inicial,
                debito,
                credito,
                saldo_total_cuenta,
                fecha_movimiento,
                numero_identificacion
            ))
            
            # Obtener el ID generado
            result = cursor.fetchone()
            id_encabezado = result[0] if result else None
            app_logger.debug(f"Encabezado insertado con ID: {id_encabezado}")
            if not id_encabezado:
                app_logger.error("No se pudo obtener el ID del encabezado después de la inserción.")
                raise Exception("No se pudo obtener el ID del encabezado")
            
            return id_encabezado
            
        except Exception as e:
            app_logger.error(f"Error al insertar encabezado: {str(e)}")
            raise Exception(f"Error al insertar encabezado: {str(e)}")
        finally:
            cursor.close()
    
    def insertar_detalle(
        self,
        conn: pyodbc.Connection,
        codigo_contable: str,
        cuenta_contable: str,
        comprobante: str,
        secuencia: str,
        fecha_elaboracion: str,
        identificacion: str,
        suc: str,
        nombre_tercero: str,
        descripcion: str,
        detalle: str,
        centro_costo: str,
        debito: float,
        credito: float,
        saldo_movimiento: float,
        id_encabezado: int
    ) -> None:
        """
        Inserta el detalle del flujo de caja.
        """
        cursor = conn.cursor()
        try:
            query = """
            EXEC [dbo].[FlujoCajaInsertar]
                @CodigoContable = ?,
                @CuentaContable = ?,
                @Comprobante = ?,
                @Secuencia = ?,
                @FechaElaboracion = ?,
                @Identificacion = ?,
                @Suc = ?,
                @NombreTercero = ?,
                @Descripcion = ?,
                @Detalle = ?,
                @CentroCosto = ?,
                @SaldoInicial = NULL,
                @Debito = ?,
                @Credito = ?,
                @SaldoMovimiento = ?,
                @IdEncabezadoFlujoCaja = ?;
            """
            
            cursor.execute(query, (
                codigo_contable,
                cuenta_contable,
                comprobante,
                secuencia,
                fecha_elaboracion,
                identificacion,
                suc,
                nombre_tercero,
                descripcion,
                detalle,
                centro_costo,
                debito,
                credito,
                saldo_movimiento,
                id_encabezado
            ))
            app_logger.debug(f"Detalle insertado para encabezado ID: {id_encabezado}, Código Contable: {codigo_contable}")
        except Exception as e:
            app_logger.error(f"Error al insertar detalle: {str(e)}")
            raise Exception(f"Error al insertar detalle: {str(e)}")
        finally:
            cursor.close()
    
    def validar_saldos(
        self,
        conn: pyodbc.Connection,
        id_encabezado: int
    ) -> Tuple[bool, str]:
        """
        Valida que los saldos del encabezado coincidan con la suma de los detalles.
        Retorna (es_valido, mensaje).
        """
        cursor = conn.cursor()
        try:
            query = """
            SELECT 
                e.Debito as Debito_Encabezado,
                e.Credito as Credito_Encabezado,
                ISNULL(SUM(d.Debito), 0) as Debito_Detalles,
                ISNULL(SUM(d.Credito), 0) as Credito_Detalles
            FROM EncabezadoFlujoCaja e
            LEFT JOIN FlujoCaja d ON e.Id = d.IdEncabezadoFlujoCaja
            WHERE e.Id = ?
            GROUP BY e.Debito, e.Credito;
            """
            
            cursor.execute(query, (id_encabezado,))
            app_logger.info(f"Validando saldos para encabezado ID: {id_encabezado}")
            result = cursor.fetchone()
            
            if not result:
                app_logger.error("No se encontró el encabezado para validar.")
                return False, "No se encontró el encabezado para validar"
            
            debito_enc = float(result[0] or 0)
            credito_enc = float(result[1] or 0)
            debito_det = float(result[2] or 0)
            credito_det = float(result[3] or 0)
            
            # Validar con tolerancia de 0.01 por redondeos
            tolerancia = 0.01
            
            if abs(debito_enc - debito_det) > tolerancia:
                app_logger.error(f"Diferencia en débitos: Encabezado {debito_enc} vs Detalles {debito_det}")
                return False, f"Los débitos no coinciden. Encabezado: {debito_enc}, Detalles: {debito_det}"
            
            if abs(credito_enc - credito_det) > tolerancia:
                app_logger.error(f"Diferencia en créditos: Encabezado {credito_enc} vs Detalles {credito_det}")
                return False, f"Los créditos no coinciden. Encabezado: {credito_enc}, Detalles: {credito_det}"
            
            app_logger.info(f"Saldos validados correctamente para encabezado ID: {id_encabezado}")
            return True, "Validación exitosa"
            
        except Exception as e:
            app_logger.error(f"Error al validar saldos: {str(e)}")
            return False, f"Error al validar saldos: {str(e)}"
        finally:
            cursor.close()
    
    def subir_flujo_caja_secuencial(
        self,
        grupos: List[Dict],
        fecha_movimiento: str,
        numero_identificacion: str
    ) -> Tuple[bool, str, Optional[List[int]]]:
        """
        Sube los datos de flujo de caja a la base de datos con transacción.
        PROCESA SECUENCIALMENTE: cada grupo tiene su encabezado y sus detalles.
        
        Args:
            grupos: Lista de diccionarios con estructura:
                    {
                        'encabezado': {...datos del encabezado...},
                        'detalles': [{...detalle1...}, {...detalle2...}, ...]
                    }
            fecha_movimiento: Fecha del movimiento
            numero_identificacion: Número de identificación
        
        Returns:
            Tuple (éxito, mensaje, lista_ids_encabezados)
        """
        conn = None
        ids_encabezados = []
        
        try:
            conn = self.get_connection()
            conn.autocommit = False  # Iniciar transacción
            app_logger.info("Iniciando subida de flujo de caja secuencialmente.")
            # Procesar cada grupo (encabezado + sus detalles) EN ORDEN
            for idx, grupo in enumerate(grupos, 1):
                encabezado = grupo['encabezado']
                detalles = grupo['detalles']
                
                # 1. Insertar el encabezado y obtener su ID
                id_encabezado = self.insertar_encabezado(
                    conn=conn,
                    codigo_contable=encabezado['codigo_contable'],
                    saldo_inicial=encabezado['saldo_inicial'],
                    debito=encabezado['debito'],
                    credito=encabezado['credito'],
                    saldo_total_cuenta=encabezado['saldo_total_cuenta'],
                    fecha_movimiento=fecha_movimiento,
                    numero_identificacion=numero_identificacion
                )
                
                ids_encabezados.append(id_encabezado)
                app_logger.info(f"Encabezado {idx} insertado con ID: {id_encabezado}")
                # 2. Insertar TODOS los detalles de este encabezado
                for detalle in detalles:
                    self.insertar_detalle(
                        conn=conn,
                        codigo_contable=detalle['codigo_contable'],
                        cuenta_contable=detalle['cuenta_contable'],
                        comprobante=detalle.get('comprobante', ''),
                        secuencia=detalle.get('secuencia', ''),
                        fecha_elaboracion=detalle.get('fecha_elaboracion', ''),
                        identificacion=detalle.get('identificacion', ''),
                        suc=detalle.get('suc', ''),
                        nombre_tercero=detalle.get('nombre_tercero', ''),
                        descripcion=detalle.get('descripcion', ''),
                        detalle=detalle.get('detalle', ''),
                        centro_costo=detalle.get('centro_costo', ''),
                        debito=detalle['debito'],
                        credito=detalle['credito'],
                        saldo_movimiento=detalle.get('saldo_movimiento', 0),
                        id_encabezado=id_encabezado
                    )
                
                es_valido, mensaje_validacion = self.validar_saldos(conn, id_encabezado)
                app_logger.info(f"Validación de saldos para grupo {idx} (ID: {id_encabezado}): {mensaje_validacion}")
                if not es_valido:
                    app_logger.error(f"Validación fallida para grupo {idx}: {mensaje_validacion}")
                    conn.rollback()
                    return False, f"Validación fallida en grupo {idx} (código: {encabezado['codigo_contable']}): {mensaje_validacion}", None
            
            conn.commit()
            app_logger.info("Subida de flujo de caja completada exitosamente.")
            return True, f"Flujo de caja subido exitosamente. {len(ids_encabezados)} encabezados procesados.", ids_encabezados
            
        except Exception as e:
            if conn:
                app_logger.error(f"Error al subir flujo de caja: {str(e)}. Realizando rollback.")
                conn.rollback()
            return False, f"Error al subir flujo de caja: {str(e)}", None
            
        finally:
            if conn:
                conn.close()