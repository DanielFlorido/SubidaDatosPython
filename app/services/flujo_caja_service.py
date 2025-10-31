import pandas as pd
from typing import List, Dict, Tuple
from datetime import datetime
from app.repositories.flujo_caja_repository import FlujoCajaRepository
from app.models.schemas import JobStatus
from app.utils.job_manager import job_manager
from app.utils.logger import app_logger

class FlujoCajaService:
    def __init__(self):
        self.repository = FlujoCajaRepository()
    
    def _es_fila_encabezado(self, fila: pd.Series) -> bool:
        """
        Determina si una fila es un encabezado o un detalle.
        Una fila es encabezado si no tiene Comprobante ni Secuencia.
        """
        comprobante = str(fila.get('Comprobante', '')).strip()
        secuencia = str(fila.get('Secuencia', '')).strip()
        
        return not comprobante and not secuencia
    
    def _limpiar_valor_numerico(self, valor) -> float:
        """
        Limpia y convierte un valor a float, manejando diferentes formatos.
        """
        if pd.isna(valor) or valor == '':
            return 0.0
        
        # Si ya es numérico
        if isinstance(valor, (int, float)):
            return float(valor)
        
        # Si es string, limpiar
        valor_str = str(valor).strip()
        # Remover separadores de miles y reemplazar coma decimal por punto
        valor_str = valor_str.replace(',', '')
        
        try:
            return float(valor_str)
        except ValueError:
            return 0.0
    
    def _limpiar_fecha(self, fecha) -> str:
        """
        Limpia y formatea una fecha al formato esperado por SQL Server (YYYY-MM-DD).
        """
        if pd.isna(fecha) or fecha == '':
            return ''
        
        # Si es string, intentar parsear
        if isinstance(fecha, str):
            try:
                # Intentar formato DD/MM/YYYY
                fecha_obj = datetime.strptime(fecha.strip(), '%d/%m/%Y')
                return fecha_obj.strftime('%Y-%m-%d')
            except ValueError:
                try:
                    # Intentar otros formatos comunes
                    fecha_obj = datetime.strptime(fecha.strip(), '%Y-%m-%d')
                    return fecha_obj.strftime('%Y-%m-%d')
                except ValueError:
                    return ''
        
        # Si es datetime
        if isinstance(fecha, datetime):
            return fecha.strftime('%Y-%m-%d')
        
        return ''
    
    def _procesar_encabezado(self, fila: pd.Series) -> Dict:
        """
        Procesa una fila de encabezado y retorna un diccionario con los datos.
        """
        return {
            'codigo_contable': str(fila['Código contable']).strip(),
            'cuenta_contable': str(fila['Cuenta contable']).strip(),
            'saldo_inicial': self._limpiar_valor_numerico(fila.get('Saldo inicial', 0)),
            'debito': self._limpiar_valor_numerico(fila.get('Débito', 0)),
            'credito': self._limpiar_valor_numerico(fila.get('Crédito', 0)),
            'saldo_total_cuenta': self._limpiar_valor_numerico(fila.get('Saldo total cuenta', 0))
        }
    
    def _procesar_detalle(self, fila: pd.Series) -> Dict:
        """
        Procesa una fila de detalle y retorna un diccionario con los datos.
        """
        return {
            'codigo_contable': str(fila['Código contable']).strip(),
            'cuenta_contable': str(fila['Cuenta contable']).strip(),
            'comprobante': str(fila.get('Comprobante', '')).strip(),
            'secuencia': str(fila.get('Secuencia', '')).strip(),
            'fecha_elaboracion': self._limpiar_fecha(fila.get('Fecha elaboración', '')),
            'identificacion': str(fila.get('Identificación', '')).strip(),
            'suc': str(fila.get('Suc', '')).strip(),
            'nombre_tercero': str(fila.get('Nombre del tercero', '')).strip(),
            'descripcion': str(fila.get('Descripción', '')).strip(),
            'detalle': str(fila.get('Detalle', '')).strip(),
            'centro_costo': str(fila.get('Centro de costo', '')).strip(),
            'debito': self._limpiar_valor_numerico(fila.get('Débito', 0)),
            'credito': self._limpiar_valor_numerico(fila.get('Crédito', 0)),
            'saldo_movimiento': self._limpiar_valor_numerico(fila.get('Saldo Movimiento', 0))
        }
    
    def procesar_excel_secuencial(self, archivo_excel: str) -> List[Dict]:
        """
        Lee y procesa el archivo Excel SECUENCIALMENTE.
        Retorna una lista de grupos donde cada grupo tiene:
        {
            'encabezado': {...},
            'detalles': [...]
        }
        
        IMPORTANTE: Este método aprovecha que el Excel viene ordenado:
        1. Encabezado
        2. Sus detalles
        3. Siguiente encabezado
        4. Sus detalles
        ...
        
        El procesamiento se detiene cuando encuentra la PRIMERA fila completamente vacía.
        """
        try:
            # Leer el archivo Excel
            df = pd.read_excel(archivo_excel)
            
            grupos = []  # Lista de grupos {encabezado, detalles}
            grupo_actual = None
            
            # Procesar cada fila EN ORDEN
            for idx, fila in df.iterrows():
                # Si encontramos una fila completamente vacía, TERMINAR procesamiento
                if fila.isna().all():
                    app_logger.info(f"Fila {idx + 2} completamente vacía detectada. Finalizando procesamiento.")
                    break
                
                if self._es_fila_encabezado(fila):
                    # Si ya había un grupo en proceso, guardarlo
                    if grupo_actual is not None:
                        grupos.append(grupo_actual)
                    
                    # Iniciar nuevo grupo con este encabezado
                    grupo_actual = {
                        'encabezado': self._procesar_encabezado(fila),
                        'detalles': []
                    }
                else:
                    # Es un detalle, agregarlo al grupo actual
                    if grupo_actual is None:
                        raise Exception(f"Se encontró un detalle (fila {idx + 2}) sin encabezado previo")
                    
                    grupo_actual['detalles'].append(self._procesar_detalle(fila))
            
            # No olvidar el último grupo
            if grupo_actual is not None:
                grupos.append(grupo_actual)
            
            return grupos
            
        except Exception as e:
            raise Exception(f"Error al procesar el archivo Excel: {str(e)}")
    
    def validar_grupos(self, grupos: List[Dict]) -> Tuple[bool, str]:
        """
        Valida los grupos de datos antes de subirlos a la base de datos.
        """
        if not grupos:
            return False, "No se encontraron datos en el archivo"
        
        for idx, grupo in enumerate(grupos, 1):
            encabezado = grupo['encabezado']
            detalles = grupo['detalles']
            
            # Validar que el encabezado tenga código contable
            if not encabezado['codigo_contable']:
                return False, f"El grupo {idx} no tiene código contable en el encabezado"
            
            # Validar que tenga al menos un detalle
            if not detalles:
                codigo = encabezado['codigo_contable']
                return False, f"El encabezado con código {codigo} (grupo {idx}) no tiene detalles"
            
            # Validar que los detalles tengan datos mínimos
            for det_idx, detalle in enumerate(detalles, 1):
                if not detalle['codigo_contable']:
                    return False, f"El detalle {det_idx} del grupo {idx} no tiene código contable"
        
        return True, "Validación exitosa"
    
    def subir_flujo_caja(
        self,
        archivo_excel,
        fecha_movimiento: str,
        numero_identificacion: str
    ) -> Tuple[bool, str, List[int]]:
        """
        Procesa y sube el flujo de caja desde un archivo Excel.
        PROCESA SECUENCIALMENTE aprovechando el orden del Excel.
        
        Args:
            archivo_excel: Archivo Excel (ruta o bytes)
            fecha_movimiento: Fecha del movimiento (formato YYYY-MM-DD)
            numero_identificacion: Número de identificación
        
        Returns:
            Tuple (éxito, mensaje, lista_ids_encabezados)
        """
        try:
            # Procesar el archivo Excel SECUENCIALMENTE
            grupos = self.procesar_excel_secuencial(archivo_excel)
            
            # Validar los grupos
            es_valido, mensaje_validacion = self.validar_grupos(grupos)
            if not es_valido:
                return False, mensaje_validacion, []
            
            # Subir los datos a la base de datos
            exito, mensaje, ids = self.repository.subir_flujo_caja_secuencial(
                grupos=grupos,
                fecha_movimiento=fecha_movimiento,
                numero_identificacion=numero_identificacion
            )
            
            return exito, mensaje, ids or []
            
        except Exception as e:
            return False, f"Error en el servicio: {str(e)}", []
    
    def process_and_save_async(
        self,
        file_path: str,
        identificacion_cliente: str,
        fecha: str,
        job_id: str
    ):
        """
        Procesa y guarda el flujo de caja de forma asíncrona con actualización de progreso.
        
        Args:
            file_path: Ruta del archivo Excel
            identificacion_cliente: Identificación del cliente
            fecha: Fecha en formato YYYYMMDD
            job_id: ID del trabajo para tracking
        """
        try:
            app_logger.info(f"[Job {job_id}] Iniciando procesamiento de flujo de caja")
            
            # Actualizar estado: Leyendo archivo
            job_manager.update_job(
                job_id,
                status=JobStatus.PROCESSING,
                message="Leyendo archivo Excel...",
                progress=10
            )
            
            # Convertir fecha de YYYYMMDD a YYYY-MM-DD
            fecha_formateada = f"{fecha[:4]}-{fecha[4:6]}-{fecha[6:]}"
            
            # Procesar Excel secuencialmente
            app_logger.info(f"[Job {job_id}] Procesando Excel...")
            grupos = self.procesar_excel_secuencial(file_path)
            
            job_manager.update_job(
                job_id,
                message=f"Archivo procesado: {len(grupos)} grupos encontrados",
                progress=30
            )
            
            # Validar grupos
            app_logger.info(f"[Job {job_id}] Validando datos...")
            es_valido, mensaje_validacion = self.validar_grupos(grupos)
            
            if not es_valido:
                app_logger.error(f"[Job {job_id}] Validación fallida: {mensaje_validacion}")
                job_manager.update_job(
                    job_id,
                    status=JobStatus.FAILED,
                    message=f"Validación fallida: {mensaje_validacion}",
                    progress=100,
                    errors=[mensaje_validacion]
                )
                return
            
            job_manager.update_job(
                job_id,
                message="Validación exitosa. Guardando en base de datos...",
                progress=50
            )
            
            # Guardar en base de datos
            app_logger.info(f"[Job {job_id}] Guardando en base de datos...")
            exito, mensaje, ids_encabezados = self.repository.subir_flujo_caja_secuencial(
                grupos=grupos,
                fecha_movimiento=fecha_formateada,
                numero_identificacion=identificacion_cliente
            )
            
            if exito:
                app_logger.info(f"[Job {job_id}] Procesamiento exitoso. IDs: {ids_encabezados}")
                
                # Calcular totales para el resultado
                total_detalles = sum(len(grupo['detalles']) for grupo in grupos)
                total_debitos = sum(
                    sum(d['debito'] for d in grupo['detalles'])
                    for grupo in grupos
                )
                total_creditos = sum(
                    sum(d['credito'] for d in grupo['detalles'])
                    for grupo in grupos
                )
                
                job_manager.update_job(
                    job_id,
                    status=JobStatus.COMPLETED,
                    message=mensaje,
                    progress=100,
                    result={
                        "ids_encabezados": ids_encabezados,
                        "total_encabezados": len(ids_encabezados),
                        "total_detalles": total_detalles,
                        "total_debitos": total_debitos,
                        "total_creditos": total_creditos,
                        "fecha_movimiento": fecha_formateada,
                        "identificacion_cliente": identificacion_cliente
                    }
                )
            else:
                app_logger.error(f"[Job {job_id}] Error al guardar: {mensaje}")
                job_manager.update_job(
                    job_id,
                    status=JobStatus.FAILED,
                    message=f"Error al guardar en base de datos: {mensaje}",
                    progress=100,
                    errors=[mensaje]
                )
        
        except Exception as e:
            app_logger.error(f"[Job {job_id}] Error inesperado: {str(e)}", exc_info=True)
            job_manager.update_job(
                job_id,
                status=JobStatus.FAILED,
                message=f"Error inesperado: {str(e)}",
                progress=100,
                errors=[str(e)]
            )