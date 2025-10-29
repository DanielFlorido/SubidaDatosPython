from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from typing import Optional
from datetime import datetime
import os
import shutil
from app.utils.logger import app_logger
router = APIRouter(prefix="/api/logs", tags=["Logs y Trazabilidad"])

@router.get("/list")
async def list_logs():
    """
    Lista todos los archivos de log disponibles
    
    Retorna información sobre cada archivo de log:
    - Nombre del archivo
    - Tamaño en KB
    - Fecha de última modificación
    """
    try:
        log_dir = "logs"
        if not os.path.exists(log_dir):
            app_logger.info("No hay directorio de logs disponible.")
            return {"logs": [], "message": "No hay logs disponibles"}
        
        log_files = []
        for filename in os.listdir(log_dir):
            if filename.endswith('.log'):
                filepath = os.path.join(log_dir, filename)
                file_stats = os.stat(filepath)
                log_files.append({
                    "name": filename,
                    "size_kb": round(file_stats.st_size / 1024, 2),
                    "modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                    "size_mb": round(file_stats.st_size / (1024 * 1024), 2)
                })
        
        log_files.sort(key=lambda x: x["modified"], reverse=True)
        app_logger.info(f"Listado de {len(log_files)} archivos de log obtenido.")
        return {
            "logs": log_files,
            "total": len(log_files),
            "log_directory": os.path.abspath(log_dir)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando logs: {str(e)}")

@router.get("/view/{log_name}")
async def view_log(
    log_name: str,
    lines: Optional[int] = Query(100, description="Número de líneas a mostrar (últimas)", ge=1, le=10000),
    search: Optional[str] = Query(None, description="Buscar texto específico en el log"),
    level: Optional[str] = Query(None, description="Filtrar por nivel (INFO, ERROR, WARNING, DEBUG)")
):
    """
    Visualiza el contenido de un archivo de log
    
    - **log_name**: Nombre del archivo (application.log, errors.log, etc.)
    - **lines**: Número de líneas a mostrar (1-10000, por defecto 100 últimas)
    - **search**: Filtrar líneas que contengan este texto
    - **level**: Filtrar por nivel de log (INFO, ERROR, WARNING, DEBUG)
    
    Ejemplo: `/api/logs/view/application.log?lines=200&search=error&level=ERROR`
    """
    try:
        log_dir = "logs"
        filepath = os.path.join(log_dir, log_name)
        app_logger.info(f"Visualizando log '{log_name}' con filtros - lines: {lines}, search: {search}, level: {level}")
        # Validar seguridad: archivo debe estar en directorio de logs
        if not os.path.exists(filepath):
            raise HTTPException(status_code=404, detail=f"Log '{log_name}' no encontrado")
        
        # Validar que es un archivo .log
        if not log_name.endswith('.log'):
            app_logger.warning(f"Intento de acceso a archivo no log: {log_name}")
            raise HTTPException(status_code=400, detail="Solo se pueden ver archivos .log")
        
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            all_lines = f.readlines()
        
        total_lines = len(all_lines)
        filtered_lines = all_lines
        
        # Filtrar por nivel si se especificó
        if level:
            level_upper = level.upper()
            filtered_lines = [line for line in filtered_lines if level_upper in line]
        
        # Filtrar por búsqueda si se especificó
        if search:
            filtered_lines = [line for line in filtered_lines if search.lower() in line.lower()]
        
        # Obtener últimas N líneas
        last_lines = filtered_lines[-lines:] if lines else filtered_lines
        app_logger.info(f"Log '{log_name}' leído exitosamente. Total líneas: {total_lines}, Filtradas: {len(filtered_lines)}, Mostradas: {len(last_lines)}")
        return {
            "log_name": log_name,
            "total_lines": total_lines,
            "filtered_lines": len(filtered_lines),
            "displayed_lines": len(last_lines),
            "content": "".join(last_lines),
            "filters": {
                "search": search,
                "level": level,
                "lines": lines
            }
        }
    except FileNotFoundError:
        app_logger.error(f"Log '{log_name}' no encontrado.")
        raise HTTPException(status_code=404, detail=f"Log '{log_name}' no encontrado")
    except Exception as e:
        app_logger.error(f"Error leyendo log '{log_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error leyendo log: {str(e)}")

@router.get("/download/{log_name}")
async def download_log(log_name: str):
    """
    Descarga un archivo de log completo
    
    - **log_name**: Nombre del archivo a descargar
    
    El archivo se descargará en formato texto plano.
    """
    try:
        log_dir = "logs"
        filepath = os.path.join(log_dir, log_name)
        
        if not os.path.exists(filepath):
            app_logger.error(f"Intento de descarga de log no existente: {log_name}")
            raise HTTPException(status_code=404, detail=f"Log '{log_name}' no encontrado")
        
        if not log_name.endswith('.log'):
            app_logger.warning(f"Intento de descarga de archivo no log: {log_name}")
            raise HTTPException(status_code=400, detail="Solo se pueden descargar archivos .log")
        
        return FileResponse(
            path=filepath,
            filename=log_name,
            media_type='text/plain'
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error descargando log: {str(e)}")

@router.get("/errors")
async def view_errors(
    lines: int = Query(50, description="Número de errores a mostrar", ge=1, le=1000)
):
    """
    Muestra solo las líneas de ERROR del log de errores
    
    - **lines**: Número de errores a mostrar (1-1000, por defecto 50)
    
    Este endpoint lee específicamente el archivo `errors.log` que contiene
    solo registros de nivel ERROR y superior.
    """
    try:
        log_dir = "logs"
        filepath = os.path.join(log_dir, "errors.log")
        
        if not os.path.exists(filepath):
            app_logger.info("No hay archivo errors.log disponible.")
            return {
                "errors": [],
                "message": "No hay errores registrados",
                "total_errors": 0
            }
        
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            error_lines = f.readlines()
        
        last_errors = error_lines[-lines:]
        app_logger.info(f"Mostrando {len(last_errors)} errores del log de errores.")
        return {
            "total_errors": len(error_lines),
            "displayed_errors": len(last_errors),
            "errors": "".join(last_errors)
        }
    except Exception as e:
        app_logger.error(f"Error leyendo errores del log: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error leyendo errores: {str(e)}")

@router.get("/tail/{log_name}")
async def tail_log(
    log_name: str,
    lines: int = Query(20, description="Número de líneas finales", ge=1, le=500)
):
    """
    Muestra las últimas N líneas de un log (similar al comando 'tail')
    
    - **log_name**: Nombre del archivo de log
    - **lines**: Número de líneas a mostrar (1-500, por defecto 20)
    
    Útil para monitoreo en tiempo real del último contenido del log.
    """
    try:
        log_dir = "logs"
        filepath = os.path.join(log_dir, log_name)
        print(f"log_name: {log_name}")
        print(f"filepath: {filepath}")
        if not os.path.exists(filepath):
            app_logger.error(f"Intento de tail de log no existente: {log_name}")
            raise HTTPException(status_code=404, detail=f"Log '{log_name}' no encontrado")
        
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            app_logger.info(f"Obteniendo últimas {lines} líneas del log '{log_name}'")
            all_lines = f.readlines()
        
        tail_lines = all_lines[-lines:]
        app_logger.info(f"Log '{log_name}' tail leído exitosamente. Total líneas: {len(all_lines)}, Mostradas: {len(tail_lines)}")
        return {
            "log_name": log_name,
            "total_lines": len(all_lines),
            "tail_lines": len(tail_lines),
            "content": "".join(tail_lines)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo log: {str(e)}")

@router.post("/clear/{log_name}")
async def clear_log(log_name: str):
    """
    Limpia (vacía) un archivo de log específico
    
    ⚠️ **USE CON CUIDADO** - Esta acción borra el contenido del log
    
    - **log_name**: Nombre del archivo a limpiar
    
    Se crea un backup automático antes de limpiar el archivo.
    El backup se guarda con formato: `{log_name}.backup.{timestamp}`
    """
    try:
        log_dir = "logs"
        filepath = os.path.join(log_dir, log_name)
        
        if not os.path.exists(filepath) or not filepath.startswith(os.path.abspath(log_dir)):
            app_logger.error(f"Intento de limpieza de log no existente: {log_name}")
            raise HTTPException(status_code=404, detail=f"Log '{log_name}' no encontrado")
        
        if not log_name.endswith('.log'):
            app_logger.warning(f"Intento de limpieza de archivo no log: {log_name}")
            raise HTTPException(status_code=400, detail="Solo se pueden limpiar archivos .log")
        
        # Hacer backup antes de limpiar
        backup_filename = f"{log_name}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_path = os.path.join(log_dir, backup_filename)
        shutil.copy2(filepath, backup_path)
        app_logger.info(f"Backup del log '{log_name}' creado como '{backup_filename}' antes de limpieza.")
        # Limpiar el archivo
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"# Log cleared at {datetime.now().isoformat()}\n")
            f.write(f"# Backup saved as: {backup_filename}\n\n")
        app_logger.info(f"Log '{log_name}' limpiado exitosamente.")
        return {
            "success": True,
            "message": f"Log '{log_name}' limpiado exitosamente",
            "backup": backup_filename,
            "backup_path": os.path.join("logs", backup_filename)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error limpiando log: {str(e)}")

@router.get("/stats")
async def log_statistics():
    """
    Muestra estadísticas generales de los logs
    
    Retorna información agregada sobre:
    - Número total de archivos de log
    - Tamaño total ocupado
    - Cantidad de errores registrados
    - Última actualización
    """
    try:
        log_dir = "logs"
        if not os.path.exists(log_dir):
            return {
                "total_logs": 0,
                "total_size_mb": 0,
                "message": "No hay logs disponibles"
            }
        
        total_size = 0
        log_count = 0
        latest_modification = None
        
        for filename in os.listdir(log_dir):
            if filename.endswith('.log'):
                filepath = os.path.join(log_dir, filename)
                file_stats = os.stat(filepath)
                total_size += file_stats.st_size
                log_count += 1
                
                mod_time = datetime.fromtimestamp(file_stats.st_mtime)
                if latest_modification is None or mod_time > latest_modification:
                    latest_modification = mod_time
        app_logger.info(f"Cálculo de estadísticas de logs: {log_count} archivos, tamaño total {total_size} bytes.")
        # Contar errores en errors.log
        error_count = 0
        error_log_path = os.path.join(log_dir, "errors.log")
        if os.path.exists(error_log_path):
            with open(error_log_path, 'r', encoding='utf-8', errors='replace') as f:
                error_count = len(f.readlines())
        app_logger.info(f"Estadísticas de logs obtenidas: {log_count} archivos, {error_count} errores.")
        
        return {
            "total_logs": log_count,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "total_size_kb": round(total_size / 1024, 2),
            "total_errors": error_count,
            "latest_modification": latest_modification.isoformat() if latest_modification else None,
            "log_directory": os.path.abspath(log_dir)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo estadísticas: {str(e)}")