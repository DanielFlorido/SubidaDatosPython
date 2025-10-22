import os
import shutil
import uuid
import threading
from app.services.excel_service import ExcelService
from app.models.schemas import JobResponse, JobStatusResponse, JobStatus
from app.utils.job_manager import job_manager
from fastapi import APIRouter, UploadFile, File, HTTPException, Form, BackgroundTasks
from typing import List

router = APIRouter(prefix="/api/balance", tags=["Balance General"])
excel_service = ExcelService()

@router.post("/process", response_model=JobResponse)
async def process_excel(
    file: UploadFile = File(...),
    identificacion_cliente: str = Form(...),
    fecha: str = Form(...)
):
    """
    Procesa un archivo Excel de Balance General de forma asíncrona.
    """
    
    # Validaciones básicas
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400, 
            detail="Solo archivos Excel permitidos (.xlsx, .xls)"
        )
    
    if not fecha or len(fecha) != 8 or not fecha.isdigit():
        raise HTTPException(
            status_code=400, 
            detail="Fecha debe estar en formato YYYYMMDD (ej: 20240630)"
        )
    
    if not identificacion_cliente or not identificacion_cliente.strip():
        raise HTTPException(
            status_code=400,
            detail="Identificación del cliente es requerida"
        )
    
    # Crear job ID único
    job_id = str(uuid.uuid4())
    
    # Guardar archivo temporalmente
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, f"{job_id}_{file.filename}")
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error guardando archivo: {str(e)}"
        )
    
    # Crear el trabajo
    job = job_manager.create_job(job_id)
    
    # Imprimir el job_id en consola
    print(f"🔄 Nuevo trabajo creado - Job ID: {job_id}")
    print(f"   Cliente: {identificacion_cliente} | Fecha: {fecha} | Archivo: {file.filename}")
    
    # Ejecutar en un thread separado
    def process_in_thread():
        try:
            excel_service.process_and_save_async(
                file_path,
                identificacion_cliente,
                fecha,
                job_id
            )
        except Exception as e:
            print(f"Error en thread de procesamiento: {str(e)}")
            job_manager.update_job(
                job_id,
                status=JobStatus.FAILED,
                message=f"Error inesperado: {str(e)}",
                progress=100,
                errors=[str(e)]
            )
        finally:
            cleanup_file(file_path)

    thread = threading.Thread(target=process_in_thread, daemon=True)
    thread.start()

@router.get("/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Consulta el estado de un trabajo de procesamiento
    
    - **job_id**: ID del trabajo retornado por /process
    """
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Trabajo {job_id} no encontrado"
        )
    
    return job

@router.get("/health")
async def health_check():
    """Verifica la conexión a la base de datos"""
    try:
        is_connected = excel_service.repository.test_connection()
        if is_connected:
            return {"status": "healthy", "database": "connected"}
        else:
            return {"status": "unhealthy", "database": "disconnected"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def cleanup_file(file_path: str):
    """Elimina el archivo temporal después del procesamiento"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception:
        pass  