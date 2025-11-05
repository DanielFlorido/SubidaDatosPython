import os
import shutil
import uuid
import threading
from app.services.excel_service import ExcelService
from app.models.schemas import JobResponse, JobStatusResponse, JobStatus
from app.utils.job_manager import job_manager
from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from fastapi.responses import JSONResponse
from typing import List
from datetime import datetime
from app.utils.logger import app_logger
from starlette.status import HTTP_200_OK, HTTP_503_SERVICE_UNAVAILABLE

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
    
    job = job_manager.create_job(job_id)

    app_logger.info(f"Nuevo trabajo creado - Job ID: {job_id}, Cliente: {identificacion_cliente}, Fecha: {fecha}, Archivo: {file.filename}")
    def process_in_thread():
        try:
            excel_service.process_and_save_async(
                file_path,
                identificacion_cliente,
                fecha,
                job_id
            )
        except Exception as e:
            app_logger.error(f"Error en procesamiento del trabajo {job_id}: {str(e)}")
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
    return JobResponse(
        job_id=job_id,
        status=JobStatus.PENDING,
        message="Archivo recibido y en proceso",
        progress=0,
        created_at= datetime.utcnow().isoformat()
    )

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
    try:
        result = excel_service.repository.test_connection()
        if result.get("success"):
            return JSONResponse(
                content={"status": "healthy", "database": "connected"},
                status_code=HTTP_200_OK,
            )
        else:
            return JSONResponse(
                content={"status": "unhealthy", "database": "disconnected", "details": result},
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
            )
    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
        )


def cleanup_file(file_path: str):
    """Elimina el archivo temporal después del procesamiento"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception:
        pass  