from typing import Dict
from datetime import datetime
from app.models.schemas import JobStatus, JobStatusResponse
from datetime import datetime
from typing import Dict, Any

class JobManager:
    def __init__(self):
        self.jobs: Dict[str, JobStatusResponse] = {}
    
    def create_job(self, job_id: str) -> JobStatusResponse:
        """Crea un nuevo trabajo"""
        now = datetime.now().isoformat()
        job = JobStatusResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            message="Trabajo creado, esperando procesamiento",
            progress=0,
            total_rows=0,
            processed_rows=0,
            errors=[],
            result=None, 
            created_at=now,
            updated_at=now,
            started_at=None,  
            completed_at=None
        )
        self.jobs[job_id] = job
        return job
    
    def update_job(
        self, 
        job_id: str, 
        status: JobStatus = None,
        message: str = None,
        progress: int = None,
        total_rows: int = None,
        processed_rows: int = None,
        errors: list = None,
        result: Any = None 
    ):
        """Actualiza el estado de un trabajo"""
        if job_id not in self.jobs:
            return None
        
        job = self.jobs[job_id]
        
        if status:
            job.status = status
            print(f"📊 [{job_id[:8]}...] Estado: {status.value} - {message if message else ''}")
        if message:
            job.message = message
        if progress is not None:
            job.progress = progress
        if total_rows is not None:
            job.total_rows = total_rows
        if processed_rows is not None:
            job.processed_rows = processed_rows
        if errors is not None:
            job.errors = errors
        if result is not None:
            job.result = result  
        
        job.updated_at = datetime.now().isoformat()
        
        if status == JobStatus.PROCESSING and not getattr(job, "started_at", None):
            job.started_at = datetime.now().isoformat()
        
        if status in [JobStatus.COMPLETED, JobStatus.FAILED]:
            job.completed_at = datetime.now().isoformat()
        
        return job
    
    def get_job(self, job_id: str) -> JobStatusResponse:
        """Obtiene el estado de un trabajo"""
        return self.jobs.get(job_id)
    
    def delete_job(self, job_id: str):
        """Elimina un trabajo"""
        if job_id in self.jobs:
            del self.jobs[job_id]


# ✅ Instancia global
job_manager = JobManager()
