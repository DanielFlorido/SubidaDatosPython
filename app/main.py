import string
from fastapi import FastAPI
from app.controllers import excel_controller, log_controller, flujo_caja_controller

app = FastAPI(
    title="Excel to SQL API",
    description="API para cargar Excel y guardar en SQL Server",
    version="1.1.24"
)

# Registra los routers
app.include_router(excel_controller.router)
app.include_router(log_controller.router)
app.include_router(flujo_caja_controller.router)

@app.get("/")
async def root():
    return {
        "app_name": app.title,
        "description": app.description,
        "version": app.version,
        "status": "running"
    }
@app.get("/health")
async def health_check():
    return {"status": "healthy"}