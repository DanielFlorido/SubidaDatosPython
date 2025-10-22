from fastapi import FastAPI
from app.controllers import excel_controller

app = FastAPI(
    title="Excel to SQL API",
    description="API para cargar Excel y guardar en SQL Server",
    version="1.0.0"
)

# Registra los routers
app.include_router(excel_controller.router)

@app.get("/")
async def root():
    return {"message": "API funcionando correctamente"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}