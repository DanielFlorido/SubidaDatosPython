import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime


LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(name: str = "app_logger", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, 'application.log'),
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    error_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, 'errors.log'),
        maxBytes=10*1024*1024,
        backupCount=10,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    return logger

# Crear logger global
app_logger = setup_logger()

def log_database_connection(success: bool, details: dict = None):
    if success:
        app_logger.info(f" Conexión a BD exitosa")
    else:
        app_logger.error(f" Error de conexión a BD: {details}")

def log_excel_processing(job_id: str, status: str, message: str, **kwargs):
    extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
    log_msg = f"[Job: {job_id}] {status} - {message}"
    if extra_info:
        log_msg += f" | {extra_info}"
    
    if status in ['FAILED', 'ERROR']:
        app_logger.error(log_msg)
    elif status == 'WARNING':
        app_logger.warning(log_msg)
    else:
        app_logger.info(log_msg)

def log_transaction(action: str, details: str, success: bool = True):
    if success:
        app_logger.info(f" Transacción: {action} - {details}")
    else:
        app_logger.error(f" Transacción FALLIDA: {action} - {details}")

def log_validation(validation_type: str, passed: bool, details: str = ""):
    if passed:
        app_logger.info(f" Validación [{validation_type}] PASÓ: {details}")
    else:
        app_logger.warning(f"Validación [{validation_type}] FALLÓ: {details}")