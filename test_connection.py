import pyodbc
import os
from dotenv import load_dotenv
from app.config import settings
load_dotenv()

try:
    connection_string = (
            f"DRIVER={{{settings.db_driver}}};"
            f"SERVER={settings.db_server};"
            f"PORT=1433;"
            f"DATABASE={settings.db_database};"
            f"UID={settings.db_username};"
            f"PWD={settings.db_password};"
            f"TDS_Version=8.0;"
            f"Encrypt=yes;"
        )
    
    print("Intentando conectar...")
    print(f"Servidor: {os.getenv('DB_SERVER')}")
    print(f"Base de datos: {os.getenv('DB_DATABASE')}")
    print(f"Usuario: {os.getenv('DB_USERNAME')}")
    
    conn = pyodbc.connect(connection_string, timeout=10)
    print("✓ Conexión exitosa!")
    conn.close()
    
except Exception as e:
    print(f"✗ Error de conexión: {str(e)}")