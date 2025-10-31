import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    def __init__(self):
        self.db_driver = os.getenv("DB_DRIVER")
        self.db_server = os.getenv("DB_SERVER")
        self.db_database = os.getenv("DB_DATABASE")
        self.db_username = os.getenv("DB_USERNAME")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_port = os.getenv("DB_PORT")

settings = Settings()