FROM python:3.11-slim

WORKDIR /workspace
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    freetds-dev \
    freetds-bin \
    unixodbc \
    unixodbc-dev \
    tdsodbc \
    && rm -rf /var/lib/apt/lists/*

RUN echo "[FreeTDS]\n\
    Description = FreeTDS Driver\n\
    Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
    Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so\n\
    UsageCount = 1\n\
    " > /etc/odbcinst.ini

RUN echo "[global]\n\
tds version = 8.0\n\
client charset = UTF-8\n\
port = 1433\n\
encryption = required\n\
text size = 64512" > /etc/freetds/freetds.conf

RUN odbcinst -q -d || echo "No se pudieron listar drivers"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p logs && chmod 755 logs


# Comando de inicio
CMD ["gunicorn", "app.main:app", \
     "--workers", "4", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "300", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]