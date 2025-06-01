FROM python:3.11.12-bookworm

WORKDIR /app

# Instala dependências
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copia o código ETL
COPY etl_ida.py .
