# Imagen única para el cliente de ingesta, el scheduler y los workers de Dask:
# todos tienen las mismas versiones de librerías y el mismo código (geohash.py).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    DASK_DATAFRAME__CONVERT_STRING=False
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN mkdir -p /data/raw /data/clean /data/reports
ENV PYTHONPATH=/app

CMD ["python", "-u", "ingest.py"]
