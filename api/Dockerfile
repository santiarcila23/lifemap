FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Versión y commit que muestra /health (Jenkins pasa el hash del commit desplegado)
ARG APP_VERSION=local
ARG GIT_COMMIT=local
ENV APP_VERSION=${APP_VERSION} GIT_COMMIT=${GIT_COMMIT}

EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "3", "--timeout", "120", "--access-logfile", "-", "app:create_app()"]
