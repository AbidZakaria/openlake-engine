FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/app PYTHONPATH=/app
WORKDIR /app

RUN pip install --no-cache-dir duckdb==1.4.1 fastapi==0.115.12 uvicorn==0.34.0 requests==2.32.5 pyyaml==6.0.2 pytz==2025.2 \
    && python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('iceberg', 'httpfs', 'avro')]"

COPY openlake ./openlake
COPY config ./config
COPY dashboard ./dashboard

EXPOSE 8501
CMD ["uvicorn", "dashboard.app:app", "--host", "0.0.0.0", "--port", "8501", "--proxy-headers"]
