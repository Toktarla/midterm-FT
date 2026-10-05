FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /app/data /app/logs /app/runtime /app/results \
    && chmod 777 /app/data /app/logs /app/runtime /app/results
EXPOSE 8001 8002 8101 8102 8201 8202 8301 8302
CMD ["python", "-m", "uvicorn", "services.gateway:app", "--host", "0.0.0.0", "--port", "8001"]
