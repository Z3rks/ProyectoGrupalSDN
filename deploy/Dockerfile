# Imagen única para control, nodo de datos y cliente: cambia solo el comando de arranque.
#   docker build -f deploy/Dockerfile -t dfsha .
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY control ./control
COPY datanode ./datanode
COPY client ./client

ENV PYTHONUNBUFFERED=1
# Sin ENTRYPOINT fijo: docker-compose indica `python -m control.main` o `python -m datanode.main`.
CMD ["python", "-m", "client", "--help"]
