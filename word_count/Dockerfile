FROM python:3.9-slim-bullseye

RUN apt-get update \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir rpyc redis numpy matplotlib

WORKDIR /app

CMD ["python", "/app/server.py"]