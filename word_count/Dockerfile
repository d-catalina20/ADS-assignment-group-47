FROM python:3.13-slim-bookworm AS base

RUN apt-get update \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir rpyc redis numpy matplotlib

WORKDIR /app

COPY server/server.py /app/server.py
COPY data /app/data

CMD ["python", "/app/server.py"]

FROM base AS loadbalancer

COPY loadbalancer /app/loadbalancer

CMD ["python", "/app/loadbalancer/loadbalancer.py"]
