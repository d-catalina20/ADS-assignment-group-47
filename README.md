# ADS-assignment-group-47

## Benchmarking & Evaluation

### Start Cluster

```sh
docker compose up --build -d --wait
```

### Baseline (1 Server)


```sh
docker compose run --rm -T \
  -e SERVER_HOST=server_1 -e SERVER_PORT=18861 \
  -e MPLCONFIGDIR=/tmp/matplotlib -e MPLBACKEND=Agg \
  client python -u /app/benchmark.py \
  --file article1.txt \
  --rates 50,70,90,110,130 --duration 10 --workers 64 \
  --output-dir /app/benchmark_results
```

### Least connections (3 Servers)

```sh
LB_ALGORITHM=least_connections docker compose up -d loadbalancer

docker compose run --rm -T \
  -e MPLCONFIGDIR=/tmp/matplotlib -e MPLBACKEND=Agg \
  client python -u /app/benchmark.py \
  --file article1.txt \
  --rates 50,70,90,110,130 --duration 10 --workers 64 \
  --output-dir /app/results_3s_least_conn
```

### Least Response Time (3 Servers)

```sh
LB_ALGORITHM=least_response_time docker compose up -d loadbalancer

docker compose run --rm -T \
  -e MPLCONFIGDIR=/tmp/matplotlib -e MPLBACKEND=Agg \
  client python -u /app/benchmark.py \
  --file article1.txt \
  --rates 50,70,90,110,130 --duration 10 --workers 64 \
  --output-dir /app/results_3s_least_rt
```

### Generate Comparative Figures

```sh
python plt_cmp.py
```

### Teardown
```sh
docker compose down -v
```
