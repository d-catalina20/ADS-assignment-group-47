# ADS-assignment-group-47

```sh
docker compose up --build -d --wait
docker compose run --rm -T \
  -e MPLCONFIGDIR=/tmp/matplotlib -e MPLBACKEND=Agg \
  client python -u /app/benchmark.py \
  --rates 50,70,90,110,130 --duration 10 --workers 64 \
  --output-dir /app/benchmark_results
```
