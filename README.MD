Otel Infra end to end poc using Apache Arrow as the transfer protocol 

<div style="position: relative; padding-bottom: 42.1875%; height: 0;"><iframe src="https://www.loom.com/embed/c631bb6b0b304244a1ab1f2bd6895531?sid=59fa1400-d9a4-4f92-8541-e3f047b7e1c1" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

Steps for POC

Run the collector

```
cd collector

./otelcol-contrib --config otelcol-config.yaml
```

Run the backend service

```
cd service/arrow_receiver

go run main.go
```

Run the Frontend

```
cd frontend

yarn
yarn run dev
```

Check data in DB

```
duckdb traces.db

select * from traces;

COPY (
    SELECT * FROM traces WHERE trace_id = '13c99620c7c04bda2e5bb16f25c81ee6'
  ) TO 'trace_details.json' (FORMAT JSON);
```

Generate mock metrics, logs and traces

```
  telemetrygen traces --otlp-endpoint=localhost:4317 --otlp-insecure
  telemetrygen logs --otlp-endpoint=localhost:4317 --otlp-insecure
  telemetrygen metrics --otlp-endpoint=localhost:4317 --otlp-insecure
```

Adv mock gen

```
telemetrygen traces \
  --otlp-endpoint=localhost:4317 \
  --otlp-insecure \
  --traces 2 \
  --child-spans 3 \
  --telemetry-attributes 'http.method="GET"' \
  --telemetry-attributes 'service.name="frontend"' \
  --telemetry-attributes 'env="prod"' \
  --span-kind server \
  --span-duration 100ms \
  --span-duration-jitter 50ms \
  --trace-attributes 'user.id="123"' \
  --trace-attributes 'order.id="456"' \
  --trace-groups 1 \
  --trace-group-size 2 \
  --trace-group-interval 1s \
  --trace-group-jitter 500ms

telemetrygen metrics \
  --otlp-endpoint=localhost:4317 \
  --otlp-insecure \
  --metrics 2 \
  --metric-name "http_requests_total" \
  --metric-type counter \
  --metric-unit "requests" \
  --telemetry-attributes 'service.name="backend"' \
  --telemetry-attributes 'env="prod"' \
  --telemetry-attributes 'endpoint="/api"' \
  --metric-value 42 \
  --metric-value-jitter 10 \
  --resource-attributes 'host.name="host1"' \
  --resource-attributes 'region="us-west"'

telemetrygen logs \
  --otlp-endpoint=localhost:4317 \
  --otlp-insecure \
  --logs 5 \
  --log-body "User login event" \
  --telemetry-attributes 'user.id="123"' \
  --telemetry-attributes 'action="login"' \
  --telemetry-attributes 'env="prod"' \
  --log-severity-number 9 \
  --log-severity-text "Info" \
  --resource-attributes 'service.name="auth"' \
  --resource-attributes 'host.name="host2"'
```
