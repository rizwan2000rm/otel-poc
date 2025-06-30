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
