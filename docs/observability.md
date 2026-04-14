
```text
[ Ingestion Pipeline ]
    ↓
(OTEL SDK: logs + metrics + traces)
    ↓
[ OTEL Collector ]
    ↓
 ┌───────────────┬───────────────┬───────────────┐
 │   Prometheus  │     Tempo     │     Loki      │
 │   (metrics)   │   (traces)    │    (logs)     │
 └───────────────┴───────────────┴───────────────┘
                ↓
             Grafana
```