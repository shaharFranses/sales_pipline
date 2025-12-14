# airflow-sales-project

Lightweight Apache Airflow project containing a DAG for a sales ETL pipeline.

## What’s included

- `dags/` — DAG definitions (e.g. `sales_dag.py`)
- `plugins/` — helper operators and pipeline code
- `etl_scripts/` — ETL scripts
- `docker-compose.yaml` — for local Airflow + dependencies

## Quickstart

Requirements: Docker & Docker Compose

1. Build and start local Airflow:

```powershell
docker-compose up --build
```

2. Visit the Airflow UI at `http://localhost:8080` and enable the `daily_sales_pipeline` DAG.

## License

This project is released under the MIT License — see `LICENSE`.

---

(You can edit this README later to add more details.)
