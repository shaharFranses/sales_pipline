from airflow.decorators import dag, task
from datetime import datetime
from sales_pipeline_package.load_sales_pipeline import run_sales_elt
import os


@dag(
    dag_id="daily_sales_pipeline",
    start_date=datetime(2025, 12, 1),
    schedule=None,          # run manually (no schedule)
    catchup=False,
    tags=["sales", "etl"],
)
def sales_elt_workflow():
    # config you want available to the task
    env_vars = {
        "DB_CONN_STRING": "postgresql://airflow:airflow@postgres:5432/airflow",
        "SALES_FILE_PATH": "/opt/airflow/plugins/sales_pipeline_package/sales_data.csv",
    }

    @task(task_id="run_sales_elt_task")
    def run_elt_script(env_vars: dict):
        # make them available as environment variables (if your code expects os.getenv)
        os.environ.update(env_vars)

        print(f"Running ELT with DB: {os.getenv('DB_CONN_STRING')}")
        run_sales_elt()  # uses env vars inside

    # pass config into the task as a normal argument
    run_elt_script(env_vars)


sales_dag = sales_elt_workflow()
