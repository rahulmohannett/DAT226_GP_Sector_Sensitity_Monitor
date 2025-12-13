from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

DBT_DIR = "/opt/airflow/dbt"
DBT_PROJECT_DIR = f"{DBT_DIR}/stock_analytics"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_dbt_upstream",
    start_date=datetime(2025, 1, 1),
    schedule_interval='00 3 * * *', # Runs at 03:00 AM
    catchup=False,
    default_args=default_args,
    description="DAG 2: Upstream dbt (Seed -> Run -> Test)",
    max_active_runs=1,
) as dag:

    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_market_data_ingest",
        external_dag_id="market_data_ingest",
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        poke_interval=60,
        timeout=3600,
        mode="poke",
        execution_delta=timedelta(minutes=30)
    )

    # 1. SEED: Load static CSVs (Sector Names/Weights) into Snowflake
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt seed --profiles-dir {DBT_DIR}"
        ),
    )

    # 2. RUN: Build models up to the Volatility Mart
    dbt_run_upstream = BashOperator(
        task_id="dbt_run_upstream",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select +mart_sector_volatility --profiles-dir {DBT_DIR}"
        ),
    )

    # 3. TEST: Validate data quality
    dbt_test_upstream = BashOperator(
        task_id="dbt_test_upstream",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select +mart_sector_volatility --profiles-dir {DBT_DIR}"
        ),
    )

    # Updated Dependency Chain
    wait_for_ingest >> dbt_seed >> dbt_run_upstream >> dbt_test_upstream