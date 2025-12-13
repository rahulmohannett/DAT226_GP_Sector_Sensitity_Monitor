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
    dag_id="elt_dbt_downstream",
    start_date=datetime(2025, 1, 1),
    schedule_interval='00 4 * * *', # 30 mins after ML
    catchup=False,
    default_args=default_args,
    description="DAG 4: Downstream dbt (Cleanup & Dashboards)",
    max_active_runs=1,
) as dag:

    wait_for_ml = ExternalTaskSensor(
        task_id="wait_for_TrainPredict",
        external_dag_id="TrainPredict",
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        poke_interval=60,
        timeout=3600,
        mode="poke",
        execution_delta=timedelta(minutes=30)
    )

    # Run only the dashboard models
    dbt_run_downstream = BashOperator(
        task_id="dbt_run_downstream",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select mart_forecast_combined mart_risk_reward_scatter mart_market_regime --profiles-dir {DBT_DIR}"
        ),
    )

    dbt_test_downstream = BashOperator(
        task_id="dbt_test_downstream",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select mart_forecast_combined mart_risk_reward_scatter mart_market_regime --profiles-dir {DBT_DIR}"
        ),
    )

    wait_for_ml >> dbt_run_downstream >> dbt_test_downstream