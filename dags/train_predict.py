from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.utils.state import DagRunState
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

DBT_DIR = "/opt/airflow/dbt"
DBT_PROJECT_DIR = f"{DBT_DIR}/stock_analytics"

@task
def train(train_input_table, train_view, forecast_function_name):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Filter for the last 2 years (730 days) to optimize training costs
    # while providing enough data for a 30-day forecast.
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, ROLLING_BETA, SYMBOL
        FROM {train_input_table}
        WHERE ROLLING_BETA IS NOT NULL
          AND DATE >= DATEADD(day, -730, CURRENT_DATE());"""
          
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'ROLLING_BETA',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""
    
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(create_view_sql)
            cur.execute(create_model_sql)

@task
def predict(forecast_function_name, train_input_table, forecast_table, final_table):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # UPDATED: Forecast for 30 days instead of 7
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 30,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(make_prediction_sql)
            cur.execute(create_final_table_sql)

with DAG(
    dag_id='TrainPredict',
    start_date=datetime(2025, 9, 29),
    catchup=False,
    tags=['ML', 'snowflake', 'layer-3'],
    schedule='30 3 * * *'
) as dag:

    train_input_table = "PUBLIC_ANALYTICS.MART_SECTOR_VOLATILITY"
    train_view = "PUBLIC.sector_volatility_view"
    forecast_table = "PUBLIC.sector_volatility_forecast"
    forecast_function_name = "PUBLIC_ANALYTICS.predict_sector_beta"
    final_table = "PUBLIC_ANALYTICS.sector_volatility_prediction"

    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_elt_dbt_upstream",
        external_dag_id="elt_dbt_upstream",
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        poke_interval=60,
        timeout=3600,
        mode="poke",
        execution_delta=timedelta(minutes=30)
    )

    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)

    # Run downstream dbt to merge the new 30-day forecast into the dashboard tables
    dbt_cleanup_and_test = BashOperator(
        task_id="dbt_run_downstream_dashboards",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select mart_forecast_combined mart_risk_reward_scatter mart_market_regime --profiles-dir {DBT_DIR} && "
            f"dbt test --select mart_forecast_combined mart_risk_reward_scatter mart_market_regime --profiles-dir {DBT_DIR}"
        )
    )
    
    wait_for_upstream >> train_task >> predict_task >> dbt_cleanup_and_test