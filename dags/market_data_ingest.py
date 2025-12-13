from __future__ import annotations

from datetime import datetime, timedelta
from typing import List
import logging
import time

import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# ----------------------------
# Config via Airflow Variables
# ----------------------------
DB_NAME: str = Variable.get("db_name", "USER_DB_OSTRICH")
RAW_SCHEMA: str = Variable.get("raw_schema", "RAW")

# Default symbols to SPY + 11 Sectors
DEFAULT_SYMBOLS = "SPY,XLK,XLF,XLY,XLI,XLE,XLB,XLV,XLU,XLC,XLP,XLRE"
SYMBOLS: List[str] = [
    s.strip().upper() 
    for s in Variable.get("symbols", DEFAULT_SYMBOLS).split(",") 
    if s.strip()
]

# Fetch 6 years (approx 2200 days) to cover Pre-COVID to Now
TRAIN_DAYS: int = int(Variable.get("train_days", "2200"))

RAW_TABLE = "MARKET_DATA"
STAGE_TABLE = "MARKET_DATA_STAGE"

logger = logging.getLogger("airflow.task")

def fq(obj: str) -> str:
    """Return 3-part, quoted identifier for DB objects."""
    return f'"{DB_NAME}"."{RAW_SCHEMA}"."{obj}"'

DEFAULT_ARGS = {
    "owner": "data",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="market_data_ingest",
    description="DAG 1: Ingest yfinance data into Snowflake",
    start_date=datetime(2025, 1, 1),
    schedule='30 2 * * *',
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingest", "yfinance", "snowflake", "full-refresh"],
) as dag:

    # 1. Create RAW Schema & Tables
    ensure_objects = SnowflakeOperator(
        task_id="ensure_objects",
        snowflake_conn_id="snowflake_conn",
        sql=[
            f'create schema if not exists "{DB_NAME}"."{RAW_SCHEMA}"',
            f"""
            create table if not exists {fq(RAW_TABLE)} (
                SYMBOL string,
                DATE date,
                OPEN float,
                CLOSE float,
                LOW float,
                HIGH float,
                VOLUME number
            )
            """,
            f"""
            create table if not exists {fq(STAGE_TABLE)} (
                SYMBOL string,
                DATE date,
                OPEN float,
                CLOSE float,
                LOW float,
                HIGH float,
                VOLUME number
            )
            """,
        ],
    )

    # 2. [NEW TASK] Create PUBLIC Schema
    create_public_schema = SnowflakeOperator(
        task_id="create_public_schema",
        snowflake_conn_id="snowflake_conn",
        sql=f'CREATE SCHEMA IF NOT EXISTS "{DB_NAME}"."PUBLIC"',
    )

    # 3. [EXISTING TASK] Create SEED Schema
    # This ensures the landing zone for dbt seeds exists before dbt tries to run
    create_seed_schema = SnowflakeOperator(
        task_id="create_seed_schema",
        snowflake_conn_id="snowflake_conn",
        sql=f'CREATE SCHEMA IF NOT EXISTS "{DB_NAME}"."PUBLIC_SEEDS"',
    )

    # 4. Empty the Stage
    truncate_stage = SnowflakeOperator(
        task_id="truncate_stage",
        snowflake_conn_id="snowflake_conn",
        sql=f"truncate table if exists {fq(STAGE_TABLE)}",
    )

    # 5. Download & Load Data (Python)
    def load_all_symbols_to_stage(train_days: int = TRAIN_DAYS) -> None:
        if not SYMBOLS:
            raise ValueError("Airflow Variable 'symbols' resolved to an empty list.")

        frames = []
        logger.info(f"[load_all] symbols={SYMBOLS} train_days={train_days}")

        for symbol in SYMBOLS:
            # Retry logic for yfinance
            retries = 3
            df = None
            for attempt in range(1, retries + 1):
                try:
                    df = yf.download(
                        symbol,
                        period=f"{train_days}d",
                        interval="1d",
                        group_by="column",
                        auto_adjust=False,
                        threads=False,
                        progress=False,
                    )
                    if df is not None and not df.empty:
                        break
                except Exception as e:
                    logger.warning(f"Attempt {attempt} failed for {symbol}: {e}")
                time.sleep(2)
            
            if df is None or df.empty:
                logger.error(f"Skipping {symbol}: No data found.")
                continue

            # Handle MultiIndex headers if they exist
            if isinstance(df.columns, pd.MultiIndex):
                if symbol in df.columns.get_level_values(-1):
                    df = df.xs(symbol, axis=1, level=-1, drop_level=True)
                else:
                    df.columns = df.columns.get_level_values(0)

            df = df.reset_index()

            # Normalize columns
            df = df.rename(
                columns={
                    "Date": "DATE", "Open": "OPEN", "High": "HIGH",
                    "Low": "LOW", "Close": "CLOSE", "Volume": "VOLUME",
                }
            )
            df["DATE"] = pd.to_datetime(df["DATE"]).dt.date

            # Cleanup numerics
            for col in ["OPEN", "CLOSE", "LOW", "HIGH", "VOLUME"]:
                df[col] = pd.to_numeric(df.get(col), errors="coerce")

            df["SYMBOL"] = symbol.upper()
            df = df[["SYMBOL", "DATE", "OPEN", "CLOSE", "LOW", "HIGH", "VOLUME"]]

            # Deduplicate
            df = df.dropna(subset=["DATE"]).drop_duplicates(subset=["SYMBOL", "DATE"])
            frames.append(df)

        if not frames:
            raise RuntimeError("No data loaded for any symbol.")

        all_df = pd.concat(frames, ignore_index=True)

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            success, _, nrows, _ = write_pandas(
                conn=conn,
                df=all_df,
                table_name=STAGE_TABLE,
                database=DB_NAME,
                schema=RAW_SCHEMA,
                quote_identifiers=True,
                overwrite=False,
            )
            logger.info(f"Loaded {nrows} rows to stage.")

    load_stage = PythonOperator(
        task_id="load_stage_full",
        python_callable=load_all_symbols_to_stage,
        execution_timeout=timedelta(minutes=10),
    )

    # 6. Full Refresh: Move from Stage to Raw
    full_refresh = SnowflakeOperator(
        task_id="full_refresh",
        snowflake_conn_id="snowflake_conn",
        sql=[
            f"truncate table {fq(RAW_TABLE)}",
            f"""
            insert into {fq(RAW_TABLE)} (SYMBOL, DATE, OPEN, CLOSE, LOW, HIGH, VOLUME)
            select SYMBOL, DATE, OPEN, CLOSE, LOW, HIGH, VOLUME
            from {fq(STAGE_TABLE)}
            qualify row_number() over (partition by SYMBOL, DATE order by DATE) = 1
            """,
        ],
    )

    # 7. Data Quality Check
    dq_check = SnowflakeOperator(
        task_id="dq_no_dupes",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            select count(*) from (
              select SYMBOL, DATE, count(*) 
              from {fq(RAW_TABLE)}
              group by 1,2 
              having count(*) > 1
            ) having count(*) = 0
        """,
    )

    # -----------------
    # Task Wiring
    # -----------------
    # Ensure all three schemas exist before proceeding
    [ensure_objects, create_public_schema, create_seed_schema] >> truncate_stage >> load_stage >> full_refresh >> dq_check