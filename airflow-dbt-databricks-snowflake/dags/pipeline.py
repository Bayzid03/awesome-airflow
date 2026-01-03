"""
Airflow DAG: DBT Databricks to Snowflake Pipeline
Simple pipeline that runs DBT on Databricks and loads data to Snowflake
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Default arguments
default_args = {
    'owner': 'data-team',
    'retries': 2,
    'email_on_failure': True,
}

# DAG definition
with DAG(
    dag_id='dbt_databricks_snowflake_pipeline',
    default_args=default_args,
    description='Run DBT on Databricks and load to Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 3),
    catchup=False,
    tags=['dbt', 'databricks', 'snowflake'],
) as dag:

    # Task 1: Run DBT on Databricks
    run_dbt = DatabricksRunNowOperator(
        task_id='run_dbt',
        databricks_conn_id='databricks_default',
        job_id= .....,  # Replace with your Databricks job ID
        wait_for_completion=True,
        polling_period_seconds=30,
        databricks_retry_limit=3,
        databricks_retry_delay=10,
    )

    # Task 2: Load to Snowflake
    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        sql="COPY INTO analytics.table FROM @stage/path FILE_FORMAT = (TYPE=PARQUET);",
        snowflake_conn_id='snowflake_default',
    )

    # Define task dependency
    run_dbt >> load_to_snowflake
