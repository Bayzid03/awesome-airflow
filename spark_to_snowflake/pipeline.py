"""
Airflow DAG: Spark (PySpark) to Snowflake Pipeline

Simple pipeline that runs Spark ETL jobs and loads data to Snowflake
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Default arguments
default_args = {
    'owner': 'data-team',
    'retries': 2,
    'email_on_failure': True,
}

# DAG definition
with DAG(
    dag_id='spark_to_snowflake_pipeline',
    default_args=default_args,
    description='Run Spark ETL and load to Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 3),
    catchup=False,
    tags=['spark', 'pyspark', 'snowflake', 'etl'],
) as dag:

    # Task 1: Run Spark ETL job
    spark_etl = SparkSubmitOperator(
        task_id='run_spark_etl',
        application='',  # Path to your PySpark script
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.executor.memory': '4g',
            'spark.driver.memory': '2g',
        },
    )

    # Task 2: Load transformed data to Snowflake
    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        sql="""
            COPY INTO analytics.sales_fact
            FROM @my_s3_stage/curated/sales_fact/
            FILE_FORMAT = (TYPE=PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
        snowflake_conn_id='snowflake_default',
    )

    # Task 3: Validate data in Snowflake
    validate_in_snowflake = SnowflakeOperator(
        task_id='validate_data',
        sql='SELECT COUNT(*) FROM analytics.sales_fact;',
        snowflake_conn_id='snowflake_default',
    )

    # Define task dependencies
    spark_etl >> load_to_snowflake >> validate_in_snowflake
