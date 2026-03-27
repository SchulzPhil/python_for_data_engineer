from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="data_mart_pipeline",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    spark_job = SparkSubmitOperator(
        task_id="build_marts",
        jars="/opt/airflow/postgresql-42.7.3.jar",
        application="/opt/airflow/spark_jobs/data_mart_pipeline.py",
        conn_id="spark_default",
        conf={
            "spark.executor.memory": "12g",
            "spark.executor.cores": "2"
        }
    )
