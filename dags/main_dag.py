from airflow import DAG
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(sys.path)
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.python import PythonSensor
from airflow.sensors.filesystem import FileSensor
import datetime
from etls import main_etl
from etls import psql_load
from etls import s3_etl
from etls import file_sensoring



with DAG(
    dag_id = 'reddit_ETL',
    start_date = datetime.datetime(2025,7,7),
    schedule_interval= '@daily',
    catchup= False
) as dag:
    wait_for_file = PythonSensor(
        task_id = 'sense_file',
        python_callable = file_sensoring.check_file,
        poke_interval=60,
        timeout = 30,
        mode='poke'
    )
    load_to_db = PythonOperator(
        task_id = 'process_csv',
        python_callable = psql_load.main
    )
    extract_data = PythonOperator(
        task_id = 'extract_from_api',
        python_callable = main_etl.main
    )
    upload_to_s3 = PythonOperator(
        task_id = 'upload_to_s3',
        python_callable = s3_etl.main
    )
    extract_data>> load_to_db >> wait_for_file>>upload_to_s3

