from airflow import DAG
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(sys.path)
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.python import PythonSensor
import boto3
from botocore.exceptions import ClientError
from airflow.sensors.filesystem import FileSensor
import datetime
from etls import main_etl
from etls import psql_load


def check_file():
    dir='/opt/airflow/data'
    files= os.listdir(dir)
    return any(file.endswith('.done') for file in files )
    

def upload_s3(file):
    s3_hook = S3Hook('s3_access_key')
    bucket_name = 'reddit-analysis-files'
    
    os.rename(file,file[:-5]) ## because the file was marked as .done at the end 
    curr_month = datetime.datetime.now().month
    key = f'reddit/{curr_month}/{file.split("/")[-1]}'

    try:
        s3_hook.load_file(filename=file, key=key, bucket_name=bucket_name)
        print(f'{file} uploaded at {bucket_name}/reddit-{curr_month}')
    except ClientError as e :
        print(e)
        return False

def upload_delete():
    dir='/opt/airflow/data'
    files= os.listdir(dir)
    for file in files :
        if file.endswith('.done'):
            print('csv file is found')
            upload_s3(os.path.join(dir,file))
            os.remove(os.path.join(dir,file))
with DAG(
    dag_id = 'reddit_ETL',
    start_date = datetime.datetime(2025,7,7),
    schedule_interval= '@daily',
    catchup= False
) as dag:
    sense_file = PythonSensor(
        task_id = 'sense_file',
        python_callable = check_file,
        poke_interval=60,
        timeout = 30,
        mode='poke'
    )
    process_csv = PythonOperator(
        task_id = 'check_csv',
        python_callable = upload_delete
    )
    etl_to_csv = PythonOperator(
        task_id = 'extract_from_api',
        python_callable = main_etl.main
    )
    load_to_db = PythonOperator(
        task_id = 'load_to_db',
        python_callable = psql_load.main
    )
    etl_to_csv>>sense_file>>process_csv
    etl_to_csv>> load_to_db

