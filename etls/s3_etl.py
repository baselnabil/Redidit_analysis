import boto3
from botocore.exceptions import ClientError
import configparser
import os
import datetime
import logging

# Logging instead of print for Airflow compatibility
log = logging.getLogger(__name__)

# Load credentials from config
parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/.conf'))

ACCESS_KEY = parser.get('S3', 'ACCESS_KEY')
SECRET_ACCESS_KEY = parser.get('S3', 'SECRET_ACCESS_KEY')
REGION = 'eu-north-1'
BUCKET_NAME = 'reddit-analysis-files'
DATA_DIR = '/opt/airflow/data'  # Or Variable.get("data_dir")

def initialize_session():
    try:
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_ACCESS_KEY,
            region_name=REGION
        )
        return session
    except ClientError as e:
        log.error(f"Error initializing session: {e}")
        raise

def get_files(directory=DATA_DIR):
    try:
        return [f for f in os.listdir(directory) if f.endswith('.done')]
    except FileNotFoundError:
        log.error(f"Directory not found: {directory}")
        return []

def create_bucket(session):
    s3 = session.client('s3')
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        log.info('Bucket already exists.')
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            try:
                s3.create_bucket(
                    Bucket=BUCKET_NAME,
                    CreateBucketConfiguration={'LocationConstraint': REGION}
                )
                log.info('Bucket created.')
                return True
            except ClientError as err:
                log.error(f"Error creating bucket: {err}")
                raise
        else:
            log.error(f"Error accessing bucket: {e}")
            raise

def upload_s3(session, directory=DATA_DIR):
    s3 = session.client('s3')
    files_to_delete = []
    files = get_files(directory)

    for file in files:
        full_path = os.path.join(directory, file)
        new_path = full_path[:-5]  # Remove .done

        try:
            os.rename(full_path, new_path)
        except Exception as e:
            log.error(f"Failed to rename file {file}: {e}")
            continue

        curr_month = datetime.datetime.now().month
        key = f'reddit/{curr_month}/{os.path.basename(new_path)}'

        try:
            s3.upload_file(Filename=new_path, Bucket=BUCKET_NAME, Key=key)
            log.info(f'Uploaded {new_path} to s3://{BUCKET_NAME}/{key}')
            files_to_delete.append(new_path)
        except ClientError as e:
            log.error(f"Failed to upload {new_path}: {e}")

    return files_to_delete

def delete_file(file_path):
    try:
        os.remove(file_path)
        log.info(f"Deleted file: {file_path}")
    except FileNotFoundError:
        log.warning(f"File not found for deletion: {file_path}")
    except Exception as e:
        log.error(f"Error deleting file {file_path}: {e}")

def main(**kwargs):
    session = initialize_session()
    if not create_bucket(session):
        log.error("Bucket creation or verification failed.")
        return

    files_uploaded = upload_s3(session)
    for file in files_uploaded:
        delete_file(file)
if __name__=='__main__':
    main()