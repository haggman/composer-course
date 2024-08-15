from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

from datetime import datetime, timedelta

from airflow.models import Variable
project_id = Variable.get("project_id")

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='sensor_example',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
)

create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=project_id,
    project_id=project_id, 
    dag=dag
)

wait_for_gcs_file = GCSObjectExistenceSensor(
    task_id='wait_for_gcs_file',
    bucket=project_id,
    object='test.txt',
    timeout=600,  # Wait for up to 10 minutes
    poke_interval=30,  # Check every 30 seconds
    mode='poke',  # Can also be 'reschedule'
    dag=dag,
)

def process_data():
    print("File found, starting data processing...")

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

create_bucket >> wait_for_gcs_file >> process_data_task