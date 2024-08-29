import os

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Define the DAG
dag = DAG(
    'data_processing_workflow',
    catchup = False,
    start_date = days_ago(0),
    schedule_interval=None,
)

# Get the directory of the current DAG file
dag_folder = os.path.dirname(__file__)

# Get the project ID from the variable
project_id = Variable.get("project_id")

# Create the GCS bucket if it doesn't exist
create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=project_id,
    project_id=project_id,
    location='us-central1',
    storage_class='STANDARD',
    labels={'env': 'dev', 'team': 'airflow'},
    dag=dag
)

# Upload the file to GCS
upload_sample_data = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    bucket=project_id,
    dst='sample_data/events.json',
    src = os.path.join(dag_folder, 'sample_data/events.json'),
    dag=dag,
)

create_bucket >> upload_sample_data