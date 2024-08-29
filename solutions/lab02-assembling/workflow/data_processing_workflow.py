import os

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSCreateObjectOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Define the DAG
dag = DAG(
    'data_processing_workflow',
    depends_on_past = False,
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
    gcp_conn_id='google_cloud_default',
    storage_class='STANDARD',
    labels={'env': 'dev', 'team': 'airflow'},
    dag=dag
)

# Upload the file to GCS
upload_sample_data = GCSCreateObjectOperator(
    task_id='upload_to_gcs',
    bucket_name=project_id,
    object_name='sample_data/events.json',
    filename = os.path.join(dag_folder, 'sample_data/events.json'),
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

create_bucket >> upload_sample_data