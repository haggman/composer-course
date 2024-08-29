from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateObjectOperator
from airflow.utils.dates import days_ago

dag = DAG(
    'data_processing_workflow',
    depends_on_past = False,
    catchup = False,
    start_date = days_ago(0),
    schedule_interval=None,
)

# Download the file content
get_file = SimpleHttpOperator(
    task_id='get_file',
    http_conn_id='github_conn',
    endpoint='user/repo/raw/main/path/to/file.txt',
    method='GET',
    response_filter=lambda response: response.text,
    dag=dag,
)

# Upload the file to GCS
upload_to_gcs = GCSCreateObjectOperator(
    task_id='upload_to_gcs',
    bucket_name='your-bucket-name',
    object_name='path/to/destination/file.txt',
    data="{{ task_instance.xcom_pull(task_ids='get_file') }}",
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

check_file >> get_file >> upload_to_gcs