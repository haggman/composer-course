# To help with delays, like retry delay
from datetime import timedelta
# To determine the path to the DAG folder
import os 
# We'll use this to help make a unique table name
import uuid

 # DAG object definition
from airflow import DAG
# To set the start date of the DAG
from airflow.utils.dates import days_ago
# To access variables defined in the Airflow UI
from airflow.models import Variable
# PythonOperator to execute Python callables
from airflow.operators.python_operator import PythonOperator
# Sensor to check for the existence of objects in Google Cloud Storage (GCS)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
# Operator to run Beam pipelines on Dataflow
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
# Enum for specifying the Beam runner type
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
# Operator to export data from BigQuery to GCS
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
# Configuration class for Dataflow pipelines
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
# Sensor to monitor the status of Dataflow jobs
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
# Enum for Dataflow job states
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
# Operator to delete objects from GCS
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
# Operator to delete tables from BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator

# Client library for interacting with the Dataflow API
from google.cloud import dataflow_v1beta3

dag_folder = os.path.dirname(__file__)

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'file_ingest_workflow',
    default_args = default_args,
    catchup = False,
    start_date = days_ago(0),
    schedule_interval=None,
)

# Task 1: Wait for file to appear in GCS
wait_for_file = GCSObjectExistenceSensor(
    task_id='wait_for_file',
    bucket='{{ var.value.bucket }}',
    object='sample_data/events.json',
    mode='poke',
    poke_interval=30,
    timeout=600,
    dag=dag
)

# Task 2: Generate a temp table name
def generate_temp_table_name(**kwargs):
    logger = kwargs['ti'].log
    table_name = f'logs.user_traffic_{uuid.uuid4().hex}'
    logger.info(f'Generated temp table name: {table_name}')
    kwargs['ti'].xcom_push(key='temp_table_name', value=table_name)

gen_temp_table_name = PythonOperator(
        task_id='gen_temp_table_name',
        python_callable=generate_temp_table_name,
        dag=dag,
    )

# Task 3: Run datafile through Dataflow
run_dataflow = BeamRunPythonPipelineOperator(
    task_id='run_dataflow',
    py_file=os.path.join(dag_folder, 'dataflow/batch_user_traffic_pipeline.py'),
    runner=BeamRunnerType.DataflowRunner,
    pipeline_options={
        'project': '{{ var.value.project_id }}',
        'staging_location': 'gs://{{ var.value.bucket }}/staging',
        'temp_location': 'gs://{{ var.value.bucket }}/temp',
        'input_path': 'gs://{{ var.value.bucket }}/sample_data/events.json',
        'table_name': '{{ task_instance.xcom_pull(task_ids="gen_temp_table_name", key="temp_table_name") }}',
        'job_name': 'batch-user-traffic-pipeline-{{ ts_nodash | lower }}',
        'region': 'us-central1',
    },
    dag=dag
)

 # Task 4: Get the Dataflow job ID
def get_dataflow_job_id(job_name_prefix, project_id, **kwargs):
    client = dataflow_v1beta3.JobsV1Beta3Client()

    request = dataflow_v1beta3.ListJobsRequest(
        project_id=project_id,
        location='us-central1',
        filter=dataflow_v1beta3.ListJobsRequest.Filter.ALL,
    )

    jobs = client.list_jobs(request=request)

    job_id = None
    for job in jobs:
        if job.name.startswith(job_name_prefix):
            job_id = job.id
            break

    # Push the job ID to XCom
    kwargs['ti'].xcom_push(key='dataflow_job_id', value=job_id)

get_job_id = PythonOperator(
    task_id='get_job_id',
    python_callable=get_dataflow_job_id,
    op_kwargs={
        'job_name_prefix': 'batch-user-traffic-pipeline-{{ ts_nodash | lower }}',
        'project_id': '{{ var.value.project_id }}',
    },
    provide_context=True,
    dag=dag,
)

# Task 5: Wait for the Dataflow job to complete
wait_until_dataflow_job_done = DataflowJobStatusSensor(
    task_id="wati_until_dataflow_job_done",
    job_id="{{ti.xcom_pull(task_ids='get_job_id', key='dataflow_job_id')}}",
    expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
    project_id = Variable.get("project_id"),
    location="us-central1",
    poke_interval=30, 
    timeout=600,
)

# Task 6: Export results from BigQuery to GCS
export_results = BigQueryToGCSOperator(
    task_id='export_results',
    source_project_dataset_table='{{ task_instance.xcom_pull(task_ids="gen_temp_table_name", key="temp_table_name") }}',
    destination_cloud_storage_uris=['gs://{{ var.value.bucket }}/output/results_*.csv'],
    export_format='CSV',
    dag=dag
)

# Task 7: Delete the input events file
delete_input_file = GCSDeleteObjectsOperator(
    task_id='delete_input_file',
    bucket_name='{{ var.value.bucket }}',
    objects=['sample_data/events.json'],   
    dag=dag,
)

# Task 8: Drop the temp BigQuery table
drop_temp_table = BigQueryDeleteTableOperator(
    task_id='drop_temp_table',
    deletion_dataset_table='{{ task_instance.xcom_pull(task_ids="gen_temp_table_name", key="temp_table_name") }}',
    ignore_if_missing=True,  # Set to False if you want the task to fail when the table doesn't exist
    dag=dag,
)

wait_for_file >> gen_temp_table_name >> run_dataflow
run_dataflow >> get_job_id >> wait_until_dataflow_job_done
wait_until_dataflow_job_done >> export_results >> delete_input_file
delete_input_file >> drop_temp_table