from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator

from google.cloud import dataflow_v1beta3

from datetime import timedelta
import os
import logging
import uuid


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

# Get the project ID from the variable
project_id = Variable.get("project_id")

# Get the directory of the current DAG file
dag_folder = os.path.dirname(__file__)

# Set a variable to hold the name of the bucket (same as project ID)
bucket = project_id

# Task 1: Wait for file to appear in GCS
wait_for_file = GCSObjectExistenceSensor(
    task_id='wait_for_file',
    bucket=bucket,
    object='sample_data/events.json',
    mode='poke',
    poke_interval=30,
    timeout=600,  # Timeout after 10 minutes (600 seconds)
    dag=dag
)

# Task 2: Generate a temp table name
def generate_temp_table_name(**context):
    logger = context['ti'].log
    table_name = f'logs.user_traffic_{uuid.uuid4().hex}'
    logger.info(f'Generated temp table name: {table_name}')
    context['ti'].xcom_push(key='temp_table_name', value=table_name)

gen_temp_table_name = PythonOperator(
        task_id='gen_temp_table_name',
        python_callable=generate_temp_table_name,
        dag=dag,
    )

# Task 3: Run datafile through Dataflow
run_dataflow = BeamRunPythonPipelineOperator(
    task_id='run_dataflow',
    py_file=os.path.join(dag_folder, 'dataflow/batch_user_traffic_pipeline.py'),
    # py_requirements=["apache-beam[gcp]"],
    runner=BeamRunnerType.DataflowRunner,
    pipeline_options={
        'staging_location': f'gs://{bucket}/staging',
        'temp_location': f'gs://{bucket}/temp',
        'input_path': f'gs://{bucket}/sample_data/events.json',
        'table_name': '{{ task_instance.xcom_pull(task_ids="gen_temp_table_name", key="temp_table_name") }}'
    },
    dataflow_config = DataflowConfiguration(
       job_name ='batch-user-traffic-pipeline-{{ ts_nodash | lower }}',
       project_id = project_id,
       location = 'us-central1',
       wait_until_finished = True,
    ),
    dag=dag
)

# Task 4: Get the Dataflow job ID
def get_dataflow_job_id(job_name_prefix, **kwargs):
    client = dataflow_v1beta3.JobsV1Beta3Client()

    request = dataflow_v1beta3.ListJobsRequest(
        project_id=project_id,
        location='us-central1',
        filter=dataflow_v1beta3.ListJobsRequest.Filter.ALL,
    )
    print('Getting the jobs data')
    jobs = client.list_jobs(request=request)
    print(f'Looking for job_name_prefix: {job_name_prefix}')

    job_id = None
    for job in jobs:
        print(f'job.name={job.name}')
        print(f'job.id={job.id}')
        if job.name.startswith(job_name_prefix):
            job_id = job.id
            print('Found a match!')
            break
    print(f'Did break, about to push ID. job_id={job_id}')
    # Push the job ID to XCom
    kwargs['ti'].xcom_push(key='dataflow_job_id', value=job_id)
    return job_id

get_job_id = PythonOperator(
    task_id='get_job_id',
    python_callable=get_dataflow_job_id,
    op_kwargs={
        'job_name_prefix': 'batch-user-traffic-pipeline-{{ ts_nodash | lower }}',
    },
    provide_context=True,
    dag=dag,
)

# Task 5: Wait for the Dataflow job to complete
wati_until_dataflow_job_done = DataflowJobStatusSensor(
    task_id="wati_until_dataflow_job_done",
    job_id="{{ti.xcom_pull(task_ids='get_job_id', key='dataflow_job_id')}}",
    expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
    project_id=project_id,
    location="us-central1",
    poke_interval=30, 
    timeout=600,
)

# Task 6: Export results from BigQuery to GCS
export_results = BigQueryToGCSOperator(
    task_id='export_results',
    source_project_dataset_table='{{ task_instance.xcom_pull(task_ids="gen_temp_table_name", key="temp_table_name") }}',
    destination_cloud_storage_uris=[f'gs://{bucket}/output/results_*.json'],
    export_format='JSON',
    dag=dag
)

# Task 7: Delete the input events file
delete_input_file = GCSDeleteObjectsOperator(
    task_id='delete_input_file',
    bucket_name=bucket,
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

# Define task dependencies
wait_for_file >> gen_temp_table_name >> run_dataflow
run_dataflow >> get_job_id >> wati_until_dataflow_job_done
wati_until_dataflow_job_done >> export_results >> delete_input_file
delete_input_file >> drop_temp_table