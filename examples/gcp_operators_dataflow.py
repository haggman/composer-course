from datetime import datetime, timedelta
from airflow import DAG
import logging
import airflow

logger = logging.getLogger(__name__)

from airflow.models import Variable
project_id = Variable.get("project_id")


default_args = {
	'start_date': '2024-01-01',
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'gcp_operators_dataflow',
    default_args=default_args,
    description='Experimenting with Airflow GCP operators for Dataflow.',
    # Manual start
	schedule_interval = None,
	catchup = False,
)

from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=project_id,
    project_id=project_id, 
    dag=dag
)

from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator

run_dataflow = DataflowTemplatedJobStartOperator(
    task_id='dataflow_job',
    template='gs://dataflow-templates/latest/Word_Count',
    parameters={
        'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
        'output': f'gs://{project_id}/wordcount-output'
    },
    dag=dag
)

create_bucket >> run_dataflow

from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

run_beam_pipeline = BeamRunPythonPipelineOperator(
    task_id='run_beam_pipeline',
    py_file='gs://your-gcs-bucket/path/to/your/beam/pipeline.py',
    py_options=[], 
    pipeline_func='your_pipeline_function',  
    runner='DataflowRunner', 
    temp_location='gs://your-temp-bucket/temp',  
    dag=dag
)