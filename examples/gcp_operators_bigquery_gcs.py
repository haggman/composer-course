from datetime import datetime, timedelta
from airflow import DAG
import logging
import airflow


from airflow.models import Variable
project_id = Variable.get("project_id")


default_args = {
	'start_date': '2024-01-01',
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'gcp_operators_bigquery',
    default_args=default_args,
    description='Experimenting with Airflow GCP operators for BigQuery.',
    # Manual start
	schedule_interval = None,
	catchup = False,
)

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryExecuteQueryOperator

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset", 
    project_id=project_id, 
    dataset_id='demo_dataset',
    location='US',
    if_exists='skip',
    dag=dag,
)

create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    project_id=project_id, 
    dataset_id='demo_dataset',
    location='US',
    if_exists='skip',
    table_id="top_page_views",
    schema_fields=[
        {"name": "title", "type": "STRING", "mode": "REQUIRED"},
        {"name": "views", "type": "INTEGER", "mode": "NULLABLE"},
    ],
)

run_query = BigQueryExecuteQueryOperator(
    task_id='run_query',
    trigger_rule='all_done',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    location='US',
    sql="""
SELECT
  title,
  SUM(views) AS views
FROM
  `bigquery-public-data.wikipedia.pageviews_2024`
WHERE
  DATE(datehour) BETWEEN "2024-01-01"
  AND "2024-01-31"
GROUP BY
  title
ORDER BY
  views DESC
LIMIT
  100
        """,
    destination_dataset_table=f'{project_id}.demo_dataset.top_page_views',
    dag=dag
)

from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=project_id,
    project_id=project_id, 
    dag=dag
)

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# Export the results to Cloud Storage
export_to_gcs = BigQueryToGCSOperator(
    task_id='export_to_gcs',
    source_project_dataset_table=f'{project_id}.demo_dataset.top_page_views',
    destination_cloud_storage_uris=[f'gs://{project_id}/page_views.csv'],
    export_format='CSV',
    trigger_rule='all_done',
    dag=dag,
)
create_dataset >> create_table >> run_query
run_query >> create_bucket >> export_to_gcs
