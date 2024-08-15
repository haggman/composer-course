from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='xcom_demo',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
)

def extract_data_from_api(**kwargs):
    # Simulating an API call
    data = {"key1": "value1", "key2": "value2"}
    
    # Push the data to XCom using the TaskInstance (ti)
    kwargs['ti'].xcom_push(key='api_data', value=data)
    # Another option is to simply:
    # return data


extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_api,
    dag=dag,
)

def transform_data(**kwargs):
    # Pull the data from XCom
    ti = kwargs['ti']
    data = ti.xcom_pull(key='api_data', task_ids='extract_data')
    # If the data is returned by the upstream task, just skip the key
    # data = ti.xcom_pull(task_ids='extract_data')
    
    # Process the data (e.g., add a new key-value pair)
    transformed_data = {**data, "key3": "value3"}
    
    # Push the transformed data to XCom
    ti.xcom_push(key='transformed_data', value=transformed_data)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

def load_data_to_gcs(**kwargs):
    # Pull the transformed data from XCom
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    
    # Here you would write the logic to store the data in Google Cloud Storage
    # For simplicity, we're just printing the data
    print("Storing data to GCS:", transformed_data)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_gcs,
    dag=dag,
)

extract_data >> transform_data >> load_data
