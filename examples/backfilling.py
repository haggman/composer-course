from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from airflow.models import Variable
project_id = Variable.get("project_id")

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='backfilling_example',
    default_args=default_args,
    schedule_interval='@daily',  # DAG will run daily
    start_date=datetime(2024, 8, 1),  # Backfilling will start from this date
    catchup=True,  # Enables backfilling for missed runs
    max_active_runs=4,  # Limit the number of active runs
)

def print_execution_date(**kwargs):
    print(f"Execution date is: {kwargs['execution_date']}")

# Define the task
print_date_task = PythonOperator(
    task_id='print_execution_date',
    python_callable=print_execution_date,
    provide_context=True,
    dag=dag,
)

print_date_task
