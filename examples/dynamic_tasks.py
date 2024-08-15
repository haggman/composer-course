from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from airflow.models import Variable
project_id = Variable.get("project_id")

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dynamic_tasks_example',
    default_args=default_args,
    schedule_interval = None,
	catchup = False,
    start_date=datetime(2024, 1, 1),
) as dag:

    def create_dynamic_tasks():
        previous_task = None
        for i in range(5):
            task = PythonOperator(
                task_id=f'dynamic_task_{i}',
                python_callable=lambda x: print(f"Executing task {x}"),
                op_kwargs={'x': i},
                dag=dag,
            )
            
            if previous_task:
                previous_task >> task

            previous_task = task

    create_dynamic_tasks()