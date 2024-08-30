from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

import airflow

# Define the default arguments for the DAG
default_args = {
	'start_date': airflow.utils.dates.days_ago(0),
	'retries': 3,
	'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    'dag_structure',
    default_args=default_args,
    description='A simple introductory DAG',
	schedule_interval = timedelta(days=1),
	catchup = False,
) as dag:

    # Function to do a little work in Python
    def print_hello():
        print("Hello World!")

    # Operators represent building blocks in your pipelines
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
        dag=dag,
    )

    def print_goodbye():
        print("Goodbye World!")

    goodbye_task = PythonOperator(
        task_id='goodbye_task',
        python_callable=print_goodbye,
        dag=dag,
    )

    with TaskGroup('extract_load_tasks') as extract_load_tasks:
        extract = DummyOperator(task_id='extract')
        load = DummyOperator(task_id='load')

        extract >> load
    
    def my_python_function():
        try:
            print('some code that might raise an exception')
        except Exception as e:
            print(f"An error occurred: {e}")

    task3 = PythonOperator(
        task_id='python_task_with_error_handling',
        python_callable=my_python_function,
        retries=2,  # Retry the task twice if it fails
    )

    cool_variable = Variable.get("cool_variable")

    task4 = BashOperator(
        task_id='bash_task_with_variable',
        bash_command=f'echo {cool_variable}',
    )

# Set task dependencies to put your operators together
hello_task >> extract_load_tasks >> task3 >> task4 >> goodbye_task
