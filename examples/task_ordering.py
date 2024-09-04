from datetime import datetime, timedelta
from airflow import DAG
import logging
import airflow

from airflow.operators.dummy_operator import DummyOperator

default_args = {
	'start_date': '2024-01-01',
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'task_ordering',
    default_args=default_args,
    description='Experimenting with Airflow task ordering.',
    # Manual start
	schedule_interval = None,
	catchup = False,
)

task1 = DummyOperator(task_id='task_1', dag=dag, )
task2 = DummyOperator(task_id='task_2', dag=dag, )
task3 = DummyOperator(task_id='task_3', dag=dag, )
# task4 and 5 start with task 1, in parallel
task4 = DummyOperator(task_id='task_4', dag=dag, )
task5 = DummyOperator(task_id='task_5', dag=dag, )
# task1 executes before task2
task1 >> task2
# task3 executes after task2
task3 << task2
# from airflow.models.baseoperator import chain

# chain(task1, task2, task3)

# An alternative to << and >> is 
# set_upstream and set_downstream
# For tasks that should run in parallel, use [ , ]
task3.set_downstream([task4, task5])
#task3.set_upstream([task1, task2])
