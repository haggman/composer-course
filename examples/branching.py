from datetime import datetime, timedelta
from airflow import DAG
import logging
import airflow

from airflow.operators.dummy_operator import DummyOperator

logger = logging.getLogger(__name__)

default_args = {
	'start_date': '2024-01-01',
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'branching',
    default_args=default_args,
    description='Experimenting with Airflow branching.',
    # Manual start
	schedule_interval = None,
	catchup = False,
)

task1 = DummyOperator(task_id='task_1', dag=dag, )
task2 = DummyOperator(task_id='task_2', dag=dag, )
task3 = DummyOperator(task_id='task_3', dag=dag, )
task4 = DummyOperator(task_id='task_4', dag=dag, )

from airflow.operators.python import BranchPythonOperator

def branch_func():
    if True:
        return 'task_1'
    else:
        return 'task_2'

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=branch_func,
    dag=dag
)
# The branching operator makes the decision, task1
branching >> [task1, task2]
# The task 1 branch executes and continues here
task1 >> task3
# The task two branch is skipped, so is this branch
task2 >> task4