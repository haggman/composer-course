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
    'trigger_rules',
    default_args=default_args,
    description='Experimenting with trigger rules.',
    # Manual start
	schedule_interval = None,
	catchup = False,
)

task1 = DummyOperator(task_id='task_1', dag=dag, )
task2 = DummyOperator(task_id='task_2', dag=dag, )
task3 = DummyOperator(
    task_id='task_3', 
    dag=dag, 
    trigger_rule='all_success'
)
task4 = DummyOperator(
    task_id='task_4', 
    dag=dag, 
    trigger_rule='one_failed',
)

# task3 will run only if both task1 and task2 succeed
task1 >> task2 >> task3 >> task4 
# task4 will run if any tasks1-3 fail