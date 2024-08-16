from datetime import datetime, timedelta
from airflow import DAG
# Missing imports?
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import time
import random
import airflow
from airflow.exceptions import AirflowFailException

# Define the default arguments for the DAG
default_args = {
	'start_date': airflow.utils.dates.days_ago(0),
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
    
}

# Initialize the DAG
dag = DAG(
   'test_dag',
   default_args=default_args,
	schedule_interval = None,
	catchup = False,
)

# Define Python function for a task
def print_hello():
   # Step blowing up
   raise AirflowFailException("Something blew up!")
   return 'Hello from Airflow!'

t1 = BashOperator(
   task_id='sleep',
   bash_command='sleep 5',
   dag=dag,
)

t2 = PythonOperator(
   task_id='print_hello',
   python_callable=print_hello,
   dag=dag,
)

# Task 3: Print the date
t3 = BashOperator(
   task_id='print_date',
   bash_command='date',
   dag=dag,
)
# Set task dependencies
t1 >> t2 >> t3



