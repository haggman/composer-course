from datetime import datetime, timedelta
from airflow import DAG
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
    'example_dag',
    default_args=default_args,
    description='A simple introductory DAG',
	 schedule_interval = '*/1 * * * *',
	 catchup = False,
)

# Define Python function for a task
# Print a value or throw an error
def print_hello():
	# Generate a random number between 0 and 99 (inclusive)
	random_number = random.randint(0, 99)
	# 10% chance of raising an error
	if random_number < 10:
		raise AirflowFailException("Random error triggered")

	return 'Hello from Airflow!'

# Task 1: Print hello
t1 = PythonOperator(
   task_id='print_hello',
   python_callable=print_hello,
   dag=dag,
)

# Task 2: Sleep for 5 seconds
t2 = BashOperator(
   task_id='sleep',
   bash_command='sleep 5',
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

