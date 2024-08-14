from datetime import datetime, timedelta
from airflow import DAG
import logging
import airflow


# Define the default arguments for the DAG
default_args = {
	'start_date': '2024-01-01',
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

# Initialize the DAG
dag = DAG(
    'common_operators',
    default_args=default_args,
    description='Example of common Airflow operators',
    # Manual start
	schedule_interval = None,
	catchup = False,
)



from airflow.operators.bash_operator import BashOperator

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello from Bash!"',
    dag=dag,
)

from airflow.operators.dummy_operator import DummyOperator

placeholder_task = DummyOperator(
    task_id='placeholder_task',
    dag=dag,
)

from airflow.operators.python import PythonOperator

def my_python_function(x, y):
    answer = x + y
    logger.info(f"Answer: {answer}")

calculate_sum = PythonOperator(
    dag=dag,
    task_id='calculate_sum',
    python_callable=my_python_function,
    op_kwargs={'x': 10, 'y': 20},
)

from airflow.operators.email import EmailOperator
# Don't forget to configure the email settings in
# your Airflow instance
send_email = EmailOperator(
    task_id='send_email',
    to='patrick@roitraining.com',
    subject='Hello from Airflow',
    html_content='<b>Airflow is awesome!</b',
)

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MyCustomOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_parameter, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.my_parameter = my_parameter

    def execute(self, context):
        # Your custom logic here
        print(f"Executing with parameter: {self.my_parameter}")

custom_task = MyCustomOperator(
    task_id='custom_task',
    my_parameter='custom_value',
    dag=dag,
)

# Put them all together (excluding mail, since it's not configured)
bash_task >> placeholder_task >> calculate_sum >> custom_task