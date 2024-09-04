generate_content_task = GenerativeModelGenerateContentOperator(
        task_id="generate_content_task",
        project_id="your-project",
        location="your-location",
        contents=[
            "Explain how to integrate Generative AI into an Airflow DAG in 25 words or less."
        ],
        pretrained_model="gemini-1.5-pro",
    )

from airflow.exceptions import AirflowException

def my_task():
    try:
        # Your task logic here
        result = some_operation()
    except SomeSpecificException as e:
        logger = kwargs['ti'].log
        logger.error(f"Operation failed: {str(e)}")
        raise AirflowException("Task failed due to a specific error")
