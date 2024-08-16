generate_content_task = GenerativeModelGenerateContentOperator(
        task_id="generate_content_task",
        project_id="your-project",
        location="your-location",
        contents=[
            "Explain how to integrate Generative AI into an Airflow DAG in 25 words or less."
        ],
        pretrained_model="gemini-1.5-pro",
    )