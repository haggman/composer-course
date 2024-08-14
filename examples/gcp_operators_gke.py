from datetime import datetime, timedelta
from airflow import DAG
import logging
import airflow

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

from airflow.models import Variable
project_id = Variable.get("project_id")


default_args = {
	'start_date': '2024-01-01',
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'gcp_operators_gke',
    default_args=default_args,
    description='Experimenting with Airflow GCP operators for GKE.',
    # Manual start
	schedule_interval = None,
	catchup = False,
)

from airflow.providers.google.cloud.operators.kubernetes_engine import GKECreateClusterOperator

create_gke_cluster = GKECreateClusterOperator(
    task_id='create_gke_cluster',
    project_id=project_id,
    location='us-central1-a',
    body={
        "name": "demo-cluster",
        "initial_node_count": 3,
        "node_config": {
            "machine_type": "e2-standard-4",
            "disk_size_gb": 100,
        },
    },
    dag=dag,
)

from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator

deploy_nginx = GKEStartPodOperator(
    task_id='deploy_nginx',
    location='us-central1-a',
    cluster_name='demo-cluster', 
    name='nginx-pod',
    namespace='default',
    image='nginx:latest',
    cmds=["nginx", "-g", "daemon off;"],
    project_id=project_id,
    dag=dag,
)




download_deployment_file >> apply_yaml_task