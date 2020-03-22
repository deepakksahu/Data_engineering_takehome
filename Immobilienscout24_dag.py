import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator \
    import KubernetesPodOperator
from airflow.models import Variable

DEFAULT_ARGS = {
    'owner': 'de',
    'email': 'deepaksahu092@gmail.com',
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 3, 17)
}

IMAGE_CONFIG = Variable.get('crosslend_images_config', deserialize_json=True)
CONFIG = Variable.get('immobilienscout24_conf',
                      deserialize_json=True)

with DAG(
        'flat-data-ingestion',
        default_args=DEFAULT_ARGS,
        schedule_interval='0 0  * * *'
) as dag:
    KubernetesPodOperator(
        namespace='Crosslend_Dataengineering',
        image=IMAGE_CONFIG['flat-data-ingestion'],
        cmds=["python", "main.py",
              "--config", json.dumps(CONFIG)],
        name="flat-data-ingestion",
        task_id="flat-data-ingestion",
        in_cluster=True
    )
