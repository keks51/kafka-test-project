import sys
from datetime import datetime, timedelta

from airflow import DAG

sys.path.insert(0, "/opt/airflow/plugins/")

from airflow.operators.bash import BashOperator
from spark_operator_plugin import SparkSubmitOperator


with DAG(
        'kafka_app_spark',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='',
        schedule_interval='*/15 * * * *',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example-spark'],
        is_paused_upon_creation=True
) as dag:
    t1 = BashOperator(
        task_id='starting',
        bash_command='date',
    )
    t2 = SparkSubmitOperator(
        application_file='/opt/airflow/jars/kafka-app.jar',
        master='spark://0.0.0.0:7077',
        deploy_mode='client',
        application_args = 'spark localhost 5433 root root kafka_app offsets kafka_spark_app localhost:9092 orders hdfs://localhost:8020 /raw orders',
        task_id='spark_submit_task'
    )

    t1 >> t2
