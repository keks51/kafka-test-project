import os
import sys
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG


# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

with DAG(
        'kafka-producer',
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple spark tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example-spark'],
        is_paused_upon_creation=False
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    #
    # dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    # dag.doc_md = """
    # This is a documentation placed anywhere
    # """  # otherwise, type it like this

    t2 = BashOperator(
        task_id='java',
        bash_command = 'java -cp /opt/airflow/jars/kafka-app.jar com.test_task.MainApp producer localhost:9092'
    )

    t1 >> t2
