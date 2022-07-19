from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from vtex import extractions

with DAG(
        default_args={
            'retries': 3,
            'retry_delay': timedelta(minutes=1),
        },
        dag_id='vtex', schedule_interval=None, start_date=datetime(2022, 5, 1), catchup=False) as dag:
    task_start = DummyOperator(task_id='start')
    task_end = DummyOperator(task_id='end')

    with TaskGroup(group_id='extraction') as task_extraction:
        brand_operator = extractions.BrandExtraction(task_id='brand_extraction')

    task_start >> task_extraction >> task_end
