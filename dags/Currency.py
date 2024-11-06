import os
import json
from airflow import DAG
from datetime import datetime, timedelta

# Import custom operators
from bots.custom_http_operator import CustomHttpOperator
from bots.SimpleHttpOperator import CustomMongoOperator 

# Define the on_failure_callback
def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

with DAG(
    dag_id="Mongo",
    schedule_interval=None,
    start_date=datetime(2024, 5, 11),
    catchup=False,
    tags=["Submission"],
    default_args={
        "owner": "Faisal Shidiq2",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'on_failure_callback': on_failure_callback
    }
) as dag:

    task1 = CustomHttpOperator(
        task_id='get_data',
        method='GET',
        endpoint='2024-01-01..2024-06-30',
        headers={"Content-Type": "application/json"},
        do_xcom_push=True, dag=dag
    )

    task2 = CustomMongoOperator(
        task_id='upload_to_mongodb',
        mongo_conn_id='mongodb+srv://Sal:admin123@sal.94xhv.mongodb.net/',
        database='etl',
        collection='data'
    )

    task1 >> task2
