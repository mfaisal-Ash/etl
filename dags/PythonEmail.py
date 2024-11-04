from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from bots.EmailHelper import send

default_args = {
    'owner': 'Sidiq',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 11),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
}

with DAG(
    dag_id='PythonDemoEmail',
    default_args=default_args,
    schedule_interval=None) as dag:

    start_dag = PythonOperator(
        task_id='start_dag',
        python_callable=send
    )

    start_dag