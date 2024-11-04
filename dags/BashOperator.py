from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator



default_args = {
    'owner': 'Faidiq',
    'start_date': datetime(2024, 5, 11),
    # 'depends_on_past': False,
}

with DAG(
    dag_id= 'BashOperator',
    default_args=default_args,
    schedule_interval=None,
    tags=['Demo1']) as dag:

    start_dag = BashOperator(
        task_id='task_new',
        bash_command="date"
    )

    start_dag

