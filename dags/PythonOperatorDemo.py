from datetime import datetime
from airflow.models import Dag, Variable
from airflow.operators.python_operator import PythonOperator
from bots.Python_Helper import call
# from airflow.operators.dummy_operator import DummyOperator



default_args={

    'owner': 'ShowBeyondDoubt',
    'start_date': datetime(4, 11),
}

with DAG(

    dag_id="PythonOperatorDemo",
    start_date=datetime(2024, 4, 11),
    default_args=default_args,
    schedule_interval=None,
    # catchup=False,
) as dag:

    start_dag = PythonOperator(
        task_id='BashOperatorDemo',
        python_callable=call,
    )

    start_dag

    # def print_context(ds, **kwargs):
    #     print(kwargs)
    #     print(ds)
    #     return 'Whatever you return gets printed in the logs'

    # run_this = PythonOperator(
    #     task_id='print_the_context',
    #     provide_context=True,
    #     python_callable=print_context,
    # )

    # run_this