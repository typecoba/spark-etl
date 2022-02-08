from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="test",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval="* * * * *", # 
    tags=['test'],
)


def print_message(text:str=''):
    print('text')
    return 'text'


t1 = PythonOperator(
    task_id='task_python_operator',
    python_callable=print_message,
    dag=dag,
)

t2 = BashOperator(
    task_id="task_bash_operator",
    bash_command='date',
    dag=dag,
)
