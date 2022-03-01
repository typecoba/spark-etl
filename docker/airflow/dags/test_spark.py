from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

dag = DAG(
    dag_id="test_spark",
    start_date=datetime(2021, 1, 1), # 시작시간
    catchup=False, # 과거스케쥴 실행(backfill)을 사용하지 않도록
    # schedule_interval="@once", #  한번만실행
    schedule_interval="0 */1 * * *",
    max_active_runs=3,
    tags=['test','spark'],
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

end_dag = DummyOperator(
    task_id='end_dag'
)


bash_command = '''    
/opt/bitnami/spark/bin/spark-submit \
    --master local[6] \
    --num-executors 3 \
    --executor-memory 8G \
    --executor-cores 2 \
    --conf spark.yarn.am.waitTime=900000 \
    --conf spark.core.connection.ack.wait.timeout=600s \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.default.parallelism=4 \
    /home/workspace/app.py 
'''


t1 = SSHOperator(
    task_id='task1_ssh',
    ssh_conn_id='ssh_default',
    environment={'JAVA_HOME':'/opt/bitnami/java','PYSPARK_PYTHON':'/opt/bitnami/python/bin/python3.8'},
    command=bash_command
)

start_dag >> t1 >> end_dag