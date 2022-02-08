from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

dag = DAG(
    dag_id="test_spark",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval="@once", #  한번만실행
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
    --num-executors 2 \
    --executor-memory 2G \
    --executor-cores 2 \
    --conf spark.yarn.am.waitTime=900000 \
    --conf spark.core.connection.ack.wait.timeout=600s \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.default.parallelism=4 \
    /home/workspace/app.py 
'''

# bash_command = '''spark-submit'''

# bash_command = "source ~/.bashrc /home/workspace/submit.sh "

# t1 = DockerOperator(
#     task_id='task_docker',
#     image='docker.io/bitnami/spark:3',
#     container_name='spark',
#     api_version='auto',
#     auto_remove=True,
#     command=bash_command,
#     # docker_url='tcp://localhost:8080',
#     # newtwork_mode='bridge',
#     dag=dag,
# )

t1 = SSHOperator(
    task_id='task_ssh',
    ssh_conn_id='ssh_default',
    environment={'JAVA_HOME':'/opt/bitnami/java','PYSPARK_PYTHON':'/opt/bitnami/python/bin/python3.8'},
    command=bash_command
)

start_dag >> t1 >> end_dag