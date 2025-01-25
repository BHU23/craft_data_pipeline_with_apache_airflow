from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, #ดู dag ก่อนหน้าไหม
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # ry'
    'retry_delay': timedelta(minutes=5), # what time it wait
}

dag = DAG(
    'first_dag', # name, it shound same file name
    default_args=default_args, #set args
    description='My first Airflow DAG with an empty operator',
    schedule_interval=timedelta(days=1),# run start_date + 1
    start_date=datetime(2023, 1, 1),
    catchup=False, # get ข้อมูลย้อนหลัง
)

def hello_world():
    print("hello world")

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

hello_world = PythonOperator(
    task_id='hello-world',
    dag=dag,
    python_callable=hello_world
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

start >> hello_world >> end