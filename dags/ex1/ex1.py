from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'first_dag',
    default_args=default_args,
    description='My first Airflow DAG with an empty operator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(
    task_id="start_task",
    dag=dag
)

intermediate_task = EmptyOperator(
    task_id="intermediate_task",
    dag=dag
)

second_intermediate_task = EmptyOperator(
    task_id="second_intermediate_task",
    dag=dag
)

end_task = EmptyOperator(
    task_id="end_task",
    dag=dag
)

send_line_message = EmptyOperator(
    task_id="send_line_message",
    dag=dag
)

send_ms_team = EmptyOperator(
    task_id="send_ms_team",
    dag=dag
)

send_email = EmptyOperator(
    task_id="send_email",
    dag=dag
)

start_task >> [intermediate_task, second_intermediate_task] >> end_task >> [send_line_message, send_ms_team]
second_intermediate_task >> send_email