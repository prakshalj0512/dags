import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator



args = {
    'owner': 'Prakshal Jain',
    'start_date': datetime(2020, 7, 7, 15, 1, 0),
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    'email': "abcd@gmail.com",
    'email_on_retry': False,
    'email_on_failure': False,
}
dag = DAG(
    dag_id='aa_timeout', default_args=args,
    schedule_interval='* * * * *',
    dagrun_timeout=timedelta(seconds=90),
    catchup=False,
    max_active_runs=1,
    tags="apple")


bash_operator = BashOperator(
    task_id='aa_timeout', bash_command="sleep 30s", dag=dag)
