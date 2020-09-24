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
    'email': "prakshal.jain@clairvoyantsoft.com",
    'email_on_retry': False,
    'email_on_failure': True,
}
dag = DAG(
    dag_id='email_test', default_args=args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)


bash_operator = BashOperator(
    task_id='role', bash_command="exit 1", dag=dag)
