from airflow import DAG
from airflow.models import DagBag, DagRun, Variable, XCom
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.configuration import conf
import os
import requests

OWNER = "Prakshal Jain"

EMAIL_ON_RETRY = False
EMAIL_ON_FAILURE = False
RETRIES = 0
RETRY_DELAY_IN_SECONDS = 60
SLA_MISS_ALERT_IN_SECONDS = 90
CATCHUP = False

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
TASK_ID = "rldp_zabbix_monitoring"
SCHEDULE_INTERVAL = None
MAX_ACTIVE_RUNS = 1

ZABBIX_SERVER = "zabbix-dev.llnw.com"

AIRFLOW_HOSTS = str(conf.get("scheduler_failover",
                             "scheduler_nodes_in_cluster")).split(",")


def airflow_jobs_status(zabbix_state):

    dag_ids = []

    for dag in DagBag().dags.values():

        for tag in dag.get_dagtags():

            if (tag.name == zabbix_state):
                dag_ids.append(dag.dag_id)

    return_value = 0
    print(dag_ids)
    for dag_id in dag_ids:
        dag_runs = DagRun.find(dag_id=dag_id)
        if (len(dag_runs) > 0):
            print(f"{dag_id}: {dag_runs[-1].state}")
            if (dag_runs[-1].state == "failed"):
                return_value = 1
        else:
            print(f"{dag_id}: No runs present")

    return return_value


DEFAULT_ARGS = {
    'owner': OWNER,
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 7, 6, 0, 0),
    'email': "prakshal.jain@clairvoyantsoft.com",
    'email_on_retry': EMAIL_ON_RETRY,
    'email_on_failure': EMAIL_ON_FAILURE,
    'retries': RETRIES,
    'retry_delay': timedelta(seconds=RETRY_DELAY_IN_SECONDS),
    'sla': timedelta(seconds=SLA_MISS_ALERT_IN_SECONDS)
}

dag = DAG(
    dag_id=DAG_ID,
    catchup=CATCHUP,
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
)

states = ["INFO", "WARN", "CRITICAL"]

start = DummyOperator(task_id="Start", dag=dag)

COMMAND = "python /var/lib/airflow/scripts/send_to_zabbix.py"
AIRFLOW_HOSTS = ','.join(AIRFLOW_HOSTS)


for state in states:
    job_status_task = PythonOperator(
        python_callable=airflow_jobs_status,
        op_kwargs={"zabbix_state": state},
        task_id=f"Airflow_Status_Check_{state}",
        xcom_push=True,
        dag=dag)

    zabbix_sender_task = BashOperator(
        bash_command=f'python /var/lib/airflow/scripts/send_to_zabbix.py --hostname {AIRFLOW_HOSTS} --zabbix-server {ZABBIX_SERVER} --trigger-key rldp.airflow.{str.lower(state)} --trigger-value {{{{ ti.xcom_pull("Airflow_Status_Check_{state}") }}}}',
        task_id=f"Zabbix_Sender_{state}",
        xcom_push=True,
        dag=dag)

    start >> job_status_task >> zabbix_sender_task
