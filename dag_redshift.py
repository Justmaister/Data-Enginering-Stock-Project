import datetime
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.create_redshift_cluster import CreateRedshiftCluster

def start_dag():
    logging.info("Starting DAG")

default_args = {
    'owner': 'justmaister',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False
}

dag = DAG(
    "finance_dag_redshift",
    default_args = default_args,
    schedule_interval='@once',
    max_active_runs=1,
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
    )

start_dag_task = PythonOperator(
    task_id="start_dag",
    python_callable=start_dag,
    dag=dag)

create_redshift_cluster_task = CreateRedshiftCluster(
    task_id="create_redshift_cluster",
    dag=dag,
    clustertype= 'multi-node',
    nodetype= 'dc2.large',
    numberofnodes= '4',
    execution_timeout=timedelta(minutes=5)
)

start_dag_task >> create_redshift_cluster_task
