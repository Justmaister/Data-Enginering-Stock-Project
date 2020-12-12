import datetime
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.get_symbol_list import GetSymbolList
from airflow.operators.get_financial_report import GetFinancialReport

def start_dag():
    logging.info("Starting DAG")

def addition():
    logging.info(f"2 + 2 = {2+2}")


default_args = {
    'owner': 'justmaister',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False
}

dag = DAG(
    "finance_dag",
    default_args = default_args,
    schedule_interval='@once',
    max_active_runs=1,
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
    )

start_dag_task = PythonOperator(
    task_id="start_dag",
    python_callable=start_dag,
    dag=dag)

symbol_list = GetSymbolList(
    task_id="get_symbol_list",
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

balance_sheet = GetFinancialReport(
    task_id = "get_balance_sheet",
    dag = dag,
    report = 'balance_sheet',
    period = 'annual',
    ftype = 'full'
)

cash_flow = GetFinancialReport(
    task_id = "get_cash_flow",
    dag = dag,
    report = 'cash_flow',
    period = 'annual',
    ftype = 'full'
)

income_statement = GetFinancialReport(
    task_id = "get_income_statement",
    dag = dag,
    report = 'income_statement',
    period = 'annual',
    ftype = 'full'
)

financial_ratios = GetFinancialReport(
    task_id = "get_financial_ratios",
    dag = dag,
    report = 'financial_ratios',
    period = 'annual',
    ttm = False
)

key_metrics = GetFinancialReport(
    task_id = "get_key_metrics",
    dag = dag,
    report = 'key_metrics',
    period = 'annual'
)

enterprise_value = GetFinancialReport(
    task_id = "get_enterprise_value",
    dag = dag,
    report = 'enterprise_value',
    period = 'annual'
)

company_profile = GetFinancialReport(
    task_id = "get_company_profile",
    dag = dag,
    report = 'company_profile'
)

addition_task = PythonOperator(
    task_id="addition",
    python_callable=addition,
    dag=dag)


start_dag_task >> symbol_list >> [balance_sheet, cash_flow, income_statement, financial_ratios, key_metrics, enterprise_value, company_profile]

[balance_sheet, cash_flow, income_statement, financial_ratios, key_metrics, enterprise_value, company_profile] >> addition_task
