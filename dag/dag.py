import datetime
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.get_symbol_list import GetSymbolList
from airflow.operators.get_financial_report_request import GetFinancialReportRequest
from airflow.operators.create_redshift_cluster import CreateRedshiftCluster
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.stage_to_redshift import StageToRedshiftOperator
from airflow.operators.insert_into_final_data import UpsertData
from airflow.operators.data_quality_check import DataQualityOperator
from airflow.helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'justmaister',
    'depends_on_past': False,
    'retries': 1,
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

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

symbol_list_sts = GetSymbolList(
    task_id="get_symbol_list_sts",
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

balance_sheet = GetFinancialReportRequest(
    task_id = "get_balance_sheet",
    dag = dag,
    report = 'balance_sheet',
    period = 'annual',
    ftype = 'full'
)

cash_flow = GetFinancialReportRequest(
    task_id = "get_cash_flow",
    dag = dag,
    report = 'cash_flow',
    period = 'annual',
    ftype = 'full'
)

income_statement = GetFinancialReportRequest(
    task_id = "get_income_statement",
    dag = dag,
    report = 'income_statement',
    period = 'annual',
    ftype = 'full'
)

financial_ratios = GetFinancialReportRequest(
    task_id = "get_financial_ratios",
    dag = dag,
    report = 'financial_ratios',
    period = 'annual',
    ttm = False
)

key_metrics = GetFinancialReportRequest(
    task_id = "get_key_metrics",
    dag = dag,
    report = 'key_metrics',
    period = 'annual'
)

enterprise_value = GetFinancialReportRequest(
    task_id = "get_enterprise_value",
    dag = dag,
    report = 'enterprise_value',
    period = 'annual'
)

symbol_list = GetFinancialReportRequest(
    task_id = "get_symbol_list",
    dag = dag,
    report = 'symbol_list',
    period = 'annual'
)

create_redshift_cluster_task = CreateRedshiftCluster(
    task_id="create_redshift_cluster",
    dag=dag,
    clustertype= 'multi-node',
    nodetype= 'dc2.large',
    numberofnodes= '2',
    execution_timeout=timedelta(minutes=5)
)

create_stg_tables = PostgresOperator(
    task_id="create_stg_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_stg_tables
)

stage_balance_sheet_copy = StageToRedshiftOperator(
    task_id="balance_sheet_copy",
    dag=dag,
    table="balance_sheet_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="balance_sheet/annual",
    data_format="CSV",
    region ="us-west-2"
)

stage_cash_flow_copy = StageToRedshiftOperator(
    task_id="cash_flow_copy",
    dag=dag,
    table="cash_flow_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="cash_flow/annual",
    data_format="CSV",
    region ="us-west-2"
)

stage_income_statement_copy = StageToRedshiftOperator(
    task_id="income_statement_copy",
    dag=dag,
    table="income_statement_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="income_statement/annual",
    data_format="CSV",
    region ="us-west-2"
)

stage_financial_ratios_copy = StageToRedshiftOperator(
    task_id="financial_ratios_copy",
    dag=dag,
    table="financial_ratios_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="financial_ratios/annual",
    data_format="CSV",
    region ="us-west-2"
)

stage_key_metrics_copy = StageToRedshiftOperator(
    task_id="key_metrics_copy",
    dag=dag,
    table="key_metrics_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="key_metrics/annual",
    data_format="CSV",
    region ="us-west-2"
)

stage_enterprise_value_copy = StageToRedshiftOperator(
    task_id="enterprise_value_copy",
    dag=dag,
    table="enterprise_value_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="enterprise_values/annual",
    data_format="CSV",
    region ="us-west-2"
)

stage_symbol_list_copy = StageToRedshiftOperator(
    task_id="symbol_list_copy",
    dag=dag,
    table="symbol_list_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="symbol_list.csv",
    data_format="CSV",
    region ="us-west-2"
)

create_final_tables = PostgresOperator(
    task_id="create_final_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_final_tables
)

upsert_balance_sheet = UpsertData(
    task_id='Upsert_balance_sheet_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="balance_sheet",
    sql=SqlQueries.upsert_balance_sheet
)

upsert_cash_flow = UpsertData(
    task_id='Upsert_cash_flow_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="cash_flow",
    sql=SqlQueries.upsert_cash_flow
)

upsert_income_statement = UpsertData(
    task_id='upsert_income_statement_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="income_statement",
    sql=SqlQueries.upsert_income_statement
)

upsert_financial_ratios = UpsertData(
    task_id='upsert_financial_ratios_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="financial_ratios",
    sql=SqlQueries.upsert_financial_ratios
)

upsert_key_metrics = UpsertData(
    task_id='upsert_key_metrics_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="key_metrics",
    sql=SqlQueries.upsert_key_metrics
)

upsert_enterprise_value = UpsertData(
    task_id='upsert_enterprise_value_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="enterprise_value",
    sql=SqlQueries.upsert_enterprise_value
)

upsert_symbol_list = UpsertData(
    task_id='upsert_symbol_list_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="symbol_list",
    sql=SqlQueries.upsert_symbol_list
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables={"balance_sheet":"symbol","cash_flow":"symbol","income_statement":"symbol","financial_ratios":"symbol","key_metrics":"symbol", "enterprise_value":"symbol", "symbol_list":"symbol"}
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

## Task Dependencies

start_operator >> symbol_list_sts >> [balance_sheet, cash_flow, income_statement, financial_ratios, key_metrics, enterprise_value, symbol_list]

[balance_sheet, cash_flow, income_statement, financial_ratios, key_metrics, enterprise_value, symbol_list] >> create_redshift_cluster_task >> create_stg_tables

create_stg_tables >> [stage_balance_sheet_copy, stage_cash_flow_copy, stage_income_statement_copy, stage_financial_ratios_copy, stage_key_metrics_copy, stage_enterprise_value_copy, stage_symbol_list_copy] >> create_final_tables

create_final_tables >> [upsert_balance_sheet, upsert_cash_flow, upsert_income_statement, upsert_financial_ratios, upsert_key_metrics, upsert_enterprise_value, upsert_symbol_list] >> run_quality_checks >> end_operator
