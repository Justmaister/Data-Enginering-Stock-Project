from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import os
from pyfmpcloud import settings
from pyfmpcloud import company_valuation as cv
from pyfmpcloud import stock_time_series as sts

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key
os.environ['AWS_ACCESS_KEY_ID']=access_key
os.environ['AWS_SECRET_ACCESS_KEY']=secret_key

API_KEY = Variable.get('API_KEY')
settings.set_apikey(API_KEY)
bucket = Variable.get('s3_bucket')

import awswrangler as wr

import pandas as pd

"""
import pandas as pd
import findspark
findspark.init('/home/just/spark-3.0.0-bin-hadoop2.7')
from pyspark.sql import SparkSession
"""

class GetFinancialReport(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 report = "",
                 period = None,
                 ftype = None,
                 ttm = None,
                 *args, **kwargs):

        super(GetFinancialReport, self).__init__(*args, **kwargs)
        self.report = report
        self.period = period
        self.ftype = ftype
        self.ttm = ttm

    def execute(self, context):
        self.log.info("Getting {} Report".format(self.report))
        """
        df = wr.s3.read_parquet('{}symbol_list.parquet.gzip'.format(bucket))
        """

        df = pd.read_parquet('/home/justairflow/airflow/stimbol_list.parquet.gzip')

        for i in df['symbol'].values.tolist():
            if self.report == 'balance_sheet':
                try:
                    balance_sheet = cv.balance_sheet(str(i), period = str(self.period), ftype = str(self.ftype))
                    self.log.info("Balance Sheet data retrieved from API")
                    balance_sheet.to_parquet('{}balance_sheet/{}/{}.parquet.gzip'.format(bucket,self.period ,i))
                    self.log.info("Balance Sheet saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'cash_flow':
                try:
                    cash_flow_statement = cv.cash_flow_statement(str(i), period = str(self.period), ftype = str(self.ftype))
                    self.log.info("Cash flow data retrieved from API")
                    cash_flow_statement.to_parquet('{}cash_flow/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Cash Flow saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'income_statement':
                try:
                    income_statement = cv.income_statement(str(i), period = str(self.period), ftype = str(self.ftype))
                    self.log.info("Income Statement data retrieved from API")
                    income_statement.to_parquet('{}income_statement/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Income Statement saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'financial_ratios':
                try:
                    financial_ratios = cv.financial_ratios(str(i), period = str(self.period), ttm = self.ttm)
                    self.log.info("Financial Ratios data retrieved from API")
                    financial_ratios.to_parquet('{}financial_ratios/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Financial Ratios saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'key_metrics':
                try:
                    key_metrics = cv.key_metrics(str(i), period = str(self.period))
                    self.log.info("Key Metrics data retrieved from API")
                    key_metrics.to_parquet('{}key_metrics/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Key Metrics saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'enterprise_value':
                try:
                    enterprise_value = cv.enterprise_value(str(i), period = str(self.period))
                    self.log.info("Enterprise Value data retrieved from API")
                    enterprise_value.to_parquet('{}key_metrics/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Enterprise Value saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'company_profile':
                try:
                    company_profile = sts.company_profile(str(i))
                    self.log.info("Company Profile data retrieved from API")
                    company_profile.to_parquet('{}company_profile/{}.parquet.gzip'.format(bucket, i))
                    self.log.info("Company Profile saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)
