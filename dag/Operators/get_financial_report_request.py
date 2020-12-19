from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import os
import requests
import awswrangler as wr
import pandas as pd
import numpy as np

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key
os.environ['AWS_ACCESS_KEY_ID']=access_key
os.environ['AWS_SECRET_ACCESS_KEY']=secret_key

API_KEY = Variable.get('API_KEY')
bucket = Variable.get('s3_bucket')

class GetFinancialReportRequest(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 report = "",
                 period = None,
                 ftype = None,
                 ttm = None,
                 *args, **kwargs):

        super(GetFinancialReportRequest, self).__init__(*args, **kwargs)
        self.report = report
        self.period = period
        self.ftype = ftype
        self.ttm = ttm

    def execute(self, context):
        self.log.info("Getting {} Report".format(self.report))

        #df = wr.s3.read_parquet('{}symbol_list.parquet.gzip'.format(bucket))

        df = pd.read_parquet('/home/justairflow/airflow/symbol_list.parquet.gzip')

        if self.report == 'symbol_list':
            try:
                response = requests.get(f"https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}")
                symbol_list = pd.DataFrame.from_dict(response.json())
                self.log.info("Company Profile data retrieved from API")
                symbol_list.to_csv('{}{}/symbol_list.csv'.format(bucket, self.report))
                self.log.info("Symbol List saved")
            except Exception as e:
                self.log.info(e)

        for i in df['symbol'].values.tolist():
            if self.report == 'balance_sheet':
                try:
                    response = requests.get(f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{str(i)}?apikey={API_KEY}&period={self.period}")
                    balance_sheet = pd.DataFrame.from_dict(response.json())
                    cols = balance_sheet.select_dtypes(np.number).columns
                    balance_sheet[cols] = balance_sheet[cols].astype(int)
                    self.log.info("Balance Sheet data retrieved from API")
                    balance_sheet.to_csv('{}balance_sheet/{}/{}.csv'.format(bucket,self.period ,i), index=False, header=False)
                    #balance_sheet.to_parquet('{}balance_sheet/{}/{}.parquet.gzip'.format(bucket,self.period ,i))
                    self.log.info("Balance Sheet saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'cash_flow':
                try:
                    response = requests.get(f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{str(i)}?apikey={API_KEY}&period={self.period}")
                    cash_flow = pd.DataFrame.from_dict(response.json())
                    cols = cash_flow.select_dtypes(np.number).columns
                    cash_flow[cols] = cash_flow[cols].astype(int)
                    self.log.info("Cash flow data retrieved from API")
                    cash_flow.to_csv('{}cash_flow/{}/{}.csv'.format(bucket,self.period, i), index=False, header=False)
                    #cash_flow_statement.to_parquet('{}cash_flow/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Cash Flow saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'income_statement':
                try:
                    response = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{str(i)}?apikey={API_KEY}&period={self.period}")
                    income_statement = pd.DataFrame.from_dict(response.json())
                    column_types = {
                         'revenue': int,
                         'costOfRevenue': int,
                         'grossProfit': int,
                         'grossProfitRatio': float,
                         'researchAndDevelopmentExpenses': int,
                         'generalAndAdministrativeExpenses': int,
                         'sellingAndMarketingExpenses': int,
                         'otherExpenses': int,
                         'operatingExpenses': int,
                         'costAndExpenses': int,
                         'interestExpense': int,
                         'depreciationAndAmortization': int,
                         'ebitda': int,
                         'ebitdaratio': float,
                         'operatingIncome': int,
                         'operatingIncomeRatio': float,
                         'totalOtherIncomeExpensesNet': int,
                         'incomeBeforeTax': int,
                         'incomeBeforeTaxRatio': float,
                         'incomeTaxExpense': int,
                         'netIncome': int,
                         'netIncomeRatio':float,
                         'eps': int,
                         'epsdiluted': int,
                         'weightedAverageShsOut': int,
                         'weightedAverageShsOutDil': int
                        }
                    income_statement = income_statement.astype(column_types)

                    #income_statement = cv.income_statement(str(i), period = str(self.period), ftype = str(self.ftype))

                    self.log.info("Income Statement data retrieved from API")
                    income_statement.to_csv('{}income_statement/{}/{}.csv'.format(bucket,self.period, i), index=False, header=False)
                    #income_statement.to_parquet('{}income_statement/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Income Statement saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'financial_ratios':
                try:
                    response = requests.get(f"https://financialmodelingprep.com/api/v3/ratios/{str(i)}?apikey={API_KEY}&period={self.period}")
                    financial_ratios = pd.DataFrame.from_dict(response.json())

                    #financial_ratios = cv.financial_ratios(str(i), period = str(self.period), ttm = self.ttm)

                    self.log.info("Financial Ratios data retrieved from API")
                    financial_ratios.to_csv('{}financial_ratios/{}/{}.csv'.format(bucket,self.period, i), index=False, header=False)
                    #financial_ratios.to_parquet('{}financial_ratios/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Financial Ratios saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'key_metrics':
                try:
                    response = requests.get(f"https://financialmodelingprep.com/api/v3/key-metrics/{str(i)}?apikey={API_KEY}&period={self.period}")
                    key_metrics = pd.DataFrame.from_dict(response.json())

                    #key_metrics = cv.key_metrics(str(i), period = str(self.period))

                    self.log.info("Key Metrics data retrieved from API")
                    key_metrics.to_csv('{}key_metrics/{}/{}.csv'.format(bucket,self.period, i), index=False, header=False)
                    #key_metrics.to_parquet('{}key_metrics/{}/{}.parquet.gzip'.format(bucket,self.period, i))
                    self.log.info("Key Metrics saved from {} company".format(i))
                except Exception as e:
                    self.log.info(e)

            if self.report == 'enterprise_value':
                try:
                    response = requests.get(f"https://financialmodelingprep.com/api/v3/enterprise-values/{str(i)}?apikey={API_KEY}&period={self.period}")
                    enterprise_value = pd.DataFrame.from_dict(response.json())
                    column_types = {
                         'stockPrice': float,
                         'numberOfShares':int,
                         'marketCapitalization': int,
                         'minusCashAndCashEquivalents': int,
                         'addTotalDebt': int,
                         'enterpriseValue': int
                        }
                    enterprise_value = enterprise_value.astype(column_types)

                    #enterprise_value = cv.enterprise_value(str(i), period = str(self.period))

                    self.log.info("Enterprise Value data retrieved from API")
                    enterprise_value.to_csv('{}enterprise_values/{}/{}.csv'.format(bucket,self.period, i), index=False, header=False)
                    #enterprise_value.to_parquet('{}enterprise-values/{}/{}.parquet.gzip'.format(bucket,self.period, i))
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
