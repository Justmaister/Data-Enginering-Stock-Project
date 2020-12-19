from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import os
import requests
import pandas as pd

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key
os.environ['AWS_ACCESS_KEY_ID']=access_key
os.environ['AWS_SECRET_ACCESS_KEY']=secret_key

API_KEY = Variable.get('API_KEY')
bucket = Variable.get('s3_bucket')

class GetSymbolListRequest(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 call = None,
                 *args, **kwargs):
        super(GetSymbolListRequest, self).__init__(*args, **kwargs)
        self.call = call

    def execute(self, context):
        self.log.info("Getting symbol_list")
        response = requests.get(f"https://financialmodelingprep.com/api/v3/company/stock/list?apikey={API_KEY}")
        symbol_list = pd.DataFrame.from_dict(response.json()['symbolsList'])

        symbol_list = symbol_list.head(5)

        self.log.info("Symbol List data retrieved from API")
        symbol_list.to_csv('{}symbol_list.csv'.format(bucket), index=False)
