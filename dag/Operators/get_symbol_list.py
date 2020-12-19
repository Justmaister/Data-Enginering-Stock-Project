from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import os
from pyfmpcloud import settings
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

"""
import pandas as pd
import findspark
findspark.init('/home/just/spark-3.0.0-bin-hadoop2.7')
from pyspark.sql import SparkSession
"""

class GetSymbolList(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 call = None,
                 *args, **kwargs):
        super(GetSymbolList, self).__init__(*args, **kwargs)
        self.call = call

    def execute(self, context):
        self.log.info("Getting symbol_list")

        """
        spark = SparkSession.builder.appName('symbol_list').getOrCreate()
        self.log.info("Spark session Created")
        """


        symbol_list = sts.symbol_list()

        symbol_list = symbol_list.head(5)

        self.log.info("Symbol List data retrieved from API")

        symbol_list.to_parquet('{}symbol_list.parquet.gzip'.format(bucket))
        #symbol_list.to_csv('{}symbol_list.csv'.format(bucket))


        """
        dfo = symbol_list.select_dtypes('object').astype(str)
        dfn = symbol_list.select_dtypes(exclude='object').astype(str)
        df = dfo.merge(dfn, left_index=True, right_index=True ,how='inner')
        sdf = spark.createDataFrame(df)

        sdf.write.mode("overwrite").parquet(bucket + 'symbol_list')
        self.log.info("File saved to {} bucket".format(bucket))

        spark.stop()
        self.log.info("Spark session Closed")
        """
