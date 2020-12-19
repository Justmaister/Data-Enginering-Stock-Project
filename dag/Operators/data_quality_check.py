from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.models import Variable
import psycopg2

host = Variable.get('Host')
dbname = Variable.get('DBName')
user = Variable.get('MasterUsername')
password = Variable.get('MasterUserPassword')

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables= {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id,
        self.tables = tables


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        conn = psycopg2.connect("host={} dbname={} user={} password={} port=5439".format(host, dbname, user, password))
        cur = conn.cursor()
        print("Conected to Redshift Cluster")

        tables = self.tables.keys()
        for table in tables:
            self.log.info(f'Running data quality on {table} table')
            sql_query = """SELECT COUNT(*) FROM {}""".format(table)
            cur.execute(sql_query)
            records = cur.fetchone()[0]
            if records < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            self.log.info(f"Data quality on table {table} check passed with {records} records")

"""
            #redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            self.log.info(f'Running data quality on {table} table 2')
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f'Running data quality on {table} table 3')
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                self.log.info(f'Running data quality on {table} table 4')
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
"""
