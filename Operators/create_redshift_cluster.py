from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import os
import boto3
import time

DWH_DB = Variable.get('DBName')
DWH_CLUSTER_IDENTIFIER = Variable.get('ClusterIdentifier')
DWH_DB_USER = Variable.get('MasterUsername')
DWH_DB_PASSWORD = Variable.get('MasterUserPassword')

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key
os.environ['AWS_ACCESS_KEY_ID']=access_key
os.environ['AWS_SECRET_ACCESS_KEY']=secret_key


class CreateRedshiftCluster(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 clustertype = "",
                 nodetype = "",
                 numberofnodes = "",
                 *args, **kwargs):

        super(CreateRedshiftCluster, self).__init__(*args, **kwargs)
        self.clustertype = clustertype
        self.nodetype = nodetype
        self.numberofnodes = numberofnodes

    def execute(self, context):
        iam = boto3.client('iam',
                        region_name="us-west-2",
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key
                        )
        roleArn = iam.get_role(RoleName='dwhRole')['Role']['Arn']
        self.log.info("RoleArn collected from AWS")

        redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=access_key,
                           aws_secret_access_key=secret_key
                           )


        if redshift.describe_clusters()['Clusters'] == []:
            response = redshift.create_cluster(
                ClusterType = self.clustertype,
                NodeType = self.nodetype,
                NumberOfNodes=int(self.numberofnodes),

                DBName = DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,

                IamRoles=[roleArn]
                )
            self.log.info("Creating Redshift Cluster")
            time.sleep(10)
        while redshift.describe_clusters()['Clusters'][0]['ClusterAvailabilityStatus'] == 'Modifying':
            time.sleep(5)
        self.log.info("Redshift Cluster Created")
