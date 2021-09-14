from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class CopyToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    run_query = " COPY {} \
                  FROM '{}' \
                  ACCESS_KEY_ID '{}' \
                  SECRET_ACCESS_KEY '{}' \
                  FORMAT AS {};"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id = "",
                 table="",
                 s3_bucket="",
                 file_format = "",
                 *args, **kwargs):

        super(CopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.file_format = file_format

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info("Getting credentials for AWS: {}".format(aws_hook.get_credentials()))
        aws_credentials = aws_hook.get_credentials()
        self.log.info("Getting credentials for Redshift: {}".format(self.redshift_conn_id))
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info("Deleting content of {}".format(self.table))
        redshift_hook.run("DELETE FROM {table}".format(table = self.table))
        self.log.info("Running COPY {} query from {} bucket".format(self.table, self.s3_bucket))
        redshift_hook.run(self.run_query.format(self.table,self.s3_bucket,aws_credentials.access_key,aws_credentials.secret_key, self.file_format ))
        self.log.info("COPY FINISHED")





