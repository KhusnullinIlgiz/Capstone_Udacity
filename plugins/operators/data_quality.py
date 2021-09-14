from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import re

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    check_sql = "SELECT COUNT(*) FROM {table}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_qry = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_qry = tables_qry

    def execute(self, context):
        """
        Executing sql statement in redshift for each table to check data quality
        """
        self.log.info("Getting credentials for Redshift: {}".format(self.redshift_conn_id))
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        
        for qry in self.tables_qry:
         
            table = qry.get("table")
            qry_result = re.sub('[^a-zA-Z0-9]+', '', str(redshift_hook.get_records(self.check_sql.format(table = qry.get("table")))[0]))
            expected_result = qry.get("expected_result")
            self.log.info("Begin data check for {} table".format(table))
         
            if qry_result != expected_result:
                raise ValueError("Table: {} qry_result: {}; expected_result: {}".format(table, qry_result, expected_result))
            else:
                self.log.info("Data Quality Check is successful: Table: {} qry_result: {}  = to expected_result: {}".format(table, qry_result,expected_result ))
                                     
                                     