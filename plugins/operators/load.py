from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadOperator(BaseOperator):

    ui_color = '#F98866'
    
    run_query = """
    INSERT INTO public.{table} 
    {query}
    """
                 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ="",
                 query = "",
                 table = "",
                 *args, **kwargs):

        super(LoadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        self.log.info("Getting credentials for Redshift: {}".format(self.redshift_conn_id))
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info("Deleting content of {}".format(self.table))
        redshift_hook.run("DELETE FROM {table}".format(table = self.table))
        
        self.log.info("Running INSERT INTO {} query".format(self.table))
        redshift_hook.run(self.run_query.format(table= self.table,query= self.query))
        self.log.info("INSERT INTO FINISHED")
