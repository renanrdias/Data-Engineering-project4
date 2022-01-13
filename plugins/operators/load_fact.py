from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads a fact table in Amazon Redshift.
    
    :param postgres_conn_id: Postgres connection ID.
    :param sql: Query to be executed in order to load the target table.
    :table: Target fact table.
    """

    ui_color = '#F98866'    
    

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.postgres_conn_id=postgres_conn_id
        self.sql=sql
        self.table=table

    def execute(self, context):
        self.log.info('Connecting to Redshift.')
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info('Running query.')
        formatted_sql = f"INSERT INTO {self.table} {self.sql}"
        
        redshift.run(formatted_sql)
