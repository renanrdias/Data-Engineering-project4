from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads Dimension table in Amazon Redshift.
    
    :param redshift_conn_id: Postgres connection ID.
    :param table: Target dimension table.
    :param sql: Query to be executed in order to load the target table.
    :table_append: Append-insert or Truncate-insert pattern.
    :table_pk: Target table's primary key.
    
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 table_append=False,
                 table_pk="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql
        self.table_append=table_append
        self.table_pk=table_pk

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.table_append:
            formatted_sql = f"""
                CREATE TEMP TABLE staging_{self.table} LIKE ({self.table});
                
                INSERT INTO staging_{self.table} {self.sql};
                
                DELETE FROM {self.table}
                USING staging_{self.table}
                WHERE {self.table}.{self.table_pk} = staging_{self.table}.{self.table_pk};
                
                INSERT INTO {self.table} SELECT * FROM staging_{self_table};
            """
        
        else:
            formatted_sql = f"""
                TRUNCATE table {self.table};
                INSERT INTO {self.table} {self.sql};
            """
        
        redshift.run(formatted_sql)
        
        
        
        
        
        
        
        
        
        
