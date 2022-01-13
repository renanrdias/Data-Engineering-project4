from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Data quality check. Compares the query's result and expected result.
    :param redshift_conn_id:Redshift connection ID.
    :param sql_test: Query to be executed in order to test the data quality.
    :param expect_result: The expected result from the sql_test.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,                                  
                 redshift_conn_id="",
                 sql_test="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.sql_test=sql_test
        self.expected_result=expected_result
        

    def execute(self, context):
        self.log.info('Connecting to Redshift.')        
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Start running quality check.")
        
        results = redshift.get_records(self.sql_test)
        
        if results[0][0] != self.expected_result:
            raise ValueError(f"""The result of the check diverge from the expected result: 
                                {self.expected_result} is not equal to {results[0][0]}""")
        
        else:
            self.log.info("The quality check passed successfully.")
            
            
        
        
        
        
        
        