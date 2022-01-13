from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Copies data from json files in S3 bucket into a staging table in Redshift.
    :param redshift_conn_id: Redshift connection ID.
    :param aws_credentials_id: aws credentials ID.
    :table: Target table the data will be loaded into.
    :s3_bucket: S3 bucket where json files reside.
    :s3_key: key for S3 bucket.
    
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto ignorecase'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key        
        

    def execute(self, context):
        self.log.info('Loading data into redshift')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # clear data if already exists
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Started copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table,
                                                               s3_path,
                                                               credentials.access_key,
                                                               credentials.secret_key)
        redshift.run(formatted_sql)
        
        
        
        





