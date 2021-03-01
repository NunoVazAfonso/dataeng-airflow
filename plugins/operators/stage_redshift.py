from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_s3_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON 'auto'
    """
    # sql queries helper object
    sql_queries = SqlQueries()
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        
    def execute(self, context):
        self.log.info('ETL Staging: Started')
        
        try :
            # Setup AWS credentials (for S3)
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()

            # Setup Redshift credentials
            redshift = PostgresHook( self.redshift_conn_id )
            
        except :
            self.log.error( 'Error establishing Staging AWS connections' )
     
    
        self.log.info("Creating staging table {}".format(self.table) )
        create_table_stmt = getattr( StageToRedshiftOperator.sql_queries, "{}_table_create".format(self.table) )
        redshift.run( create_table_stmt )
    
        self.log.info("Clearing data from staging table {}".format(self.table) )
        delete_stmt = "DELETE FROM {}".format(self.table)
        redshift.run( delete_stmt )
   
        self.log.info("Populating staging table {}".format(self.table) )
        #rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_s3_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        redshift.run(formatted_sql)
    
        self.log.info('ETL Staging: Complete');    
        
