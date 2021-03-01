from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 tables = [], # array of tables to check for rows
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Data Quality check: Started')
        
        try :
            redshift = PostgresHook( self.redshift_conn_id )            
        except :
            self.log.error( 'Error establishing Data Quality Redshift connection' )
        
        row_count_stmt = "SELECT count(1) FROM {}"
        
        for table in self.tables:        
            table_count = redshift.get_records( row_count_stmt.format(table) )
            
            if len(table_count) < 1 or len(table_count[0]) < 1:
                raise ValueError( 'Validation failed for Table {}: No records found'.format(table))
            num_records = table_count[0][0]
            if num_records < 1:
                raise ValueError( 'Validation failed for Table {}: 0 rows'.format(table))
            else :
                self.log.info( 'Validation passed for table {}: {} records found'.format(table, num_records) )
                
                
        self.log.info('Data Quality check: Complete')