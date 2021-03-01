from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    # sql queries helper object
    sql_queries = SqlQueries()
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table = "", 
                 full_sync = False, # True delete entries in table or False for append 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)       
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.full_sync = full_sync

    def execute(self, context):
        self.log.info('Dimension {} load: Started'.format(self.table))
        
        try :
            redshift = PostgresHook( self.redshift_conn_id )
        except:
            self.log.error( 'Error establishing Redshift connection on LoadFactOperator' )
        
        
        self.log.info( "Assuring dimension table is created : {}".format(self.table) )  
        create_table_stmt = getattr( LoadDimensionOperator.sql_queries, "{}_table_create".format(self.table) )
        redshift.run( create_table_stmt )
        
        
        if self.full_sync :
            self.log.info( "Truncating table for full-sync : {}".format(self.table) )  
            delete_table_stmt = "TRUNCATE TABLE {}".format( self.table )
            redshift.run( delete_table_stmt )
        
        
        self.log.info( "Inserting dimension table data : {}".format(self.table) )  
        insert_select_stmt = getattr( LoadDimensionOperator.sql_queries, "{}_table_insert".format(self.table) )
        insert_stmt = "INSERT INTO {} ".format(self.table) + insert_select_stmt
        redshift.run( insert_stmt )
        
        
        self.log.info('Dimension {} load: Complete'.format(self.table))