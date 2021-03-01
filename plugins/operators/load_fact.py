from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    # sql queries helper object
    sql_queries = SqlQueries()
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table = "songplays",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
              
    def execute(self, context):
        self.log.info('LoadFactOperator table {} : Started'.format(self.table))
        
        try :
            redshift = PostgresHook( self.redshift_conn_id )
        except:
            self.log.error( 'Error establishing Redshift connection on LoadFactOperator' )
    
        self.log.info( "Assuring fact table is created : {}".format(self.table) )  
        create_table_stmt = getattr( LoadFactOperator.sql_queries, "{}_table_create".format(self.table) )
        redshift.run( create_table_stmt )
        
        self.log.info( "Inserting fact table data : {}".format(self.table) )  
        insert_select_stmt = getattr( LoadFactOperator.sql_queries, "{}_table_insert".format(self.table) )
        
        insert_stmt = "INSERT INTO {} ".format(self.table) + insert_select_stmt
        
        redshift.run( insert_stmt )
        
        self.log.info('LoadFactOperator table {} : Complete'.format(self.table))
        