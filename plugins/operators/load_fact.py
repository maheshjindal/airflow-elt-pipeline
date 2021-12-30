from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    insert_query = "INSERT INTO {table_name} {query}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):
        """
        Initialization constructor with all the required parameters for the Operator

        :param redshift_conn_id: The redshift connection id for airflow
        :param table: The fact table name which is being used to insert data.
        :param sql_query: The select query for inserting data
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        """
        This method first creates a redshift connection with database and executes the received query

        :param context: The Operator default context
        :return: None
        """
        self.log.info(f'Inserting data for table {self.table}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        populated_query = self.insert_query.format(self.table,self.sql_query)
        redshift_hook.run(populated_query)
        self.log.info(f'Data Inserted to {self.table} successfully...')

