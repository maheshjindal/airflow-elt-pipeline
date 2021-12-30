from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    insert_query = "INSERT INTO {table_name} {query}"
    truncate_query = "TRUNCATE TABLE {table_name}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 truncate=False,
                 *args, **kwargs):
        """
        Initialization constructor with all the required parameters for the Operator

        :param redshift_conn_id: The redshift connection id for airflow
        :param table: The fact table name which is being used to insert data.
        :param sql_query: The select query for inserting data
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        """
        This method first creates a redshift connection with database and executes the received query.
        {@Note}: If truncate is set to true, before inserting the data it truncates the existing table.

        :param context: The Operator default context
        :return: None
        """
        self.log.info(f'Executing LoadDimensionOperator for table {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            redshift.run(self.truncate_query.format(table_name=self.table))
        redshift.run(self.insert_query.format(table_name=self.table,
                                              query=self.sql_query))
        self.log.info(f'Data Inserted to {self.table} successfully...')
