from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    count_query = "SELECT COUNT(*) FROM {table_name}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):
        """
        :param redshift_conn_id: The redshift connection which is being used to connect with database.
        :param tables: The list of tables for which data quality needs to be checked
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.tables = tables

    def execute(self, context):
        """
        This is the main method for operator execution.
        It checks the data quality for the list of tables by executing the row count query for every table

        :param context: The default Operator context
        :return: None
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            record_data = redshift_hook.get_records(self.count_query.format(table_name=table))
            if len(record_data) < 1 or len(record_data[0]) < 1 or record_data[0][0] < 1:
                error_msg = "Data quality check status ::: {}." \
                            "Records not present for table {}".format("FAILED", table)
                self.log.error(error_msg)
                raise ValueError(error_msg)
            self.log.info("Data quality check status ::: {}." \
                          "Record count for table {} is {}".format("FAILED", table, record_data[0][0]))
