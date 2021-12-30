from abc import ABC

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator, ABC):
    ui_color = '#358140'
    delete_query = "DELETE FROM {table_name}"

    # The copy query which is being used to copy data from s3 to staging table
    copy_redshift_query = """
            COPY {table_name} FROM '{s3_path}'
            ACCESS_KEY_ID '{access_key_id}'
            SECRET_ACCESS_KEY '{secret_key_id}'
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON '{json_path}'
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):
        """
        Initialization constructor with all the required parameters for the Operator

        :param redshift_conn_id: The redshift connection id for airflow
        :param aws_credentials_id: The aws credentials id for airflow
        :param table: The table name which is being used as staging table.
        :param s3_bucket: The s3 bucket name
        :param s3_key: The s3 key from which the data needs to be loaded
        :param json_path (optional) : The json path for loading data from s3 to staging table.
                                      The default value will be taken as 'auto'
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path


def execute(self, context):
    """
    This is the main method for operator execution.
    This method first creates AWS Client connection using AWS hook, Then it creates the redshift connection
    This method is responsible for deleting data from existing table and loads data to the staging table from s3

    :param self: The class reference
    :param context: The operator context
    :return: None
    """
    aws_hook = AwsHook(self.aws_credentials_id)
    credentials = aws_hook.get_credentials()
    redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

    self.log.info(f"Deleting redshift table data for {self.table}")
    redshift.run(self.delete_query.format(table_name=self.table))

    self.log.info("Copying data from S3 to Redshift")
    rendered_key = self.s3_key.format(**context)
    s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
    populated_query = self.copy_redshift_query.format(
        table_name=self.table,
        s3_path=s3_path,
        access_key_id=credentials.access_key,
        secret_key_id=credentials.secret_key,
        json_path=self.json_path)
    redshift.run(populated_query)
