import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift.
    The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's
    parameters should specify where in S3 the file is loaded and what is the target table.

    The parameters should be used to distinguish between JSON file. Another important requirement of the
    stage operator is containing a templated field that allows it to load timestamped files from S3 based on the
    execution time and run backfills.

    /****************************************************************************
    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
    ****************************************************************************/
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            IAM_ROLE '{}'
            FORMAT AS JSON '{}'
        """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 iam_role="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 *args,
                 **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.iam_role = iam_role
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.iam_role,
            self.json_format
        )
        redshift.run(formatted_sql)
