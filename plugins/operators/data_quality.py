import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        table = context["table"]
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if not records:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]

        if not num_records:
            raise ValueError(f"No results returns from {table}")
        logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
