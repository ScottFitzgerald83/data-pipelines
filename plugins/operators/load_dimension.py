import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations.
    Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement
    and target database on which to run the query against. You can also define a target table that will contain the
    results of the transformation.

    Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load.
    Thus, you could also have a parameter that allows switching between insert modes when loading dimensions.
    """
    ui_color = '#80BD9E'
    insert_sql = """
    INSERT INTO {}
    {}
        """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql="",
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context):
        try:
            table = context['params']['table']
        except KeyError:
            logging.error(msg="Table not found in context", exc_info=True)
            logging.info(f'context: {context}')

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Clearing data from table {table}")
        redshift.run(f"DELETE FROM {table}")

        self.log.info(f"Loading data into destination Redshift table {table}")
        redshift.run(LoadDimensionOperator.insert_sql.format(table, self.sql))
