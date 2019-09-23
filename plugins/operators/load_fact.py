from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations.
    Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement
    and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

    Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load.
    Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables
    are usually so massive that they should only allow append type functionality.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql="",
                 *args,
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context):
        table = context['table']
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Clearing data from table {table}")
        redshift.run(f"DELETE FROM {table}")

        self.log.info("Loading data into destination Redshift table")
        redshift.run(self.sql)
