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
    append_sql = "INSERT INTO {} {}"

    @apply_defaults
    def __init__(self,
                 conn_id="redshift",
                 sql="",
                 params=None,
                 *args,
                 **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.sql = sql
        self.truncate = params.get('truncate', False)
        self.table = params.get('table', None)

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if self.truncate:
            self.log.info(f"Clearing data from table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Loading data into destination Redshift table {self.table}")
        redshift.run(LoadDimensionOperator.append_sql.format(self.table, self.sql))
