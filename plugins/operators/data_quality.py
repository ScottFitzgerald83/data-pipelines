import logging
import textwrap

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.test_helpers import TestHelpers


class DataQualityOperator(BaseOperator):
    """The operator's main functionality is to receive one or more SQL based test cases along with the expected results
    and execute the tests. For each the test, the test result and expected result needs to be checked and if there is
    no match, the operator should raise an exception and the task should retry and fail eventually.

    For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the
    rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would
    compare the SQL statement's outcome to the expected result."""
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 *args,
                 **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.any_tests_failed = False
        self.row_counts_failed = False
        self.null_counts_failed = False
        self.failed_tests = []
        self.row_counts_summary = []
        self.null_checks_summary = []

    def execute(self, context):
        tests_to_run = context["params"]["tests_to_run"]
        self.log.info(f"Received the following tests from the caller: {tests_to_run}")
        if 'test_row_counts' in tests_to_run:
            self.test_row_counts(tests_to_run['test_row_counts'])
        if 'test_null_values' in tests_to_run:
            for table, columns in tests_to_run['test_null_values'].items():
                for column in columns:
                    self.test_null_values(table, column)

        self.display_quality_check_results()
        if self.any_tests_failed:
            self.display_failed_results()
            raise ValueError(f"Task will make 5 attempts before failing. All data quality checks must pass.")

    def display_quality_check_results(self):
        """Display results of the data quality checks in a slightly prettier format"""
        newline = '\n'
        message = f"""
        {TestHelpers.quality_checks_box}
        {newline.join(self.row_counts_summary)}
        {newline.join(self.null_checks_summary)}
        {TestHelpers.end_block}
        """
        self.log.info(textwrap.dedent(message))

    def display_failed_results(self):
        """
        Display the failed results (if any) in a slightly prettier format
        :return:
        """
        newline = '\n'
        message = f"""
        {TestHelpers.failed_summary_box}
        {newline.join(self.failed_tests)}
        {TestHelpers.end_block}
        """
        self.log.error(textwrap.dedent(message))

    def test_row_counts(self, tables):
        """
        Tests whether a table contains rows after ETL and returns the number of rows.
        :param tables: a list of tables against which to run the check
        :return: If no rows exist returns an error; otherwise, count of rows
        """
        self.row_counts_failed = False
        redshift_hook = PostgresHook("redshift")
        for table in tables:
            self.log.info(f"Running test_row_counts on {table}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if not records:
                self.row_counts_failed = True
                self.any_tests_failed = True
                self.failed_tests.append(f"Row counts failed on {table}")
                message = f"Data quality check failed. {table} returned no results"
            else:
                message = f"Data quality on table {table} check passed with {records[0][0]} records"
            row_count = records[0][0] if records else 0

            self.row_counts_summary.append(f"SUMMARY FOR {table}:")
            self.row_counts_summary.append(f"COUNT OF ROWS: {row_count}")
            self.failed_tests.append(message) if self.row_counts_failed else self.row_counts_summary.append(message)

    def test_null_values(self, table, column):
        """
        Determines how many of a column's rows are null
        :param table: the table against which to run the check
        :param column: the column against which to run the check
        :return: If no rows exist returns an error; otherwise, count of rows
        """
        self.null_counts_failed = False
        redshift_hook = PostgresHook("redshift")
        null_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} where {column} is null")
        all_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

        self.log.info(f"Running test_null_values for {column} column in {table}")
        null_count = null_records[0][0]
        row_count = all_records[0][0]
        pct_null = (null_count / row_count * 100)
        pct_passing = 100 - pct_null

        if pct_null <= 50:
            message = f"Data quality on table {table} check passed with {pct_passing:.2f}% of the records populated by non-null values"
        else:
            message = f"Data quality check failed. More than 50% of the rows in {table} are null for the {column} column"
            self.null_counts_failed = True
            self.any_tests_failed = True
            self.failed_tests.append(f"Null checks failed on {table} for {column}")

        self.null_checks_summary.append(f"SUMMARY FOR {table} column {column}:")
        self.null_checks_summary.append(f"COUNT OF NULL ROWS: {null_count}")
        self.null_checks_summary.append(f"COUNT OF ALL ROWS: {row_count}")
        self.null_checks_summary.append(f"PERCENT NULL: {pct_null:.2f}%")
        self.failed_tests.append(message) if self.null_counts_failed else self.null_checks_summary.append(message)
