from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from dq_rule_execution_engine.src.common.utils.tables_info_object_handler import TableInfoObjectHandler
from dq_rule_execution_engine.src.reports.report_generator import ReportGenerator


class DqAggregatedReportingFailedJob(ReportGenerator):

    def __init__(
            self,
            spark: SparkSession,
            config: dict
    ):
        super().__init__(spark, job_name='dq_aggregated_reporting_failed')
        #Aggregated report table details object
        self.aggregated_reporting = TableInfoObjectHandler.aggregated_report_obj
        # Main architecture table details object
        self.rule_exception = TableInfoObjectHandler.rule_exceptions_obj

        #Output aggregated report table details object
        self.aggregated_reporting_failed = TableInfoObjectHandler.aggregated_report_failed_info_obj

        self.stats_client = self.get_db_client(section="stats", config=config)
        self.report_client = self.get_db_client(section="report", config=config)

    def run(self):
        aggregated_reporting_table_name = self.aggregated_reporting.get_qualified_table_name()
        rule_exception_table_name = self.rule_exception.get_qualified_table_name()

        aggregated_reporting_table_checkpoint_col = self.aggregated_reporting.RECORD_CREATED_AT_COL_NAME
        rule_exception_table_checkpoint_col = self.rule_exception.CREATED_TIME_COL_NAME

        rule_exception_table, new_ret_checkpoint_row = \
            self.get_incremental_table(client=self.stats_client,
                                       table_name=rule_exception_table_name,
                                       checkpoint_col_name=rule_exception_table_checkpoint_col)

        aggregated_reporting_table, new_art_checkpoint_row = \
            self.get_incremental_table(client=self.report_client,
                                       table_name=aggregated_reporting_table_name,
                                       checkpoint_col_name=aggregated_reporting_table_checkpoint_col)

        ret_count = rule_exception_table.count()
        art_count = aggregated_reporting_table.count()
        new_ret_checkpoint_row = new_ret_checkpoint_row + (ret_count,)
        new_art_checkpoint_row = new_art_checkpoint_row + (art_count,)
        if rule_exception_table.isEmpty() or aggregated_reporting_table.isEmpty():
            pass

        else:
            result = aggregated_reporting_table \
                .alias('report') \
                .join(rule_exception_table.alias('exc'),
                      (col(f'report.{self.aggregated_reporting.RULE_ID_COL_NAME}') == col(
                          f'exc.{self.rule_exception.RULE_ID_COL_NAME}'))
                      &
                      (col(f'report.{self.aggregated_reporting.JOB_RUN_ID_COL_NAME}') == col(
                          f'exc.{self.rule_exception.JOB_RUN_ID_COL_NAME}')))\
                .select(f'report.{self.aggregated_reporting_failed.JOB_RUN_ID_COL_NAME}',
                        f'report.{self.aggregated_reporting_failed.RULE_ID_COL_NAME}',
                        f'report.{self.aggregated_reporting_failed.RULE_NAME_COL_NAME}',
                        f'report.{self.aggregated_reporting_failed.RULE_DESC_COL_NAME}',
                        f'report.{self.aggregated_reporting_failed.ENTITY_PHYSICAL_NAME_COL_NAME}',
                        f'report.{self.aggregated_reporting_failed.ENTITY_PRIMARY_KEY_COL_NAME}',
                        f'exc.{self.aggregated_reporting_failed.DATA_OBJECT_KEY_COL_NAME}',
                        f'exc.{self.aggregated_reporting_failed.EXCEPTION_SUMMARY_COL_NAME}',
                        f'exc.{self.aggregated_reporting_failed.CREATED_TIME_COL_NAME}')\
                .withColumn(self.aggregated_reporting.RECORD_CREATED_AT_COL_NAME, lit(self.run_time))

            self.report_client.save_data(result, self.aggregated_reporting_failed.get_qualified_table_name(), 'WRITE_APPEND')

        self.add_checkpoint_in_db(self.report_client, Row(new_ret_checkpoint_row))
        self.add_checkpoint_in_db(self.report_client, Row(new_art_checkpoint_row))
        print("Reporting Job is completed successfully")
