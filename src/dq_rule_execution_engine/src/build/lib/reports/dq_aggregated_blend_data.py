from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from dq_rule_execution_engine.src.common.utils.tables_info_object_handler import TableInfoObjectHandler
from dq_rule_execution_engine.src.reports.report_generator import ReportGenerator


class DqAggregatedBlendData(ReportGenerator):

    def __init__(
            self,
            spark: SparkSession,
            config: dict
    ):
        super().__init__(spark, job_name='dq_aggregated_blend_data')
        #Aggregated report table details object
        self.aggregated_reporting = TableInfoObjectHandler.aggregated_report_obj
        self.aggregated_reporting_failed = TableInfoObjectHandler.aggregated_report_failed_info_obj

        #Output aggregated report table details object
        self.aggregated_blend_data = TableInfoObjectHandler.aggregated_blend_data_obj

        self.report_client = self.get_db_client(section="report", config=config)

    def run(self):
        aggregated_reporting_table_name = self.aggregated_reporting.get_qualified_table_name()
        aggregated_reporting_failed_table_name = self.aggregated_reporting_failed.get_qualified_table_name()

        aggregated_reporting_table_checkpoint_col = self.aggregated_reporting.RECORD_CREATED_AT_COL_NAME
        aggregated_reporting_failed_table_checkpoint_col = self.aggregated_reporting_failed.RECORD_CREATED_AT_COL_NAME

        aggregated_reporting_failed_table, new_arft_checkpoint_row = \
            self.get_incremental_table(client=self.report_client,
                                       table_name=aggregated_reporting_failed_table_name,
                                       checkpoint_col_name=aggregated_reporting_failed_table_checkpoint_col)

        aggregated_reporting_table, new_art_checkpoint_row = \
            self.get_incremental_table(client=self.report_client,
                                       table_name=aggregated_reporting_table_name,
                                       checkpoint_col_name=aggregated_reporting_table_checkpoint_col)

        arft_count = aggregated_reporting_failed_table.count()
        art_count = aggregated_reporting_table.count()
        new_arft_checkpoint_row = new_arft_checkpoint_row + (arft_count,)
        new_art_checkpoint_row = new_art_checkpoint_row + (art_count,)
        if aggregated_reporting_failed_table.isEmpty() or aggregated_reporting_table.isEmpty():
            pass

        else:
            result = aggregated_reporting_table \
                .alias('report') \
                .join(aggregated_reporting_failed_table.alias('failed_report'),
                      (col(f'report.{self.aggregated_reporting.RULE_ID_COL_NAME}') == col(
                          f'failed_report.{self.aggregated_reporting_failed.RULE_ID_COL_NAME}'))
                      &
                      (col(f'report.{self.aggregated_reporting.JOB_RUN_ID_COL_NAME}') == col(
                          f'failed_report.{self.aggregated_reporting_failed.JOB_RUN_ID_COL_NAME}')))\
                .select('failed_report.*',
                        f'report.{self.aggregated_blend_data.ATTRIBUTE_COL_NAME}',
                        f'report.{self.aggregated_blend_data.THRESHOLD_PERCT_COL_NAME}',
                        f'report.{self.aggregated_blend_data.FAIL_COUNT_COL_NAME}',
                        f'report.{self.aggregated_blend_data.PASS_COUNT_COL_NAME}',
                        f'report.{self.aggregated_blend_data.TOTAL_RECORDS_COL_NAME}',
                        f'report.{self.aggregated_blend_data.DQ_METRIC_COL_NAME}')\
                .withColumn(self.aggregated_blend_data.RECORD_CREATED_AT_COL_NAME, lit(self.run_time))

            self.report_client.save_data(result, self.aggregated_blend_data.get_qualified_table_name(), 'WRITE_APPEND')

        self.add_checkpoint_in_db(self.report_client, Row(new_arft_checkpoint_row))
        self.add_checkpoint_in_db(self.report_client, Row(new_art_checkpoint_row))
        print("Reporting Job is completed successfully")
