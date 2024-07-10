from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from dq_rule_execution_engine.src.common.utils.tables_info_object_handler import TableInfoObjectHandler
from dq_rule_execution_engine.src.reports.report_generator import ReportGenerator


class DqAggregatedReportingJob(ReportGenerator):

    def __init__(
            self,
            spark: SparkSession,
            config: dict
    ):
        super().__init__(spark, job_name='dq_aggregated_reporting')
        #Reference table details object
        self.rules = TableInfoObjectHandler.rules_obj
        self.rule_entity_map = TableInfoObjectHandler.rule_entity_map_obj
        self.rule_properties = TableInfoObjectHandler.rule_properties_obj
        self.rule_set = TableInfoObjectHandler.rule_set_obj
        self.rule_template = TableInfoObjectHandler.rule_template_obj
        self.entity = TableInfoObjectHandler.entity_obj

        #Operational stat table details object
        self.rule_run_stats = TableInfoObjectHandler.rule_run_stats_obj

        #Output aggregated report table details object
        self.aggregated_reporting = TableInfoObjectHandler.aggregated_report_obj

        self.reference_client = self.get_db_client(section="reference", config=config)
        self.stats_client = self.get_db_client(section="stats", config=config)
        self.report_client = self.get_db_client(section="report", config=config)

    def run(self):
        rules_run_stats_table_name = self.rule_run_stats.get_qualified_table_name()
        rules_table_name = self.rules.get_qualified_table_name()
        rule_entity_map_table_name = self.rule_entity_map.get_qualified_table_name()
        rule_properties_table_name = self.rule_properties.get_qualified_table_name()
        rule_set_table_name = self.rule_set.get_qualified_table_name()
        rule_template_table_name = self.rule_template.get_qualified_table_name()
        entity_table_name = self.entity.get_qualified_table_name()

        rules_run_stats_table_checkpoint_col = self.rule_run_stats.CREATED_TIME_COL_NAME

        rules_run_stats_table, new_checkpoint_row = \
            self.get_incremental_table(client=self.stats_client,
                                       table_name=rules_run_stats_table_name,
                                       checkpoint_col_name=rules_run_stats_table_checkpoint_col)

        rrst_count = rules_run_stats_table.count()
        new_checkpoint_row = new_checkpoint_row + (rrst_count,)
        if rules_run_stats_table.isEmpty():
            pass

        else:
            rules_table = self.read_whole_table(client=self.reference_client,
                                                table_name=rules_table_name)
            rule_entity_map_table = self.read_whole_table(client=self.reference_client,
                                                          table_name=rule_entity_map_table_name)
            rule_properties_table = self.read_whole_table(client=self.reference_client,
                                                          table_name=rule_properties_table_name)
            rule_set_table = self.read_whole_table(client=self.reference_client,
                                                   table_name=rule_set_table_name)
            rule_template_table = self.read_whole_table(client=self.reference_client,
                                                        table_name=rule_template_table_name)
            entity_table = self.read_whole_table(client=self.reference_client,
                                                 table_name=entity_table_name)

            rule_properties_base_criteria_table = rule_properties_table \
                .where(col(self.rule_properties.RULE_PROP_KEY_COL_NAME) == "BASE_CRITERIA_COLUMN") \
                .select(col(self.rule_properties.RULE_PROP_VALUE_COL_NAME)
                        .alias(self.aggregated_reporting.ATTRIBUTE_COL_NAME),
                        col(self.rule_properties.RULE_ID_COL_NAME))

            rule_properties_threshold_perct_table = rule_properties_table \
                .where(col(self.rule_properties.RULE_PROP_KEY_COL_NAME) == "THRESHOLD_PERCT") \
                .select(col(self.rule_properties.RULE_PROP_VALUE_COL_NAME)
                        .alias(self.aggregated_reporting.THRESHOLD_PERCT_COL_NAME),
                        col(self.rule_properties.RULE_ID_COL_NAME))

            source_entity_map_table = rule_entity_map_table \
                .where((col(self.rule_entity_map.ENTITY_BEHAVIOUR_COL_NAME) == "source") &
                       (col(self.rule_entity_map.IS_PRIMARY_COL_NAME) == "True"))

            result = rules_run_stats_table \
                .alias('stats') \
                .join(rules_table.alias('rules'),
                      (col(f'stats.{self.rule_run_stats.RULE_ID_COL_NAME}') == col(
                          f'rules.{self.rules.RULE_ID_COL_NAME}'))
                      &
                      (col(f'stats.{self.rule_run_stats.RULESET_ID_COL_NAME}') == col(
                          f'rules.{self.rules.RULESET_ID_COL_NAME}'))) \
                .join(rule_properties_base_criteria_table.alias('pbc'),
                      (col(f'stats.{self.rule_run_stats.RULE_ID_COL_NAME}') == col(
                          f'pbc.{self.rule_properties.RULE_ID_COL_NAME}'))) \
                .join(rule_properties_threshold_perct_table.alias('ptp'),
                      (col(f'stats.{self.rule_run_stats.RULE_ID_COL_NAME}') == col(
                          f'ptp.{self.rule_properties.RULE_ID_COL_NAME}'))) \
                .join(rule_set_table.alias('rs'),
                      (col(f'stats.{self.rule_run_stats.RULESET_ID_COL_NAME}') == col(
                          f'rs.{self.rule_set.RULESET_ID_COL_NAME}'))) \
                .join(rule_template_table.alias('rt'),
                      (col(f'rules.{self.rules.RULE_TEMPLATE_ID_COL_NAME}') == col(
                          f'rt.{self.rule_template.RULE_TEMPLATE_ID_COL_NAME}'))) \
                .join(source_entity_map_table.alias('rem'),
                      (col(f'stats.{self.rule_run_stats.RULE_ID_COL_NAME}') == col(
                          f'rem.{self.rule_entity_map.RULE_ID_COL_NAME}'))) \
                .join(entity_table.alias('e'),
                      (col(f'rem.{self.rule_entity_map.ENTITY_ID_COL_NAME}') == col(
                          f'e.{self.entity.ENTITY_ID_COL_NAME}'))) \
                .select('stats.*', f'rules.{self.aggregated_reporting.RULE_NAME_COL_NAME}',
                        f'rules.{self.aggregated_reporting.RULE_DESC_COL_NAME}',
                        f'pbc.{self.aggregated_reporting.ATTRIBUTE_COL_NAME}',
                        f'ptp.{self.aggregated_reporting.THRESHOLD_PERCT_COL_NAME}',
                        f'rs.{self.aggregated_reporting.RULESET_NAME_COL_NAME}',
                        f'rt.{self.aggregated_reporting.DQ_METRIC_COL_NAME}',
                        f'rt.{self.aggregated_reporting.RULE_TEMPLATE_NAME_COL_NAME}',
                        f'rem.{self.aggregated_reporting.ENTITY_ID_COL_NAME}',
                        f'rem.{self.aggregated_reporting.ENTITY_BEHAVIOUR_COL_NAME}',
                        f'rem.{self.aggregated_reporting.IS_PRIMARY_COL_NAME}',
                        f'e.{self.aggregated_reporting.ENTITY_PRIMARY_KEY_COL_NAME}',
                        f'e.{self.aggregated_reporting.ENTITY_PHYSICAL_NAME_COL_NAME}') \
                .withColumn(self.aggregated_reporting.RECORD_CREATED_AT_COL_NAME, lit(self.run_time))

            self.report_client.save_data(result, self.aggregated_reporting.get_qualified_table_name(), 'WRITE_APPEND')

        self.add_checkpoint_in_db(self.report_client, Row(new_checkpoint_row))
        print("Reporting Job is completed successfully")
