from dq_rule_execution_engine.src.common.schemas.aggregated_report_tables import *
from dq_rule_execution_engine.src.common.schemas.operational_stat_tables import *
from dq_rule_execution_engine.src.common.schemas.reference_tables import *


class TableInfoObjectHandler:

    rule_run_stats_obj = RuleRunStatsTable()
    rule_exceptions_obj = RuleExceptionsTable()

    aggregated_report_obj = AggregatedReportTable()
    aggregated_report_failed_info_obj = AggregatedReportFailedInfoTable()
    aggregated_blend_data_obj = AggregatedBlendDataTable()
    aggregated_report_job_checkpoint_obj = AggregatedReportJobCheckpoints()

    rules_obj = RulesTable()
    rule_properties_obj = RulePropertiesTable()
    rule_set_obj = RuleSetTable()
    rule_template_obj = RuleTemplateTable()
    rule_entity_map_obj = RuleEntityMapTable()
    entity_obj = EntityTable()
