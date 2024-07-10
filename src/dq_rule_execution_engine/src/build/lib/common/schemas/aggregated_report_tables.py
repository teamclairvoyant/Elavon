from pyspark.sql.types import IntegerType, StringType, TimestampType, BooleanType, StructType, StructField

from dq_rule_execution_engine.src.common.schemas.table_definition import TableDefinition

DATASET_NAME = 'data_quality_operational_stats'


class AggregatedReportTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_aggregated_reporting'
        )
        self.JOB_RUN_ID_COL_NAME = 'job_run_id'
        self.JOB_RUN_ID_DATA_TYPE = IntegerType()
        self.RULESET_ID_COL_NAME = 'ruleset_id'
        self.RULESET_ID_DATA_TYPE = IntegerType()
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.TOTAL_RECORDS_COL_NAME = 'total_records'
        self.TOTAL_RECORDS_DATA_TYPE = IntegerType()
        self.PASS_COUNT_COL_NAME = 'pass_count'
        self.PASS_COUNT_DATA_TYPE = IntegerType()
        self.FAIL_COUNT_COL_NAME = 'fail_count'
        self.FAIL_COUNT_DATA_TYPE = IntegerType()
        self.IS_PROCESSED_COL_NAME = 'is_processed'
        self.IS_PROCESSED_DATA_TYPE = StringType()
        self.IS_RULE_PASSED_COL_NAME = 'is_rule_passed'
        self.IS_RULE_PASSED_DATA_TYPE = BooleanType()
        self.EXCEPTION_SUMMARY_COL_NAME = 'exception_summary'
        self.EXCEPTION_SUMMARY_DATA_TYPE = StringType()
        self.START_TIME_COL_NAME = 'start_time'
        self.START_TIME_DATA_TYPE = TimestampType()
        self.END_TIME_COL_NAME = 'end_time'
        self.END_TIME_DATA_TYPE = TimestampType()
        self.TOTAL_EXECUTION_TIME_COL_NAME = 'total_execution_time'
        self.TOTAL_EXECUTION_TIME_DATA_TYPE = IntegerType()
        self.CREATED_TIME_COL_NAME = 'created_time'
        self.CREATED_TIME_DATA_TYPE = TimestampType()
        self.DQ_METRIC_COL_NAME = 'dq_metric'
        self.DQ_METRIC_DATA_TYPE = StringType()
        self.RULE_TEMPLATE_NAME_COL_NAME = 'rule_template_name'
        self.RULE_TEMPLATE_NAME_DATA_TYPE = StringType()
        self.RULE_NAME_COL_NAME = 'rule_name'
        self.RULE_NAME_DATA_TYPE = StringType()
        self.RULE_DESC_COL_NAME = 'rule_desc'
        self.RULE_DESC_DATA_TYPE = StringType()
        self.RULESET_NAME_COL_NAME = 'ruleset_name'
        self.RULESET_NAME_DATA_TYPE = StringType()
        self.ENTITY_ID_COL_NAME = 'entity_id'
        self.ENTITY_ID_DATA_TYPE = IntegerType()
        self.ENTITY_BEHAVIOUR_COL_NAME = 'entity_behaviour'
        self.ENTITY_BEHAVIOUR_DATA_TYPE = StringType()
        self.IS_PRIMARY_COL_NAME = 'is_primary'
        self.IS_PRIMARY_DATA_TYPE = StringType()
        self.ENTITY_PHYSICAL_NAME_COL_NAME = 'entity_physical_name'
        self.ENTITY_PHYSICAL_NAME_DATA_TYPE = StringType()
        self.ENTITY_PRIMARY_KEY_COL_NAME = 'entity_primary_key'
        self.ENTITY_PRIMARY_KEY_DATA_TYPE = StringType()
        self.ATTRIBUTE_COL_NAME = 'attribute'
        self.ATTRIBUTE_DATA_TYPE = StringType()
        self.THRESHOLD_PERCT_COL_NAME = 'threshold_perct'
        self.THRESHOLD_PERCT_DATA_TYPE = StringType()
        self.RECORD_CREATED_AT_COL_NAME = 'record_created_at'
        self.RECORD_CREATED_AT_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.JOB_RUN_ID_COL_NAME, self.JOB_RUN_ID_DATA_TYPE, True),
            StructField(self.RULESET_ID_COL_NAME, self.RULESET_ID_DATA_TYPE, True),
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.TOTAL_RECORDS_COL_NAME, self.TOTAL_RECORDS_DATA_TYPE, True),
            StructField(self.PASS_COUNT_COL_NAME, self.PASS_COUNT_DATA_TYPE, True),
            StructField(self.FAIL_COUNT_COL_NAME, self.FAIL_COUNT_DATA_TYPE, True),
            StructField(self.IS_PROCESSED_COL_NAME, self.IS_PROCESSED_DATA_TYPE, True),
            StructField(self.IS_RULE_PASSED_COL_NAME, self.IS_RULE_PASSED_DATA_TYPE, True),
            StructField(self.EXCEPTION_SUMMARY_COL_NAME, self.EXCEPTION_SUMMARY_DATA_TYPE, True),
            StructField(self.START_TIME_COL_NAME, self.START_TIME_DATA_TYPE, True),
            StructField(self.END_TIME_COL_NAME, self.END_TIME_DATA_TYPE, True),
            StructField(self.TOTAL_EXECUTION_TIME_COL_NAME, self.TOTAL_EXECUTION_TIME_DATA_TYPE, True),
            StructField(self.CREATED_TIME_COL_NAME, self.CREATED_TIME_DATA_TYPE, True),
            StructField(self.DQ_METRIC_COL_NAME, self.DQ_METRIC_DATA_TYPE, True),
            StructField(self.RULE_TEMPLATE_NAME_COL_NAME, self.RULE_TEMPLATE_NAME_DATA_TYPE, True),
            StructField(self.RULE_NAME_COL_NAME, self.RULE_NAME_DATA_TYPE, True),
            StructField(self.RULE_DESC_COL_NAME, self.RULE_DESC_DATA_TYPE, True),
            StructField(self.RULESET_NAME_COL_NAME, self.RULESET_NAME_DATA_TYPE, True),
            StructField(self.ENTITY_ID_COL_NAME, self.ENTITY_ID_DATA_TYPE, True),
            StructField(self.ENTITY_BEHAVIOUR_COL_NAME, self.ENTITY_BEHAVIOUR_DATA_TYPE, True),
            StructField(self.IS_PRIMARY_COL_NAME, self.IS_PRIMARY_DATA_TYPE, True),
            StructField(self.ENTITY_PHYSICAL_NAME_COL_NAME, self.ENTITY_PHYSICAL_NAME_DATA_TYPE, True),
            StructField(self.ENTITY_PRIMARY_KEY_COL_NAME, self.ENTITY_PRIMARY_KEY_DATA_TYPE, True),
            StructField(self.ATTRIBUTE_COL_NAME, self.ATTRIBUTE_DATA_TYPE, True),
            StructField(self.THRESHOLD_PERCT_COL_NAME, self.THRESHOLD_PERCT_DATA_TYPE, True),
            StructField(self.RECORD_CREATED_AT_COL_NAME, self.RECORD_CREATED_AT_DATA_TYPE, True)
        ])


class AggregatedReportFailedInfoTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_aggregated_reporting_failed_info'
        )
        self.JOB_RUN_ID_COL_NAME = 'job_run_id'
        self.JOB_RUN_ID_DATA_TYPE = IntegerType()
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.RULE_NAME_COL_NAME = 'rule_name'
        self.RULE_NAME_DATA_TYPE = StringType()
        self.RULE_DESC_COL_NAME = 'rule_desc'
        self.RULE_DESC_DATA_TYPE = StringType()
        self.ENTITY_PHYSICAL_NAME_COL_NAME = 'entity_physical_name'
        self.ENTITY_PHYSICAL_NAME_DATA_TYPE = StringType()
        self.ENTITY_PRIMARY_KEY_COL_NAME = 'entity_primary_key'
        self.ENTITY_PRIMARY_KEY_DATA_TYPE = StringType()
        self.DATA_OBJECT_KEY_COL_NAME = 'data_object_key'
        self.DATA_OBJECT_KEY_DATA_TYPE = StringType()
        self.EXCEPTION_SUMMARY_COL_NAME = 'exception_summary'
        self.EXCEPTION_SUMMARY_DATA_TYPE = StringType()
        self.CREATED_TIME_COL_NAME = 'created_time'
        self.CREATED_TIME_DATA_TYPE = TimestampType()
        self.RECORD_CREATED_AT_COL_NAME = 'record_created_at'
        self.RECORD_CREATED_AT_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.JOB_RUN_ID_COL_NAME, self.JOB_RUN_ID_DATA_TYPE, True),
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.RULE_NAME_COL_NAME, self.RULE_NAME_DATA_TYPE, True),
            StructField(self.RULE_DESC_COL_NAME, self.RULE_DESC_DATA_TYPE, True),
            StructField(self.ENTITY_PHYSICAL_NAME_COL_NAME, self.ENTITY_PHYSICAL_NAME_DATA_TYPE, True),
            StructField(self.ENTITY_PRIMARY_KEY_COL_NAME, self.ENTITY_PRIMARY_KEY_DATA_TYPE, True),
            StructField(self.DATA_OBJECT_KEY_COL_NAME, self.DATA_OBJECT_KEY_DATA_TYPE, True),
            StructField(self.EXCEPTION_SUMMARY_COL_NAME, self.EXCEPTION_SUMMARY_DATA_TYPE, True),
            StructField(self.CREATED_TIME_COL_NAME, self.CREATED_TIME_DATA_TYPE, True),
            StructField(self.RECORD_CREATED_AT_COL_NAME, self.RECORD_CREATED_AT_DATA_TYPE, True)
        ])


class AggregatedBlendDataTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_aggregated_blend_data'
        )
        self.JOB_RUN_ID_COL_NAME = 'job_run_id'
        self.JOB_RUN_ID_DATA_TYPE = IntegerType()
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.RULE_NAME_COL_NAME = 'rule_name'
        self.RULE_NAME_DATA_TYPE = StringType()
        self.RULE_DESC_COL_NAME = 'rule_desc'
        self.RULE_DESC_DATA_TYPE = StringType()
        self.ENTITY_PHYSICAL_NAME_COL_NAME = 'entity_physical_name'
        self.ENTITY_PHYSICAL_NAME_DATA_TYPE = StringType()
        self.ENTITY_PRIMARY_KEY_COL_NAME = 'entity_primary_key'
        self.ENTITY_PRIMARY_KEY_DATA_TYPE = StringType()
        self.DATA_OBJECT_KEY_COL_NAME = 'data_object_key'
        self.DATA_OBJECT_KEY_DATA_TYPE = StringType()
        self.EXCEPTION_SUMMARY_COL_NAME = 'exception_summary'
        self.EXCEPTION_SUMMARY_DATA_TYPE = StringType()
        self.CREATED_TIME_COL_NAME = 'created_time'
        self.CREATED_TIME_DATA_TYPE = TimestampType()
        self.ATTRIBUTE_COL_NAME = 'attribute'
        self.ATTRIBUTE_DATA_TYPE = StringType()
        self.THRESHOLD_PERCT_COL_NAME = 'threshold_perct'
        self.THRESHOLD_PERCT_DATA_TYPE = StringType()
        self.FAIL_COUNT_COL_NAME = 'fail_count'
        self.FAIL_COUNT_DATA_TYPE = IntegerType()
        self.PASS_COUNT_COL_NAME = 'pass_count'
        self.PASS_COUNT_DATA_TYPE = IntegerType()
        self.TOTAL_RECORDS_COL_NAME = 'total_records'
        self.TOTAL_RECORDS_DATA_TYPE = IntegerType()
        self.DQ_METRIC_COL_NAME = 'dq_metric'
        self.DQ_METRIC_DATA_TYPE = StringType()
        self.RECORD_CREATED_AT_COL_NAME = 'record_created_at'
        self.RECORD_CREATED_AT_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.JOB_RUN_ID_COL_NAME, self.JOB_RUN_ID_DATA_TYPE, True),
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.RULE_NAME_COL_NAME, self.RULE_NAME_DATA_TYPE, True),
            StructField(self.RULE_DESC_COL_NAME, self.RULE_DESC_DATA_TYPE, True),
            StructField(self.ENTITY_PHYSICAL_NAME_COL_NAME, self.ENTITY_PHYSICAL_NAME_DATA_TYPE, True),
            StructField(self.ENTITY_PRIMARY_KEY_COL_NAME, self.ENTITY_PRIMARY_KEY_DATA_TYPE, True),
            StructField(self.DATA_OBJECT_KEY_COL_NAME, self.DATA_OBJECT_KEY_DATA_TYPE, True),
            StructField(self.EXCEPTION_SUMMARY_COL_NAME, self.EXCEPTION_SUMMARY_DATA_TYPE, True),
            StructField(self.CREATED_TIME_COL_NAME, self.CREATED_TIME_DATA_TYPE, True),
            StructField(self.ATTRIBUTE_COL_NAME, self.ATTRIBUTE_DATA_TYPE, True),
            StructField(self.THRESHOLD_PERCT_COL_NAME, self.THRESHOLD_PERCT_DATA_TYPE, True),
            StructField(self.FAIL_COUNT_COL_NAME, self.FAIL_COUNT_DATA_TYPE, True),
            StructField(self.PASS_COUNT_COL_NAME, self.PASS_COUNT_DATA_TYPE, True),
            StructField(self.TOTAL_RECORDS_COL_NAME, self.TOTAL_RECORDS_DATA_TYPE, True),
            StructField(self.DQ_METRIC_COL_NAME, self.DQ_METRIC_DATA_TYPE, True),
            StructField(self.RECORD_CREATED_AT_COL_NAME, self.RECORD_CREATED_AT_DATA_TYPE, True)
        ])


class AggregatedReportJobCheckpoints(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_aggregated_report_job_checkpoints'
        )
        self.JOB_NAME_COL_NAME = 'job_name'
        self.JOB_IDENTIFIER_DATA_TYPE = StringType()
        self.TABLE_NAME_COL_NAME = 'table_name'
        self.TABLE_NAME_DATA_TYPE = StringType()
        self.RECORD_CREATED_AT_COL_NAME = 'record_created_at'
        self.RECORD_CREATED_AT_DATA_TYPE = TimestampType()
        self.LAST_RUN_AT_COL_NAME = 'last_run_at'
        self.LAST_RUN_AT_DATA_TYPE = TimestampType()
        self.RECORDS_PROCESSED_COUNT_COL_NAME = 'record_processed_count'
        self.RECORDS_PROCESSED_COUNT_DATA_TYPE = IntegerType()

    def get_schema(self):
        return StructType([
            StructField(self.JOB_NAME_COL_NAME, self.JOB_IDENTIFIER_DATA_TYPE, True),
            StructField(self.TABLE_NAME_COL_NAME, self.TABLE_NAME_DATA_TYPE, True),
            StructField(self.LAST_RUN_AT_COL_NAME, self.LAST_RUN_AT_DATA_TYPE, True),
            StructField(self.RECORD_CREATED_AT_COL_NAME, self.RECORD_CREATED_AT_DATA_TYPE, True),
            StructField(self.RECORDS_PROCESSED_COUNT_COL_NAME, self.RECORDS_PROCESSED_COUNT_DATA_TYPE, True)
        ])
