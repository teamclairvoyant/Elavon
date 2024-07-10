from pyspark.sql.types import StringType, TimestampType, IntegerType, StructType, StructField, BooleanType

from dq_rule_execution_engine.src.common.schemas.table_definition import TableDefinition

DATASET_NAME = 'data_quality_operational_stats'


class RuleRunStatsTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='rule_run_stats'
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
            StructField(self.CREATED_TIME_COL_NAME, self.CREATED_TIME_DATA_TYPE, True)
        ])


class RuleExceptionsTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='rule_exceptions'
        )
        self.JOB_RUN_ID_COL_NAME = 'job_run_id'
        self.JOB_RUN_ID_DATA_TYPE = IntegerType()
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.RULESET_ID_COL_NAME = 'ruleset_id'
        self.RULESET_ID_DATA_TYPE = IntegerType()
        self.DATA_OBJECT_KEY_COL_NAME = 'data_object_key'
        self.DATA_OBJECT_KEY_DATA_TYPE = StringType()
        self.EXCEPTION_SUMMARY_COL_NAME = 'exception_summary'
        self.EXCEPTION_SUMMARY_DATA_TYPE = StringType()
        self.CREATED_TIME_COL_NAME = 'created_time'
        self.CREATED_TIME_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.JOB_RUN_ID_COL_NAME, self.JOB_RUN_ID_DATA_TYPE, True),
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.RULESET_ID_COL_NAME, self.RULESET_ID_DATA_TYPE, True),
            StructField(self.DATA_OBJECT_KEY_COL_NAME, self.DATA_OBJECT_KEY_DATA_TYPE, True),
            StructField(self.EXCEPTION_SUMMARY_COL_NAME, self.EXCEPTION_SUMMARY_DATA_TYPE, True),
            StructField(self.CREATED_TIME_COL_NAME, self.CREATED_TIME_DATA_TYPE, True)
        ])