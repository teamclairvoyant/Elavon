import json

import pyspark
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StringType, TimestampType, IntegerType, StructType, BooleanType, FloatType

from dq_rule_execution_engine.src.constants import BLANK
from dq_rule_execution_engine.src.utils import get_spark_session, get_empty_data_frame, get_current_time, get_duration
from dq_rule_execution_engine.src.writer import write


def run_stats_schema():
    schema = StructType([
        StructField('job_run_id', IntegerType(), True),
        StructField('ruleset_id', IntegerType(), True),
        StructField('is_passed', BooleanType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('total_execution_time', FloatType(), True),
        StructField('created_time', TimestampType(), True),
        StructField('entity_name', StringType(), True),
        StructField('description', StringType(), True)
    ])
    return schema


def rule_run_stats_schema():
    schema = StructType([
        StructField('job_run_id', IntegerType(), True),
        StructField('ruleset_id', IntegerType(), True),
        StructField('rule_id', IntegerType(), True),
        StructField('total_records', IntegerType(), True),
        StructField('pass_count', IntegerType(), True),
        StructField('fail_count', IntegerType(), True),
        StructField('is_processed', StringType(), True),
        StructField('is_rule_passed', BooleanType(), True),
        StructField('exception_summary', StringType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('total_execution_time', FloatType(), True),
        StructField('created_time', TimestampType(), True),
        StructField('entity_name', StringType(), True),
        StructField('description', StringType(), True)
    ])
    return schema


def query_stats_schema():
    schema = StructType([
        StructField('job_run_id', IntegerType(), True),
        StructField('rule_id', IntegerType(), True),
        StructField('query', StringType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('total_execution_time', FloatType(), True),
        StructField('created_time', TimestampType(), True)
    ])
    return schema


def rule_exceptions():
    schema = StructType([
        StructField('job_run_id', IntegerType(), True),
        StructField('rule_id', IntegerType(), True),
        StructField('ruleset_id', IntegerType(), True),
        StructField('data_object_key', StringType(), True),
        StructField('exception_summary', StringType(), True),
        StructField('created_time', TimestampType(), True)
    ])
    return schema


class ExecutionResultsWriter:
    def __init__(self, context):
        self.context = context

    def write(self, result):
        rule_set_execution_start_time = result['rule_set_execution_start_time']
        rule_set_execution_end_time = result['rule_set_execution_end_time']
        del (result['rule_set_execution_start_time'])
        del (result['rule_set_execution_end_time'])
        query_stats_list = []
        rule_stats_list = []
        query_list = []
        rule_exception_df = get_empty_data_frame(rule_exceptions())
        failed_records_count = 0
        is_passed = True

        for rule_id, rule_execution_result in result.items():
            if 'is_data_diff' in rule_execution_result and rule_execution_result['is_data_diff']:
                rule_run_stat_entry, query_list = self.handle_data_diff(rule_id, rule_execution_result)
                is_rule_passed = rule_execution_result['is_rule_passed']
                is_passed = is_passed and is_rule_passed

            elif 'is_profiler' in rule_execution_result and rule_execution_result['is_profiler']:
                rule_run_stat_entry, query_list = self.handle_profiler(rule_id, rule_execution_result)

            else:
                if 'failed_records_query' in rule_execution_result:
                    query_list.append(self.get_query_details(rule_id, rule_execution_result, 'failed'))
                    failed_records_count = rule_execution_result['failed_records'].count()

                if 'total_records_query' in rule_execution_result:
                    query_list.append(self.get_query_details(rule_id, rule_execution_result, 'total'))

                rule_run_stat_entry = self.get_rule_execution_details(rule_id, rule_execution_result,
                                                                      failed_records_count,rule_execution_result['rule_description'])
                rule_exception_df = rule_exception_df.union(
                    self.get_rule_exception_df(rule_execution_result, rule_id, failed_records_count))

                is_rule_passed = rule_execution_result['is_rule_passed']
                is_passed = is_passed and is_rule_passed

            rule_stats_list.append(rule_run_stat_entry)
            query_stats_list.extend(query_list)

        self.write_run_stats(rule_set_execution_start_time, rule_set_execution_end_time, is_passed)
        self.write_rule_exceptions(rule_exception_df)
        self.write_rule_run_stats(rule_stats_list)
        self.write_query_stats(query_stats_list)

    def write_run_stats(self, rule_set_execution_start_time, rule_set_execution_end_time, is_passed):
        run_stats = [self.context.get_job_run_id(),
                     self.context.get_ruleset_id(),
                     is_passed,
                     rule_set_execution_start_time,
                     rule_set_execution_end_time,
                     get_duration(
                         rule_set_execution_end_time, rule_set_execution_start_time),
                     get_current_time(),
                     self.context.get_entity_name(),
                     self.context.get_description()
                     ]
        run_stats_df = get_spark_session().createDataFrame([run_stats], run_stats_schema())
        write(run_stats_df, 'run_stats', self.context)

    def write_rule_run_stats(self, rule_stats_list):
        rule_run_stats_df = get_spark_session().createDataFrame(rule_stats_list, rule_run_stats_schema())
        write(rule_run_stats_df, 'rule_run_stats', self.context)

    def write_query_stats(self, query_stats_list):
        query_stats_df = get_spark_session().createDataFrame(query_stats_list, query_stats_schema())
        write(query_stats_df, 'query_stats', self.context)

    def write_rule_exceptions(self, rule_exception_details):
        if rule_exception_details.count() > 0:
            write(rule_exception_details, 'rule_exceptions', self.context)

    def get_query_details(self, rule_id, rule_execution_result, query_type):
        query_key = f'{query_type}_records_query'
        start_time_key = f'{query_type}_records_query_execution_start_time'
        end_time_key = f'{query_type}_records_query_execution_end_time'
        return [self.context.get_job_run_id(),
                rule_id,
                rule_execution_result[query_key],
                rule_execution_result[start_time_key],
                rule_execution_result[end_time_key],
                get_duration(rule_execution_result[end_time_key], rule_execution_result[start_time_key]),
                get_current_time()]

    def get_rule_execution_details(self, rule_id, rule_execution_result, failed_records_count,rule_desc):
        pass_records_count = rule_execution_result['total_records_count'] - failed_records_count
        exception_context = ''
        if 'exception_context' in rule_execution_result:
            exception_context = json.dumps(rule_execution_result['exception_context'], indent=4)

        return [self.context.get_job_run_id(),
                self.context.get_ruleset_id(),
                rule_id,
                rule_execution_result['total_records_count'],
                pass_records_count,
                failed_records_count,
                'Y',
                rule_execution_result['is_rule_passed'],
                exception_context,
                rule_execution_result['rule_execution_start_time'],
                rule_execution_result['rule_execution_end_time'],
                get_duration(rule_execution_result['rule_execution_end_time'],
                             rule_execution_result['rule_execution_start_time']),
                get_current_time(),
                self.context.get_entity_physical_name(),
                rule_desc
                ]

    def get_rule_exception_df(self, rule_execution_result, rule_id, failed_records_count):

        rule_exception_details = get_empty_data_frame(rule_exceptions())
        if failed_records_count > 0:
            rule_exception_details = rule_execution_result['failed_records']
            primary_key = rule_execution_result['primary_key']

            rule_exception_details = rule_exception_details \
                .withColumn('job_run_id', lit(self.context.get_job_run_id()).cast(IntegerType())) \
                .withColumn('rule_id', lit(rule_id).cast(IntegerType())) \
                .withColumn('rule_set_id', lit(self.context.get_ruleset_id()).cast(IntegerType())) \
                .withColumn('data_object_key', pyspark.sql.functions.concat_ws(',', *primary_key.split(','))) \
                .withColumn('exception_summary', lit('').cast(StringType())) \
                .withColumn('created_time', lit(get_current_time()).cast(TimestampType()))

            for column_name in primary_key.split(','):
                rule_exception_details = rule_exception_details.drop(column_name)
        return rule_exception_details

    def handle_data_diff(self, rule_id, rule_execution_result):
        write(rule_execution_result['comparison_summary'], 'comparison_summary', self.context)
        write(rule_execution_result['comparison_details'], 'comparison_details', self.context)
        source_query_stat = [self.context.get_job_run_id(),
                             rule_id,
                             rule_execution_result['source_query'],
                             rule_execution_result['source_query_start_time'],
                             rule_execution_result['source_query_end_time'],
                             get_duration(rule_execution_result['source_query_end_time'],
                                          rule_execution_result['source_query_start_time']),
                             get_current_time()]
        target_query_stat = [self.context.get_job_run_id(),
                             rule_id,
                             rule_execution_result['target_query'],
                             rule_execution_result['target_query_start_time'],
                             rule_execution_result['target_query_end_time'],
                             get_duration(rule_execution_result['target_query_end_time'],
                                          rule_execution_result['target_query_start_time']),
                             get_current_time()]
        query_stats_list = [source_query_stat, target_query_stat]

        rule_execution_record = [self.context.get_job_run_id(),
                                 self.context.get_ruleset_id(),
                                 rule_id,
                                 rule_execution_result['source_count'],
                                 rule_execution_result['source_count'] - rule_execution_result[
                                     'records_mismatch_count'],
                                 rule_execution_result['records_mismatch_count'],
                                 'Y',
                                 rule_execution_result['is_rule_passed'],
                                 json.dumps(rule_execution_result['exception_summary'], indent=4),
                                 rule_execution_result['rule_execution_start_time'],
                                 rule_execution_result['rule_execution_end_time'],
                                 get_duration(rule_execution_result['rule_execution_end_time'],
                                              rule_execution_result['rule_execution_start_time']),
                                 get_current_time()]

        return rule_execution_record, query_stats_list

    def handle_profiler(self, rule_id, rule_execution_result):
        write(rule_execution_result['profiler_summary'], 'profiler_summary', self.context)
        write(rule_execution_result['profiler_details'], 'profiler_detail', self.context)
        if 'profiler_column_details' in rule_execution_result:
            write(rule_execution_result['profiler_column_details'], 'profiler_column_details', self.context)

        source_query_stat = [self.context.get_job_run_id(),
                             rule_id,
                             rule_execution_result['source_query'],
                             rule_execution_result['source_query_start_time'],
                             rule_execution_result['source_query_end_time'],
                             get_duration(rule_execution_result['source_query_end_time'],
                                          rule_execution_result['source_query_start_time']),
                             get_current_time()]
        query_stats_list = [source_query_stat]

        rule_execution_record = [self.context.get_job_run_id(),
                                 self.context.get_ruleset_id(),
                                 rule_id,
                                 rule_execution_result['source_count'],
                                 rule_execution_result['source_count'],
                                 0,
                                 'Y',
                                 True,
                                 BLANK,
                                 rule_execution_result['rule_execution_start_time'],
                                 rule_execution_result['rule_execution_end_time'],
                                 get_duration(rule_execution_result['rule_execution_end_time'],
                                              rule_execution_result['rule_execution_start_time']),
                                 get_current_time()]

        return rule_execution_record, query_stats_list
