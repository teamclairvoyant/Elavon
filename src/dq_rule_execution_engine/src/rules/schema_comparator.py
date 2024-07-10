from dq_rule_execution_engine.src.constants import *
from dq_rule_execution_engine.src.reader import read
from dq_rule_execution_engine.src.utils import get_spark_session, get_empty_data_frame, get_unique_id, get_current_time


class SchemaComparator:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()
        self.context = context
        self.rule = self.context.get_current_rule()
        self.job_id = None
        self.rule_id = None
        self.time_created = get_current_time()
        self.source_unique_key = None
        self.source_unique_key_array = None
        self.target_unique_key = None
        self.target_unique_key_array = None
        self.source_entity_name = None
        self.target_entity_name = None
        self.summary = get_empty_data_frame(summary_schema())
        self.details = get_empty_data_frame(details_schema())
        self.source_entity_name = None
        self.target_entity_name = None
        self.results = {}

    def execute(self):
        source_entity = self.context.get_source_entity()
        target_entity = self.context.get_target_entity()
        self.source_unique_key = source_entity['primary_key']
        self.target_unique_key = target_entity['primary_key']
        self.source_unique_key_array = self.source_unique_key.split(',')
        self.target_unique_key_array = self.target_unique_key.split(',')

        self.source_entity_name = source_entity['entity_name']
        self.target_entity_name = target_entity['entity_name']

        self.job_id = self.context.get_job_run_id()
        self.rule_id = self.context.get_rule_id()
        source_query = f"select * from {self.source_entity_name} where 1==2"
        target_query = f"select * from {self.target_entity_name} where 1==2"
        self.results['source_query'] = source_query
        self.results['target_query'] = target_query
        source_query_execution_start_time = get_current_time()
        source = read(source_entity, source_query, self.context)
        self.results['source_query_end_time'] = get_current_time()
        self.results['source_query_start_time'] = source_query_execution_start_time

        target_query_execution_start_time = get_current_time()
        target = read(target_entity, target_query, self.context)
        self.results['target_query_end_time'] = get_current_time()
        self.results['target_query_start_time'] = target_query_execution_start_time

        summary, details = self.compare_schema(source, target)
        self.results['comparison_summary'] = summary
        self.results['comparison_details'] = details
        self.results['is_data_diff'] = True

        threshold_percent = float(
            self.context.get_rule_property('THRESHOLD_PERCT')) if self.context.is_key_exist_in_rule_property(
            'THRESHOLD_PERCT') else 100.00
        source_column_count = self.results['source_count']
        passed_record_count = self.results['records_matched_count']
        is_rule_passed = passed_record_count * 100 / source_column_count >= threshold_percent
        self.results['is_rule_passed'] = is_rule_passed

        del self.results['records_matched_count']

        return self.results

    def compare_schema(self, source, target):
        source_columns = set(source.columns)
        target_columns = set(target.columns)

        # Find columns missing in source
        additional_in_target = target_columns - source_columns

        # Find columns missing in target
        additional_in_source = source_columns - target_columns

        mismatched_data_types = []

        common_columns = source_columns.intersection(target_columns)

        for col in common_columns:
            if source.schema[col].dataType != target.schema[col].dataType:
                mismatched_data_types.append((col, dict(source.dtypes)[col], dict(target.dtypes)[col]))

        summary_row_list = []
        sample_values = list(additional_in_source)
        if len(additional_in_source) > 5:
            sample_values = list(additional_in_source)[:5]
        summary_key_additional_in_source = get_unique_id()
        summary_row_list.append([summary_key_additional_in_source, self.job_id, self.rule_id, self.source_entity_name,
                                 self.target_entity_name,
                                 self.source_unique_key,
                                 SOURCE_ADDITIONAL_COLUMN,
                                 len(additional_in_source), SOURCE_TO_TARGET, sample_values, self.time_created])

        sample_values = list(additional_in_target)
        if len(additional_in_target) > 5:
            sample_values = list(additional_in_target)[:5]
        summary_key_additional_in_target = get_unique_id()
        summary_row_list.append([summary_key_additional_in_target, self.job_id, self.rule_id, self.source_entity_name,
                                 self.target_entity_name,
                                 self.source_unique_key,
                                 TARGET_ADDITIONAL_COLUMN,
                                 len(additional_in_target), SOURCE_TO_TARGET, sample_values, self.time_created])

        columns_with_mismatched_data_types = [entry[0] for entry in mismatched_data_types]

        sample_values = list(columns_with_mismatched_data_types)
        if len(columns_with_mismatched_data_types) > 5:
            sample_values = list(columns_with_mismatched_data_types)[:5]

        summary_key_column_matched_data_type_mismatched = get_unique_id()
        summary_row_list.append([summary_key_column_matched_data_type_mismatched, self.job_id, self.rule_id, self.source_entity_name,
                                 self.target_entity_name,
                                 self.source_unique_key,
                                 COLUMN_MATCHED_DATA_TYPE_MISMATCHED,
                                 len(mismatched_data_types), SOURCE_TO_TARGET, sample_values,
                                 self.time_created])

        matched_columns = common_columns - set(mismatched_data_types)
        sample_values = list(matched_columns)
        if len(matched_columns) > 5:
            sample_values = list(matched_columns)[:5]

        summary_row_list.append([get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                                 self.target_entity_name,
                                 self.source_unique_key,
                                 COLUMN_MATCHED_DATA_TYPE_MATCHED,
                                 len(matched_columns), SOURCE_TO_TARGET, sample_values,
                                 self.time_created])

        summary_row_list.append([get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                                 self.target_entity_name,
                                 self.source_unique_key,
                                 SOURCE_COLUMN_COUNT,
                                 len(source_columns), SOURCE_TO_SOURCE, BLANK, self.time_created])

        summary_row_list.append([get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                                 self.target_entity_name,
                                 self.source_unique_key,
                                 TARGET_COLUMN_COUNT,
                                 len(target_columns), TARGET_TO_TARGET, BLANK, self.time_created])

        self.summary = self.summary.union(
            get_spark_session().createDataFrame(summary_row_list, summary_schema()))

        exception_summary = {
            "source_column_count": len(source_columns),
            "target_column_count": len(target_columns),
            "column_datatype_mismatch_count": len(mismatched_data_types),
            "column_datatype_match_count": len(matched_columns)
        }
        self.results['source_count'] = len(source_columns)
        self.results['records_mismatch_count'] = len(mismatched_data_types)
        self.results['records_matched_count'] = len(matched_columns)

        self.results['exception_summary'] = exception_summary

        details_rows = []

        for column in additional_in_source:
            details_rows.append(
                [get_unique_id(), summary_key_additional_in_source, self.source_unique_key + "." + column, column, 1, 0,dict(source.dtypes)[column],BLANK,
                 self.time_created])

        for column in additional_in_target:
            details_rows.append(
                [get_unique_id(), summary_key_additional_in_target, self.target_unique_key + "." + column, column, 0, 1,BLANK,dict(target.dtypes)[column],
                 self.time_created])

        for entry in mismatched_data_types:
            details_rows.append(
                [get_unique_id(), summary_key_column_matched_data_type_mismatched,
                 self.source_unique_key + "." + self.target_unique_key + "." + entry[0],
                 entry[0], 1, 1,entry[1],entry[2], self.time_created])

        self.details = self.details.union(get_spark_session().createDataFrame(details_rows, details_schema()))

        self.results['exception_summary'] = exception_summary

        return self.summary, self.details
