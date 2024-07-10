from dq_rule_execution_engine.src.reader import read
from dq_rule_execution_engine.src.utils import get_current_time


def execute_rule_queries(entity, failed_records_query, total_records_query, context):
    failed_records_query_results = execute_failed_records_query(
         entity, failed_records_query,context)
    total_records_query_results = execute_total_records_query(entity, total_records_query,context)
    threshold_percent = float(context.get_rule_property('THRESHOLD_PERCT')) if context.is_key_exist_in_rule_property(
        'THRESHOLD_PERCT') else 100.00
    total_records_count = total_records_query_results['total_records_count']
    failed_record_count = failed_records_query_results['failed_records'].count()
    pass_record_count = total_records_count - failed_record_count
    is_rule_passed = pass_record_count * 100 / total_records_count >= threshold_percent
    return {**failed_records_query_results,
            **total_records_query_results,
            'primary_key': entity['primary_key'],
            'is_rule_passed': is_rule_passed}


def execute_failed_records_query(entity, failed_records_query,context):
    failed_records_query_execution_start_time = get_current_time()
    failed_records = read(entity, failed_records_query, context)
    failed_records_query_execution_end_time = get_current_time()
    return {'failed_records': failed_records,
            'failed_records_query': failed_records_query,
            'failed_records_query_execution_start_time': failed_records_query_execution_start_time,
            'failed_records_query_execution_end_time': failed_records_query_execution_end_time,
            }


def execute_total_records_query(entity, total_records_query,context):
    total_records_query_execution_start_time = get_current_time()
    total_records = read(entity, total_records_query, context)
    total_records_query_execution_end_time = get_current_time()
    return {'total_records_count': total_records.first()['total_count'],
            'total_records_query': total_records_query,
            'total_records_query_execution_start_time': total_records_query_execution_start_time,
            'total_records_query_execution_end_time': total_records_query_execution_end_time,
            }

