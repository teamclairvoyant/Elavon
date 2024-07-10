from dq_rule_execution_engine.src.query_executor import execute_failed_records_query


class ColumnCountCheck:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()

    def execute(self):
        column_count = self.context.get_rule_property('COLUMN_COUNT')
        entity = self.context.get_source_entity()
        entity_sub_type = entity['entity_sub_type']
        entity_physical_name = entity['entity_physical_name']
        if entity_sub_type == 'BIG_QUERY':
            entity_physical_name = entity['entity_name']
        failed_records_query = f"select * from {entity_physical_name} where 1==2"
        total_records_query_results = execute_failed_records_query(entity, failed_records_query, self.context)
        failed_records = total_records_query_results['failed_records']
        total_records_query_results['rule_pass'] = column_count == len(failed_records.columns)
        total_records_query_results['total_records_count'] = 0
        total_records_query_results['total_failed_records'] = 0
        total_records_query_results['exception_context'] = {'column_count': len(failed_records.columns)}

        total_records_query_results['is_rule_passed'] = column_count == len(failed_records.columns)

        return total_records_query_results
