from dq_rule_execution_engine.src.query_executor import execute_total_records_query


class RecordCountCheck:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()

    def execute(self):
        filter_condition = self.context.get_rule_property('FILTER_CONDITIONS')
        record_count = self.context.get_rule_property('RECORD_COUNT')
        entity = self.context.get_source_entity()
        entity_sub_type = entity['entity_sub_type']
        entity_physical_name = entity['entity_physical_name']
        if entity_sub_type == 'BIG_QUERY':
            entity_physical_name = entity['entity_name']
        total_records_query = f"select count(*) as total_count from {entity_physical_name} where {filter_condition}"
        total_records_query_results = execute_total_records_query(entity, total_records_query, self.context)
        total_records_query_results['rule_pass'] = total_records_query_results['total_records_count'] == record_count
        total_records_query_results['is_rule_passed'] = total_records_query_results['total_records_count'] == record_count
        return total_records_query_results
