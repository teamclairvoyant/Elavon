from dq_rule_execution_engine.src.query_executor import execute_rule_queries


class NullCheck:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()

    def execute(self):
        base_criteria_column = self.context.get_rule_property('BASE_CRITERIA_COLUMN')
        filter_condition = self.context.get_rule_property('FILTER_CONDITIONS')
        base_criteria = self.context.get_template_property('BASE_CRITERIA')
        entity = self.context.get_source_entity()
        primary_key = entity['primary_key']
        entity_sub_type = entity['entity_sub_type']
        entity_physical_name = entity['entity_physical_name']
        if entity_sub_type == 'BIG_QUERY':
            entity_physical_name = entity['entity_name']
        base_criteria = base_criteria.replace('{BASE_CRITERIA_COLUMN}', base_criteria_column)
        failed_records_query = f"select {primary_key} from {entity_physical_name} where {base_criteria} and {filter_condition}"
        total_records_query = f"select count(*) as total_count from {entity_physical_name} where {filter_condition}"
        return execute_rule_queries(entity, failed_records_query, total_records_query, self.context)
