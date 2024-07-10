from dq_rule_execution_engine.src.query_executor import execute_rule_queries


class SqlValidator:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()

    def execute(self):
        failed_records_query = self.context.get_rule_property('SOURCE_QUERY')
        filter_condition = self.context.get_rule_property('FILTER_CONDITIONS')
        entity = self.context.get_source_entity()
        entity_sub_type = entity['entity_sub_type']
        entity_physical_name = entity['entity_physical_name']
        if entity_sub_type == 'BIG_QUERY':
            entity_physical_name = entity['entity_name']
        total_records_query = f"select count(*)  as total_count from {entity_physical_name} where {filter_condition}"
        return execute_rule_queries(entity, failed_records_query, total_records_query,self.context)
