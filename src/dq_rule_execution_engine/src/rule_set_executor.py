from dq_rule_execution_engine.src.rules.rule_factory import RuleExecutorFactory
from dq_rule_execution_engine.src.utils import get_current_time
from dq_rule_execution_engine.src.utils import get_spark_session


class RuleSetExecutor:
    def __init__(self, context):
        self.context = context

    def execute(self):
        get_spark_session()
        execution_result = {'rule_set_execution_start_time': get_current_time()}
        rule_factory = RuleExecutorFactory(self.context)
        for rule in self.context.get_rules():
            self.context.set_current_rule(rule)
            rule_execution_start_time = get_current_time()
            rule_desc = self.context.get_rule_template_name()
            results = rule_factory.get_rule_executor().execute()
            results['rule_execution_end_time'] = get_current_time()
            results['rule_execution_start_time'] = rule_execution_start_time
            results['rule_description'] = rule_desc
            execution_result[self.context.get_rule_id()] = results

        execution_result['rule_set_execution_end_time'] = get_current_time()
        return execution_result
