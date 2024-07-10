from dq_rule_execution_engine.src.app_context import AppContext
from dq_rule_execution_engine.src.execution_results_writer import ExecutionResultsWriter
from dq_rule_execution_engine.src.rule_set_executor import RuleSetExecutor


def execute(run_time_parameters):
    context = AppContext(run_time_parameters)
    context.build()
    result = RuleSetExecutor(context).execute()
    ExecutionResultsWriter(context).write(result)
