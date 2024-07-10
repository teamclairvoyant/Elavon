from pathlib import Path

from dq_rule_execution_engine.src.app_context import AppContext
from dq_rule_execution_engine.src.rule_set_executor import RuleSetExecutor


def get_rule_execution_result():
    app_conf_file_path = Path(__file__).parent / "conf/properties/application.properties"
    ruleset_conf_file_path = Path(__file__).parent / "conf/json/local/payment_column_count_check.json"
    rule_set_path = str(ruleset_conf_file_path)
    app_conf = str(app_conf_file_path)
    run_time_parameters = f'rule_set_path={rule_set_path},app_conf={app_conf},job_id=12345678'
    context = AppContext(run_time_parameters)
    context.build()
    return RuleSetExecutor(context).execute()


class TestNullCheck:
    result = get_rule_execution_result()

    def test_result_has_record_count_rule(self):
        print(self.result)
        assert 31783942 in self.result, 'result map should have column count check rule id'

    def test_result_has_correct_failed_records_query(self):
        failed_records_query = self.result[31783942]['failed_records_query']
        expected_failed_records_query = "select * from payment where 1==2"
        assert failed_records_query == expected_failed_records_query, 'failed records query should form correctly'

    def test_result_has_correct_rule_pass_value(self):
        rule_pass_status = self.result[31783942]['rule_pass']
        assert rule_pass_status == False, 'rule_pass_status should be false'

    def test_result_has_correct_exception_context(self):
        exception_context = self.result[31783942]['exception_context']
        assert exception_context == {'column_count':31}, 'exception_context should have correct value'
