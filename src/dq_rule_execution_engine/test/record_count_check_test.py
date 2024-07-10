from pathlib import Path

from dq_rule_execution_engine.src.app_context import AppContext
from dq_rule_execution_engine.src.rule_set_executor import RuleSetExecutor


def get_rule_execution_result():
    app_conf_file_path = Path(__file__).parent / "conf/properties/application.properties"
    ruleset_conf_file_path = Path(__file__).parent / "conf/json/local/payment_record_count_check.json"
    rule_set_path = str(ruleset_conf_file_path)
    app_conf = str(app_conf_file_path)
    run_time_parameters = f'rule_set_path={rule_set_path},app_conf={app_conf},job_id=12345678'
    context = AppContext(run_time_parameters)
    context.build()
    return RuleSetExecutor(context).execute()


class TestNullCheck:
    result = get_rule_execution_result()

    def test_result_has_record_count_rule(self):
        assert 31783941 in self.result, 'result map should have record count check rule id'

    def test_result_has_correct_total_records_query(self):
        total_records_query = self.result[31783941]['total_records_query']
        expected_total_records_query = "select count(*) as total_count from payment where 1=1"
        assert total_records_query == expected_total_records_query, 'total records query should form correctly'

    def test_result_has_correct_rule_pass_value(self):
        rule_pass_status = self.result[31783941]['rule_pass']
        assert rule_pass_status == False, 'rule_pass_status should be false'
