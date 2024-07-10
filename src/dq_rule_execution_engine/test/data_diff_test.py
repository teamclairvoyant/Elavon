from pathlib import Path

from dq_rule_execution_engine.src.app_context import AppContext
from dq_rule_execution_engine.src.rule_set_executor import RuleSetExecutor

def get_rule_execution_result():
    app_conf_file_path = Path(__file__).parent / "conf/properties/application.properties"
    ruleset_conf_file_path = Path(__file__).parent / "conf/json/local/payments_data_diff.json"
    rule_set_path = str(ruleset_conf_file_path)
    app_conf = str(app_conf_file_path)
    run_time_parameters = f'rule_set_path={rule_set_path},app_conf={app_conf},job_id=12345680'
    context = AppContext(run_time_parameters)
    context.build()
    return RuleSetExecutor(context).execute()


class TestDataDiff:
    result = get_rule_execution_result()

    def test_result_has_data_diff_rule(self):
        assert 31783850 in self.result, 'result map should have data diff rule id'

    def test_source_target_count(self):
        print(self.result[31783850])
        rule_execution_result = self.result[31783850]
        assert 1005 == rule_execution_result['source_count']
        assert 1005 == rule_execution_result['target_count']
        assert 998 == rule_execution_result['records_match_count']
        assert 2 == rule_execution_result['records_mismatch_count']
