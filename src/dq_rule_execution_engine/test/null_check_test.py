from pathlib import Path

from pyspark import Row

from dq_rule_execution_engine.src.app_context import AppContext
from dq_rule_execution_engine.src.rule_set_executor import RuleSetExecutor


def get_rule_execution_result():
    app_conf_file_path = Path(__file__).parent / "conf/properties/application.properties"
    ruleset_conf_file_path = Path(__file__).parent / "conf/json/local/employee_null_check.json"
    rule_set_path = str(ruleset_conf_file_path)
    app_conf = str(app_conf_file_path)
    run_time_parameters = f'rule_set_path={rule_set_path},app_conf={app_conf},job_id=12345678'
    context = AppContext(run_time_parameters)
    context.build()
    return RuleSetExecutor(context).execute()


class TestNullCheck:
    result = get_rule_execution_result()

    def test_result_has_null_check_rule(self):
        assert 31783846 in self.result, 'result map should have null check rule id'

    def test_result_has_correct_total_records_query(self):
        total_records_query = self.result[31783846]['total_records_query']
        expected_total_records_query = 'select count(*) as total_count from employee where emp_id > 100'
        assert total_records_query == expected_total_records_query, 'total records query should form correctly'

    def test_result_has_correct_failed_records_query(self):
        failed_records_query = self.result[31783846]['failed_records_query']
        expected_failed_records_query = "select emp_id from employee where name IS NULL or trim(cast (name as " \
                                        "STRING))= '' and emp_id > 100"
        assert failed_records_query == expected_failed_records_query, 'failed records query should form correctly'

    def test_result_has_correct_failed_records(self):
        failed_records_list = self.result[31783846]['failed_records'].collect()
        assert failed_records_list == [Row(emp_id='105')]
