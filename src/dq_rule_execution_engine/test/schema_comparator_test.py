from pathlib import Path

from dq_rule_execution_engine.src.app_context import AppContext
from dq_rule_execution_engine.src.rule_set_executor import RuleSetExecutor


def get_rule_execution_result():
    app_conf_file_path = Path(__file__).parent / "conf/properties/application.properties"
    ruleset_conf_file_path = Path(__file__).parent / "conf/json/local/payments_schema_comparator.json"
    rule_set_path = str(ruleset_conf_file_path)
    app_conf = str(app_conf_file_path)
    run_time_parameters = f'rule_set_path={rule_set_path},app_conf={app_conf},job_id=12345680'
    context = AppContext(run_time_parameters)
    context.build()
    return RuleSetExecutor(context).execute()


class TestSchemaComparator:
    result = get_rule_execution_result()

    def test_result_has_data_diff_rule(self):
        assert 31783855 in self.result, 'result map should have data diff rule id'

    def test_summary(self):
        print(self.result[31783855])
        rule_execution_result = self.result[31783855]
        comparison_summary = rule_execution_result['comparison_summary']

        assert comparison_summary.select('COMPARISON_COUNT').filter(
            comparison_summary.COMPARISON_CATEGORY == "COLUMN_MISSING_IN_SOURCE").first()[
                   'COMPARISON_COUNT'] == 1, \
            'summary will have only 1 column missing in source'

        assert comparison_summary.select('COMPARISON_COUNT').filter(
            comparison_summary.COMPARISON_CATEGORY == "COLUMN_MISSING_IN_TARGET").first()[
                   'COMPARISON_COUNT'] == 1, \
            'summary will have only 1 column missing in target'

        assert comparison_summary.select('COMPARISON_COUNT').filter(
            comparison_summary.COMPARISON_CATEGORY == "COLUMN_MATCHED_DATA_TYPE_MISMATCHED").first()[
                   'COMPARISON_COUNT'] == 1, \
            'summary will have only 1 column column name matched and datatype mismatched'

        assert comparison_summary.select('COMPARISON_COUNT').filter(
            comparison_summary.COMPARISON_CATEGORY == "COLUMN_MATCHED_DATA_TYPE_MATCHED").first()[
                   'COMPARISON_COUNT'] == 29, \
            'summary will have only 1 column column name matched and datatype mismatched'

        assert comparison_summary.select('COMPARISON_COUNT').filter(
            comparison_summary.COMPARISON_CATEGORY == "SOURCE_COLUMN_COUNT").first()[
                   'COMPARISON_COUNT'] == 30, \
            'summary will have only 1 column column name matched and datatype mismatched'

        assert comparison_summary.select('COMPARISON_COUNT').filter(
            comparison_summary.COMPARISON_CATEGORY == "TARGET_COLUMN_COUNT").first()[
                   'COMPARISON_COUNT'] == 30, \
            'summary will have only 1 column column name matched and datatype mismatched'
