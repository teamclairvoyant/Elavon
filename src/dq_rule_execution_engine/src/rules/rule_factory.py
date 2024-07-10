from dq_rule_execution_engine.src.rules.cross_reference_value_check import CrossReferenceValueCheck
from dq_rule_execution_engine.src.rules.data_comparator import DataComparator
from dq_rule_execution_engine.src.rules.length_check import LengthCheck
from dq_rule_execution_engine.src.rules.null_check import NullCheck
from dq_rule_execution_engine.src.rules.data_profiler import Profiler
from dq_rule_execution_engine.src.rules.range_check import RangeCheck
from dq_rule_execution_engine.src.rules.record_count_check import RecordCountCheck
from dq_rule_execution_engine.src.rules.reference_values_check import ReferenceValuesCheck
from dq_rule_execution_engine.src.rules.regex_check import RegexCheck
from dq_rule_execution_engine.src.rules.schema_comparator import SchemaComparator
from dq_rule_execution_engine.src.rules.sql_validator import SqlValidator
from dq_rule_execution_engine.src.rules.uniqueness_check import UniquenessCheck
from dq_rule_execution_engine.src.rules.whole_number_check import WholeNumberCheck
from dq_rule_execution_engine.src.rules.column_count_check import ColumnCountCheck


class RuleExecutorFactory:
    def __init__(self, context):
        self.context = context

    def get_rule_executor(self):
        template_name = self.context.get_rule_template_name()
        executor = None
        if template_name == 'DATA_DIFF':
            executor = DataComparator(self.context)
        if template_name == 'SQL_VALIDATOR':
            executor = SqlValidator(self.context)
        if template_name == 'RANGE_CHECK':
            executor = RangeCheck(self.context)
        if template_name == 'NULL_CHECK':
            executor = NullCheck(self.context)
        if template_name == 'LENGTH_CHECK':
            executor = LengthCheck(self.context)
        if template_name == 'REFERENCE_VALUES_CHECK':
            executor = ReferenceValuesCheck(self.context)
        if template_name == 'UNIQUENESS_CHECK':
            executor = UniquenessCheck(self.context)
        if template_name == 'WHOLE_NUMBER_CHECK':
            executor = WholeNumberCheck(self.context)
        if template_name == 'CROSS_REFERENCE_VALUES_CHECK':
            executor = CrossReferenceValueCheck(self.context)
        if template_name == 'REGEX_CHECK':
            executor = RegexCheck(self.context)
        if template_name == 'RECORD_COUNT_CHECK':
            executor = RecordCountCheck(self.context)
        if template_name == 'COLUMN_COUNT_CHECK':
            executor = ColumnCountCheck(self.context)
        if template_name == 'SCHEMA_COMPARATOR':
            executor = SchemaComparator(self.context)
        if template_name == 'DATA_PROFILER':
            executor = Profiler(self.context)

        return executor
