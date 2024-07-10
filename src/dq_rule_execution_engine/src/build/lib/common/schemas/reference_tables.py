from pyspark.sql.types import StringType, TimestampType, IntegerType, BooleanType, StructType, StructField

from dq_rule_execution_engine.src.common.schemas.table_definition import TableDefinition

DATASET_NAME = None


class RulesTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='rule'
        )
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.RULESET_ID_COL_NAME = 'ruleset_id'
        self.RULESET_ID_DATA_TYPE = IntegerType()
        self.RULE_TEMPLATE_ID_COL_NAME = 'rule_template_id'
        self.RULE_TEMPLATE_ID_DATA_TYPE = IntegerType()
        self.RULE_NAME_COL_NAME = 'rule_name'
        self.RULE_NAME_DATA_TYPE = StringType()
        self.RULE_DESC_COL_NAME = 'rule_desc'
        self.RULE_DESC_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.RULESET_ID_COL_NAME, self.RULESET_ID_DATA_TYPE, True),
            StructField(self.RULE_TEMPLATE_ID_COL_NAME, self.RULE_TEMPLATE_ID_DATA_TYPE, True),
            StructField(self.RULE_NAME_COL_NAME, self.RULE_NAME_DATA_TYPE, True),
            StructField(self.RULE_DESC_COL_NAME, self.RULE_DESC_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class RuleEntityMapTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='rule_entity_map'
        )
        self.RULE_ENTITY_MAP_ID_COL_NAME = 'rule_entity_map_id'
        self.RULE_ENTITY_MAP_ID_DATA_TYPE = IntegerType()
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.ENTITY_ID_COL_NAME = 'entity_id'
        self.ENTITY_ID_DATA_TYPE = IntegerType()
        self.ENTITY_BEHAVIOUR_COL_NAME = 'entity_behaviour'
        self.ENTITY_BEHAVIOUR_DATA_TYPE = StringType()
        self.IS_PRIMARY_COL_NAME = 'is_primary'
        self.IS_PRIMARY_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.RULE_ENTITY_MAP_ID_COL_NAME, self.RULE_ENTITY_MAP_ID_DATA_TYPE, True),
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.ENTITY_ID_COL_NAME, self.ENTITY_ID_DATA_TYPE, True),
            StructField(self.ENTITY_BEHAVIOUR_COL_NAME, self.ENTITY_BEHAVIOUR_DATA_TYPE, True),
            StructField(self.IS_PRIMARY_COL_NAME, self.IS_PRIMARY_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class RulePropertiesTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='rule_properties'
        )
        self.RULE_PROP_ID_COL_NAME = 'rule_prop_id'
        self.RULE_PROP_ID_DATA_TYPE = IntegerType()
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.RULE_PROP_KEY_COL_NAME = 'rule_prop_key'
        self.RULE_PROP_KEY_DATA_TYPE = StringType()
        self.RULE_PROP_VALUE_COL_NAME = 'rule_prop_value'
        self.RULE_PROP_VALUE_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.RULE_PROP_ID_COL_NAME, self.RULE_PROP_ID_DATA_TYPE, True),
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.RULE_PROP_KEY_COL_NAME, self.RULE_PROP_KEY_DATA_TYPE, True),
            StructField(self.RULE_PROP_VALUE_COL_NAME, self.RULE_PROP_VALUE_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class RuleSetTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='ruleset'
        )
        self.RULESET_NAME_COL_NAME = 'ruleset_name'
        self.RULESET_NAME_DATA_TYPE = StringType()
        self.RULESET_DESC_COL_NAME = 'ruleset_desc'
        self.RULESET_DESC_DATA_TYPE = StringType()
        self.NOTIFICATION_EMAIL_COL_NAME = 'notification_email'
        self.NOTIFICATION_EMAIL_DATA_TYPE = StringType()
        self.RULESET_ID_COL_NAME = 'ruleset_id'
        self.RULESET_ID_DATA_TYPE = IntegerType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.RULESET_NAME_COL_NAME, self.RULESET_NAME_DATA_TYPE, True),
            StructField(self.RULESET_DESC_COL_NAME, self.RULESET_DESC_DATA_TYPE, True),
            StructField(self.NOTIFICATION_EMAIL_COL_NAME, self.NOTIFICATION_EMAIL_DATA_TYPE, True),
            StructField(self.RULESET_ID_COL_NAME, self.RULESET_ID_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class RuleTemplateTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='rule_template'
        )
        self.RULE_TEMPLATE_ID_COL_NAME = 'rule_template_id'
        self.RULE_TEMPLATE_ID_DATA_TYPE = IntegerType()
        self.DQ_METRIC_COL_NAME = 'dq_metric'
        self.DQ_METRIC_DATA_TYPE = StringType()
        self.RULE_TEMPLATE_NAME_COL_NAME = 'rule_template_name'
        self.RULE_TEMPLATE_NAME_DATA_TYPE = StringType()
        self.RULE_TEMPLATE_DESC_COL_NAME = 'rule_template_desc'
        self.RULE_TEMPLATE_DESC_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = StringType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = StringType()

    def get_schema(self):
        return StructType([
            StructField(self.RULE_TEMPLATE_ID_COL_NAME, self.RULE_TEMPLATE_ID_DATA_TYPE, True),
            StructField(self.DQ_METRIC_COL_NAME, self.DQ_METRIC_DATA_TYPE, True),
            StructField(self.RULE_TEMPLATE_NAME_COL_NAME, self.RULE_TEMPLATE_NAME_DATA_TYPE, True),
            StructField(self.RULE_TEMPLATE_DESC_COL_NAME, self.RULE_TEMPLATE_DESC_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class EntityTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='entity'
        )
        self.ENTITY_ID_COL_NAME = 'entity_id'
        self.ENTITY_ID_DATA_TYPE = IntegerType()
        self.ENTITY_TEMPLATE_ID_COL_NAME = 'entity_template_id'
        self.ENTITY_TEMPLATE_ID_DATA_TYPE = IntegerType()
        self.ENTITY_NAME_COL_NAME = 'entity_name'
        self.ENTITY_NAME_DATA_TYPE = StringType()
        self.ENTITY_PHYSICAL_NAME_COL_NAME = 'entity_physical_name'
        self.ENTITY_PHYSICAL_NAME_DATA_TYPE = StringType()
        self.ENTITY_PRIMARY_KEY_COL_NAME = 'entity_primary_key'
        self.ENTITY_PRIMARY_KEY_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.ENTITY_ID_COL_NAME, self.ENTITY_ID_DATA_TYPE, True),
            StructField(self.ENTITY_TEMPLATE_ID_COL_NAME, self.ENTITY_TEMPLATE_ID_DATA_TYPE, True),
            StructField(self.ENTITY_NAME_COL_NAME, self.ENTITY_NAME_DATA_TYPE, True),
            StructField(self.ENTITY_PHYSICAL_NAME_COL_NAME, self.ENTITY_PHYSICAL_NAME_DATA_TYPE, True),
            StructField(self.ENTITY_PRIMARY_KEY_COL_NAME, self.ENTITY_PRIMARY_KEY_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])
