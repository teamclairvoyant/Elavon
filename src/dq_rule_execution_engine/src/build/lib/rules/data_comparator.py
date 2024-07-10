import pyspark
from pyspark.sql import Window
from pyspark.sql.functions import array_remove, array, monotonically_increasing_id, lit, concat_ws, when, col, \
    posexplode, row_number

from dq_rule_execution_engine.src.reader import read
from dq_rule_execution_engine.src.utils import get_spark_session, get_empty_data_frame, get_unique_id, get_current_time
from dq_rule_execution_engine.src.constants import *


class DataComparator:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()
        self.job_id = None
        self.rule_id = None
        self.time_created = get_current_time()
        self.source_unique_key = None
        self.source_unique_key_array = None
        self.target_unique_key = None
        self.target_unique_key_array = None
        self.source_entity_name = None
        self.target_entity_name = None
        self.summary = get_empty_data_frame(summary_schema())
        self.details = get_empty_data_frame(details_schema())
        self.source_entity_name = None
        self.target_entity_name = None
        self.source = None
        self.target = None
        self.records_matched = None
        self.matching_from_source = None
        self.matching_from_target = None
        self.unmatched_in_source_all = None
        self.unmatched_in_target_all = None
        self.additional_in_source_all = None
        self.additional_in_target_all = None

        self.results = {}

    def execute(self):
        source_entity = self.context.get_source_entity()
        target_entity = self.context.get_target_entity()
        self.source_unique_key = source_entity['primary_key']
        self.target_unique_key = target_entity['primary_key']
        self.source_unique_key_array = self.source_unique_key.split(',')
        self.target_unique_key_array = self.target_unique_key.split(',')

        self.source_entity_name = source_entity['entity_name']
        self.target_entity_name = target_entity['entity_name']

        self.job_id = self.context.get_job_run_id()
        self.rule_id = self.context.get_rule_id()
        source_query = self.context.get_rule_property('SOURCE_QUERY')
        target_query = self.context.get_rule_property('TARGET_QUERY')

        self.results['source_query'] = source_query
        self.results['target_query'] = target_query
        source_query_execution_start_time = get_current_time()
        self.source = read(source_entity, source_query, self.context)
        self.results['source_query_end_time'] = get_current_time()
        self.results['source_query_start_time'] = source_query_execution_start_time

        target_query_execution_start_time = get_current_time()
        self.target = read(target_entity, target_query, self.context)
        self.results['target_query_end_time'] = get_current_time()
        self.results['target_query_start_time'] = target_query_execution_start_time

        self.compute_diff()
        self.results['comparison_summary'] = self.summary
        self.results['comparison_details'] = self.details
        self.results['is_data_diff'] = True

        threshold_percent = float(
            self.context.get_rule_property('THRESHOLD_PERCT')) if self.context.is_key_exist_in_rule_property(
            'THRESHOLD_PERCT') else 100.00
        total_record_count = self.results['source_count']
        pass_record_count = self.results['records_match_count']
        is_rule_passed = pass_record_count * 100 / total_record_count >= threshold_percent
        self.results['is_rule_passed'] = is_rule_passed

        exception_summary = {
            "source_count": self.results['source_count'],
            "target_count": self.results['target_count'],
            "records_match_count": self.results['records_match_count'],
            "records_mis_match_count": self.results['records_mismatch_count']
        }
        self.results['exception_summary'] = exception_summary
        return self.results

    def compute_diff(self):
        self.compare_counts()
        self.compare_matching()
        self.compare_matched_extra()
        self.compare_additional()
        self.compare_mismatched()
        self.compare_duplicates()

    def compare_counts(self):

        source_count = self.source.count()
        source_total_record_list = [get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                                    self.target_entity_name
            , self.source_unique_key, TOTAL_RECORD_SOURCE, source_count,
                                    SOURCE_TO_SOURCE, BLANK, self.time_created]
        self.results['source_count'] = source_count
        self.summary = self.summary.union(
            get_spark_session().createDataFrame([source_total_record_list], summary_schema()))

        target_count = self.target.count()
        target_total_record_list = [get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                                    self.target_entity_name,
                                    self.source_unique_key, TOTAL_RECORD_TARGET, target_count,
                                    TARGET_TO_TARGET, BLANK, self.time_created]
        self.results['target_count'] = target_count
        self.summary = self.summary.union(
            get_spark_session().createDataFrame([target_total_record_list], summary_schema()))

    def compare_matching(self):
        self.records_matched = self.source.intersectAll(self.target)
        self.build_summary(self.records_matched, self.get_sample_record(self.records_matched), RECORDS_MATCH,
                           get_unique_id())
        self.results['records_match_count'] = self.records_matched.count()

    def compare_matched_extra(self):

        get_spark_session().conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")

        records_matched_unique = self.records_matched.dropDuplicates()

        # calculate matching from source
        self.matching_from_source = self.source. \
            join(records_matched_unique, [self.source[column_name] == records_matched_unique[column_name]
                                          for column_name in self.source.columns]) \
            .select(*[self.source[column_name] for column_name in self.source.columns])

        matching_from_source_with_count = self.matching_from_source.groupBy(*self.source_unique_key_array).count()

        matching_from_source_with_count_renamed = matching_from_source_with_count. \
            withColumnRenamed("count", SOURCE_COUNT)

        # calculate matching from target
        self.matching_from_target = self.target. \
            join(records_matched_unique, [self.target[column_name] == records_matched_unique[column_name]
                                          for column_name in self.target.columns]) \
            .select(*[self.target[column_name] for column_name in self.target.columns])

        matching_from_target_with_count = self.matching_from_target.groupBy(*self.source_unique_key_array).count()

        matching_from_target_with_count_renamed = matching_from_target_with_count. \
            withColumnRenamed("count", TARGET_COUNT)

        # calculate matching extra count from source and target
        join_columns = [matching_from_source_with_count_renamed[column_name] for column_name in
                        matching_from_target_with_count_renamed.columns if
                        column_name in self.source_unique_key_array]

        counts_joined = matching_from_source_with_count_renamed.join(
            matching_from_target_with_count_renamed,
            [matching_from_source_with_count_renamed[column_name] ==
             matching_from_target_with_count_renamed[column_name] for
             column_name in self.source_unique_key_array]).select(*join_columns,
                                                                  SOURCE_COUNT,
                                                                  TARGET_COUNT)

        matched_extra_in_source = counts_joined.filter("SOURCE_COUNT > TARGET_COUNT")
        matched_extra_in_target = counts_joined.filter("TARGET_COUNT > SOURCE_COUNT")

        total_extra_in_source = 0
        sample = BLANK
        if matched_extra_in_source.count() > 0:
            matched_extra_in_source_with_extra_count = \
                matched_extra_in_source.withColumn("extra_in_source", matched_extra_in_source['SOURCE_COUNT'] - \
                                                   matched_extra_in_source['TARGET_COUNT'])

            total_extra_in_source = \
                matched_extra_in_source_with_extra_count.agg({'extra_in_source': 'sum'}).collect()[0][
                    "sum(extra_in_source)"]

            sample = COMMA.join(self.get_sample_record(matched_extra_in_source))

        # build extra in source summary row
        comparison_summary_key = get_unique_id()
        row_value = [comparison_summary_key, self.job_id, self.rule_id, self.source_entity_name,
                     self.target_entity_name,
                     self.source_unique_key,
                     MATCHED_EXTRA_IN_SOURCE,
                     total_extra_in_source, SOURCE_TO_TARGET, sample, self.time_created]
        self.summary = self.summary.union(get_spark_session().createDataFrame([row_value], summary_schema()))

        # build extra in source details row
        self.union_details(self.build_details(matched_extra_in_source, comparison_summary_key, False))

        sample = BLANK
        total_extra_in_target = 0

        if matched_extra_in_target.count() > 0:
            matched_extra_in_target_with_extra_count = \
                matched_extra_in_target.withColumn("extra_in_target", matched_extra_in_source['TARGET_COUNT'] - \
                                                   matched_extra_in_source['SOURCE_COUNT'])

            total_extra_in_target = \
                matched_extra_in_target_with_extra_count.agg({'extra_in_target': 'sum'}).collect()[0][
                    "sum(extra_in_target)"]

            sample = COMMA.join(self.get_sample_record(matched_extra_in_target))

        # extra in target summary
        comparison_summary_key = get_unique_id()
        row_value = [comparison_summary_key, self.job_id, self.rule_id, self.source_entity_name,
                     self.target_entity_name,
                     self.source_unique_key,
                     MATCHED_EXTRA_IN_TARGET,
                     total_extra_in_target, SOURCE_TO_TARGET, sample, self.time_created]
        self.summary = self.summary.union(get_spark_session().createDataFrame([row_value], summary_schema()))

        # extra in target details
        self.union_details(self.build_details(matched_extra_in_target, comparison_summary_key, False))

    def compare_mismatched(self):
        mismatched_in_source_all = self.unmatched_in_source_all.exceptAll(self.additional_in_source_all)
        mismatched_in_target_all = self.unmatched_in_target_all.exceptAll(self.additional_in_target_all)

        new_columns = [col + "_1" for col in mismatched_in_target_all.columns]
        mismatched_in_target_all_renamed = mismatched_in_target_all

        for old_col, new_col in zip(self.unmatched_in_target_all.columns, new_columns):
            mismatched_in_target_all_renamed = mismatched_in_target_all_renamed.withColumnRenamed(old_col, new_col)

        join_columns = [mismatched_in_source_all[column_name]
                        for column_name in self.source_unique_key_array]

        mismatched_in_source_all_columns = [mismatched_in_source_all[column_name]
                                            for column_name in mismatched_in_source_all.columns]

        mismatched_in_target_all_renamed_columns = [mismatched_in_target_all_renamed[column_name]
                                                    for column_name in mismatched_in_target_all_renamed.columns]

        join_conditions = [mismatched_in_source_all[column_name] == mismatched_in_target_all_renamed[column_name + "_1"]
                           for column_name in self.source_unique_key_array]

        get_column_name = [
            when(mismatched_in_source_all[column_name] != mismatched_in_target_all_renamed[column_name + "_1"],
                 column_name).otherwise(BLANK)
            for column_name in mismatched_in_source_all.columns
            if column_name not in self.source_unique_key_array]

        get_source_value = [
            when(mismatched_in_source_all[column_name] != mismatched_in_target_all_renamed[column_name + "_1"],
                 mismatched_in_source_all[column_name]).otherwise(BLANK)
            for column_name in mismatched_in_source_all.columns
            if column_name not in self.source_unique_key_array]

        get_target_value = [
            when(mismatched_in_source_all[column_name] != mismatched_in_target_all_renamed[column_name + "_1"],
                 mismatched_in_target_all_renamed[column_name + "_1"]).otherwise(BLANK)
            for column_name in mismatched_in_source_all.columns
            if column_name not in self.source_unique_key_array]

        select_expr = [*mismatched_in_source_all_columns,
                       array_remove(array(*get_column_name), BLANK).alias("column_names"),
                       array_remove(array(*get_source_value), BLANK).alias("source_value"),
                       array_remove(array(*get_target_value), BLANK).alias("target_value")]

        mismatched_joined = mismatched_in_source_all.join(mismatched_in_target_all_renamed, join_conditions) \
            .select(*select_expr)

        mismatched_joined_with_row_num = mismatched_joined.withColumn("row_number", row_number().over(
            Window.partitionBy(self.source_unique_key_array)
            .orderBy(self.source_unique_key_array)))

        join_columns_with_index = self.source_unique_key_array.copy()
        join_columns_with_index.append('index')
        join_columns_with_index.append('row_number')
        join_columns_with_row_number = join_columns.copy()
        join_columns_with_row_number.append('row_number')

        column_names_exploded = mismatched_joined_with_row_num.select(*join_columns_with_row_number,
                                                                      posexplode("column_names").alias("index",
                                                                                                       "column_names_exploded"))

        source_value_exploded = mismatched_joined_with_row_num.select(*join_columns_with_row_number,
                                                                      posexplode("source_value").alias("index",
                                                                                                       "source_value_exploded")).dropDuplicates()

        target_value_exploded = mismatched_joined_with_row_num.select(*join_columns_with_row_number,
                                                                      posexplode("target_value").alias("index",
                                                                                                       "target_value_exploded")).dropDuplicates()

        join_columns_for_exploded = [column_names_exploded[c_name] for c_name in join_columns_with_index]
        join_condition_for_exploded = [column_names_exploded[c_name] == source_value_exploded[c_name] for c_name in
                                       join_columns_with_index]

        column_name_source_value_joined = column_names_exploded.join(source_value_exploded,
                                                                     join_condition_for_exploded).select(
            *join_columns_for_exploded, "column_names_exploded", "source_value_exploded")
        join_condition_for_exploded = [column_name_source_value_joined[c_name] == target_value_exploded[c_name] for
                                       c_name in join_columns_with_index]

        records_mis_match_details = column_name_source_value_joined.join(target_value_exploded,
                                                                         join_condition_for_exploded) \
            .select(*join_columns_for_exploded, "column_names_exploded", "source_value_exploded",
                    "target_value_exploded")

        records_mis_match_details = records_mis_match_details.withColumnRenamed("column_names_exploded", CONTEXT). \
            withColumnRenamed("source_value_exploded", SOURCE_VALUE) \
            .withColumnRenamed("target_value_exploded", TARGET_VALUE)

        comparison_summary_key = get_unique_id()
        self.build_summary(mismatched_joined, self.get_sample_record(mismatched_joined), RECORDS_MISMATCH,
                           comparison_summary_key)

        self.build_summary(mismatched_in_source_all, self.get_sample_record(mismatched_in_source_all.dropDuplicates()),
                           "RECORDS_MISMATCH_FROM_SOURCE", get_unique_id())

        self.build_summary(mismatched_in_target_all, self.get_sample_record(mismatched_in_target_all.dropDuplicates()),
                           "RECORDS_MISMATCH_FROM_TARGET", get_unique_id())
        self.results['records_mismatch_count'] = mismatched_joined.count()

        record_mismatch_details = self.build_details_for_mismatched(records_mis_match_details, comparison_summary_key)
        self.union_details(record_mismatch_details)

    def compare_additional(self):

        self.unmatched_in_source_all = self.source.exceptAll(self.matching_from_source)
        self.unmatched_in_target_all = self.target.exceptAll(self.matching_from_target)

        unmatched_in_source = self.unmatched_in_source_all.select(self.source_unique_key_array).dropDuplicates()
        unmatched_in_target = self.unmatched_in_target_all.select(self.source_unique_key_array).dropDuplicates()

        join_conditions = [unmatched_in_source[column_name] == unmatched_in_target[column_name]
                           for column_name in self.source_unique_key_array]

        # Compute Additional in Source
        additional_in_source = unmatched_in_source.join(unmatched_in_target, join_conditions, "leftanti").select(
            *[unmatched_in_source[column_name] for column_name in self.source_unique_key_array])

        self.additional_in_source_all = self.unmatched_in_source_all \
            .join(additional_in_source,
                  [self.unmatched_in_source_all[column_name] == additional_in_source[column_name] for column_name in
                   additional_in_source.columns]) \
            .select(
            *[self.unmatched_in_source_all[column_name] for column_name in self.unmatched_in_source_all.columns])

        # Building summary dataset for additional in source
        additional_in_source_sample = additional_in_source.withColumn('almost_desired_output',
                                                                      concat_ws('|', *self.source_unique_key_array)) \
            .dropDuplicates().take(SAMPLE_SIZE)

        additional_in_source_sample_array = []
        for row in additional_in_source_sample:
            additional_in_source_sample_array.append(str(row[len(self.source_unique_key_array)]))

        comparison_summary_key = get_unique_id()

        self.build_summary(self.additional_in_source_all.select(
            [self.additional_in_source_all[column_name] for column_name in self.source_unique_key_array]),
            additional_in_source_sample_array, ADDITIONAL_IN_SOURCE, comparison_summary_key)

        # Building details dataset for additional in source
        additional_in_source_with_count = self.additional_in_source_all.groupBy(*self.source_unique_key_array).count()

        additional_in_source_with_count_renamed = additional_in_source_with_count. \
            withColumnRenamed("count", SOURCE_COUNT).withColumn(TARGET_COUNT, lit(0))

        self.union_details(
            self.build_details(additional_in_source_with_count_renamed, comparison_summary_key, False, True))

        # Compute Additional in Target

        additional_in_target = unmatched_in_target.join(unmatched_in_source,
                                                        join_conditions,
                                                        "leftanti").select(
            [unmatched_in_target[column_name] for column_name in self.source_unique_key_array])

        self.additional_in_target_all = self.unmatched_in_target_all \
            .join(additional_in_target,
                  [self.unmatched_in_target_all[column_name] == additional_in_target[column_name] for column_name in
                   additional_in_target.columns]) \
            .select(
            *[self.unmatched_in_target_all[column_name] for column_name in self.unmatched_in_target_all.columns])

        # Building summary dataset for additional in target

        additional_in_target_sample = additional_in_target.withColumn('almost_desired_output',
                                                                      concat_ws('|', *self.source_unique_key_array)) \
            .dropDuplicates().take(SAMPLE_SIZE)

        additional_in_target_sample_array = []
        for row in additional_in_target_sample:
            additional_in_target_sample_array.append(str(row[len(self.source_unique_key_array)]))

        comparison_summary_key = get_unique_id()

        self.build_summary(self.additional_in_target_all.select(
            [self.additional_in_target_all[column_name] for column_name in self.source_unique_key_array]),
            additional_in_target_sample_array, ADDITIONAL_IN_TARGET, comparison_summary_key)

        # Building details dataset for additional in source
        additional_in_target_with_count = self.additional_in_target_all.groupBy(*self.source_unique_key_array).count()

        additional_in_target_with_count_renamed = additional_in_target_with_count. \
            withColumnRenamed("count", TARGET_COUNT).withColumn(SOURCE_COUNT, lit(0))

        self.union_details(
            self.build_details(additional_in_target_with_count_renamed, comparison_summary_key, False, True))

    def compare_duplicates(self):

        source_count_df = self.source.groupBy(*self.source_unique_key_array).count()
        target_count_df = self.target.groupBy(*self.target_unique_key_array).count()
        # Duplicate in Source Summary
        comparison_summary_key = get_unique_id()
        duplicate_in_source = source_count_df.filter("count > 1")
        self.build_summary(duplicate_in_source, self.get_sample_record(duplicate_in_source),
                           DUPLICATE_UNIQUE_ROW_KEY_IN_SOURCE,
                           comparison_summary_key, SOURCE_TO_SOURCE)

        # Duplicate in Source details
        duplicate_in_source_details = self.build_details(duplicate_in_source, comparison_summary_key) \
            .drop(col(SOURCE_COUNT)) \
            .withColumnRenamed(COUNT, SOURCE_COUNT)

        self.union_details(duplicate_in_source_details)

        # Duplicate in Target Summary
        comparison_summary_key = get_unique_id()
        duplicate_in_target = target_count_df.filter("count > 1")

        self.build_summary(duplicate_in_target, self.get_sample_record(duplicate_in_target),
                           DUPLICATE_UNIQUE_ROW_KEY_IN_TARGET,
                           comparison_summary_key, TARGET_TO_TARGET)

        # Duplicate in Target Details
        duplicate_in_target_details = self.build_details(duplicate_in_target, comparison_summary_key) \
            .drop(col(TARGET_COUNT)) \
            .withColumnRenamed(COUNT, TARGET_COUNT)

        self.union_details(duplicate_in_target_details)

    def build_details_for_mismatched(self, records, comparison_summary_key):
        records = records \
            .withColumn(COMPARISON_DETAILS_KEY, monotonically_increasing_id() + (int(comparison_summary_key) + 1)) \
            .withColumn(COMPARISON_SUMMARY_KEY, lit(comparison_summary_key)) \
            .withColumn(UNIQUE_ROW_KEY, pyspark.sql.functions.concat_ws(',', *self.source_unique_key_array)) \
            .withColumn(TIME_CREATED, lit(self.time_created)) \
            .withColumn(SOURCE_COUNT, lit(ZERO).cast(IntegerType())) \
            .withColumn(TARGET_COUNT, lit(ZERO).cast(IntegerType()))

        return records

    def build_details(self, records, comparison_summary_key, add_count_column=True, add_source_target_value=True):
        records = records \
            .withColumn(COMPARISON_DETAILS_KEY, monotonically_increasing_id() + (int(comparison_summary_key) + 1)) \
            .withColumn(COMPARISON_SUMMARY_KEY, lit(comparison_summary_key)) \
            .withColumn(UNIQUE_ROW_KEY, pyspark.sql.functions.concat_ws(',', *self.source_unique_key_array)) \
            .withColumn(CONTEXT, lit(BLANK).cast(StringType())) \
            .withColumn(TIME_CREATED, lit(self.time_created))

        if add_count_column:
            records = records.withColumn(SOURCE_COUNT, lit(ZERO).cast(IntegerType())) \
                .withColumn(TARGET_COUNT, lit(ZERO).cast(IntegerType()))

        if add_source_target_value:
            records = records.withColumn(SOURCE_VALUE, lit(BLANK).cast(StringType())) \
                .withColumn(TARGET_VALUE, lit(BLANK).cast(StringType()))

        return records

    def get_sample_record(self, category_records):
        category_records_sample = category_records.select(
            pyspark.sql.functions.concat_ws('|', *self.source_unique_key_array)) \
            .distinct().take(SAMPLE_SIZE)

        category_records_sample_array = []
        for row in category_records_sample:
            category_records_sample_array.append(str(row[ZERO]))

        return category_records_sample_array

    def build_summary(self, category_records, category_records_sample_array, category_name, comparison_summary_key,
                      comparison_direction=SOURCE_TO_TARGET):
        sample = BLANK
        if len(category_records_sample_array) > 0:
            sample = COMMA.join(category_records_sample_array)

        row_value = [comparison_summary_key, self.job_id, self.rule_id, self.source_entity_name,
                     self.target_entity_name,
                     self.source_unique_key,
                     category_name,
                     category_records.count(), comparison_direction, sample, self.time_created]
        self.summary = self.summary.union(get_spark_session().createDataFrame([row_value], summary_schema()))

    def union_details(self, category_records):
        self.details = self.details.union(category_records.select(COMPARISON_DETAILS_KEY, COMPARISON_SUMMARY_KEY,
                                                                  UNIQUE_ROW_KEY, CONTEXT, SOURCE_COUNT,
                                                                  TARGET_COUNT, SOURCE_VALUE, TARGET_VALUE,
                                                                  TIME_CREATED))
