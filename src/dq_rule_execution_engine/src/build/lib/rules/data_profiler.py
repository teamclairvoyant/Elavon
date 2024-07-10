from pyspark.sql.functions import lit
from pyspark.sql import Window, functions
from pyspark.sql.functions import desc, length, row_number, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

from dq_rule_execution_engine.src.reader import read
from dq_rule_execution_engine.src.utils import get_current_time, get_spark_session, get_unique_id, get_empty_data_frame


class Profiler:
    def __init__(self, context):

        self.source = None
        self.context = context
        self.rule = self.context.get_current_rule()
        self.results = {}
        self.profile_details = get_empty_data_frame(self.profile_details_schema())
        self.profile_summary = get_empty_data_frame(self.profile_summary_schema())
        self.profile_column_details = get_empty_data_frame(self.profile_column_details_schema())
        self.time_created = get_current_time()

    def execute(self):
        source_query = self.context.get_rule_property('SOURCE_QUERY')
        source_entity = self.context.get_source_entity()
        self.results['source_query'] = source_query
        source_query_execution_start_time = get_current_time()
        self.source = read(source_entity, source_query, self.context)
        self.results['source_query_end_time'] = get_current_time()
        self.results['source_query_start_time'] = source_query_execution_start_time

        total_count = self.source.count()

        source_count_df = self.source.groupBy(*[column_name for column_name in self.source.columns]).count()
        unique_records_count_in_source = source_count_df.filter("count == 1").count()

        self.results['source_count'] = total_count
        profile_summary_key = get_unique_id()
        column_count = len(self.source.columns)

        existing_data_types = dict(self.source.dtypes)

        for column_name in self.source.columns:
            profile_details_key = get_unique_id()
            min_value = max_value = value_range = average_value = mode = std_deviation = outlier_per = 'NA'
            null_percentage = (1 - self.calculate_completeness(column_name, total_count))*100
            distinct_count = self.calculate_distinct_count(column_name)
            unique_percentage = round((distinct_count / total_count) * 100, 2)
            existing_data_type = existing_data_types[column_name]

            identified_data_type = existing_data_type
            if existing_data_type == 'string':
                identified_data_types_and_count = self.identify_datatype_from_value(column_name)
                identified_data_type = identified_data_types_and_count['identified_data_type']
                datatype_count = identified_data_types_and_count['datatype_count']
                profile_column_details_rows = []
                for data_type, percent in datatype_count.items():
                    if percent == 0.0:
                        continue
                    else:
                        row = [get_unique_id(), profile_details_key, 'IDENTIFIED_DATA_TYPES', data_type, percent,
                               get_current_time()]
                        profile_column_details_rows.append(row)
                self.profile_column_details = self.profile_column_details.union(
                    get_spark_session().createDataFrame(profile_column_details_rows,
                                                        self.profile_column_details_schema()))

            if identified_data_type in ('bigint', 'int', 'long', 'double'):
                basic_stats_numeric = self.calculate_basic_stats_for_numeric(column_name, identified_data_type)
                min_value = basic_stats_numeric['min']
                max_value = basic_stats_numeric['max']
                value_range = max_value - min_value
                average_value = basic_stats_numeric['mean']
                mode = self.calculate_mode(column_name)
                std_deviation = basic_stats_numeric['stddev']
                outlier_per = self.calculate_outliers(column_name, identified_data_type, total_count)

            average_length, min_length, max_length = self.calculate_length(column_name)

            row = [profile_details_key, profile_summary_key, column_name, null_percentage,
                   unique_percentage, distinct_count,
                   existing_data_type,
                   identified_data_type, min_value, max_value, value_range, average_value, mode, std_deviation,
                   outlier_per, min_length,
                   max_length, average_length, self.time_created
                   ]
            self.profile_details = self.profile_details.union(
                get_spark_session().createDataFrame([row], self.profile_details_schema()))

        profile_summary_row = [profile_summary_key, self.context.get_job_run_id(), self.context.get_rule_id(),
                               source_entity['entity_name'], total_count, column_count,
                               round((unique_records_count_in_source / total_count) * 100,2), get_current_time()]

        self.profile_summary = self.profile_summary.union(
            get_spark_session().createDataFrame([profile_summary_row], self.profile_summary_schema()))

        self.results['profiler_details'] = self.profile_details
        self.results['profiler_summary'] = self.profile_summary

        if self.profile_column_details.count() > 0:
            self.results['profiler_column_details'] = self.profile_column_details

        self.results['is_profiler'] = True

        return self.results

    def calculate_completeness(self, column_name, total_count):
        complete_record_count = self.source.filter(col(column_name).isNotNull()).count()
        return round(complete_record_count/total_count, 2)

    def calculate_distinct_count(self, column_name):
        return self.source.select(col(column_name)).distinct().count()

    def identify_datatype_from_value(self, column_name):
        filtered_source = self.source.filter(col(column_name).isNotNull())

        filtered_source_count = filtered_source.count()

        casted_data = filtered_source\
            .withColumn(f'{column_name}_long', col(column_name).cast('long'))\
            .withColumn(f'{column_name}_double', col(column_name).cast('double'))\
            .withColumn(f'{column_name}_date', to_date(col(column_name), 'yyyy-MM-dd'))\
            .withColumn(f'{column_name}_boolean', col(column_name).cast('boolean'))

        long_not_null_count = casted_data.filter(col(f'{column_name}_long').isNotNull()).count()
        double_not_null_count = casted_data.filter(col(f'{column_name}_double').isNotNull()).count()
        long_double_similar_count = casted_data.filter(col(f'{column_name}_double') == col(f'{column_name}_long')).count()
        date_not_null_count = casted_data.filter(col(f'{column_name}_date').isNotNull()).count()
        boolean_not_null_count = casted_data.filter(col(f'{column_name}_boolean').isNotNull()).count()

        datatype_count = {
            'date': date_not_null_count * 100 / filtered_source_count,
            'boolean': boolean_not_null_count * 100 / filtered_source_count
        }

        identified_data_type = 'string'
        if filtered_source_count == date_not_null_count:
            identified_data_type = 'date'
        elif filtered_source_count == boolean_not_null_count:
            identified_data_type = 'boolean'
        elif filtered_source_count == double_not_null_count & filtered_source_count == long_not_null_count & filtered_source_count == long_double_similar_count:
            identified_data_type = 'long'
        elif filtered_source_count == double_not_null_count & filtered_source_count == long_not_null_count & filtered_source_count != long_double_similar_count:
            identified_data_type = 'double'

        if filtered_source_count == long_double_similar_count:
            datatype_count['long'] = long_not_null_count * 100 / filtered_source_count
        elif double_not_null_count != 0:
            datatype_count['double'] = double_not_null_count * 100 / filtered_source_count

        if date_not_null_count != 0 or boolean_not_null_count != 0 or double_not_null_count != 0 or long_not_null_count != 0:
            datatype_count['string'] = (filtered_source_count - date_not_null_count - boolean_not_null_count - double_not_null_count) * 100 / filtered_source_count

        result = {'identified_data_type': identified_data_type, 'datatype_count': datatype_count}

        return result

    def calculate_basic_stats_for_numeric(self, column_name, identified_data_type):
        filtered_source = self.source.filter(col(column_name).isNotNull()).withColumn(column_name, col(column_name).cast(identified_data_type))
        stats_list = filtered_source.agg(functions.min(column_name),
                                         functions.max(column_name),
                                         functions.mean(column_name),
                                         functions.stddev(column_name)).collect()[0]

        result = {
            "min": stats_list[0],
            "max": stats_list[1],
            'mean': round(stats_list[2], 2),
            "stddev": round(stats_list[3], 2)
        }
        return result

    def calculate_median(self, df, column_name, total_count):
        middle_index = int(total_count / 2)
        window_spec = Window.orderBy(df[column_name])
        df = df.withColumn("row_num", row_number().over(window_spec))
        df = df.filter((df["row_num"] == lit(middle_index)) | (df["row_num"] == lit(middle_index + 1)))
        middle_index_val = df.filter((df["row_num"] == lit(middle_index))).select(column_name).collect()[0][column_name]
        middle_index_plus_1_val = df.filter((df["row_num"] == lit(middle_index + 1))).select(column_name).collect()[0][
            column_name]

        if total_count % 2 == 0:
            med = middle_index_val
        else:
            med = (middle_index_val + middle_index_plus_1_val) / 2

        return med

    def calculate_length(self, column_name):
        df1 = self.source.select(column_name).withColumn('converted' + column_name, col(column_name).cast("string"))
        df1 = df1.withColumn('length', length(df1['converted' + column_name]))
        avg_val = df1.agg({"length": "avg"}).collect()[0]['avg(length)']
        min_val = df1.agg({"length": "min"}).collect()[0]['min(length)']
        max_val = df1.agg({"length": "max"}).collect()[0]['max(length)']

        return round(avg_val, 2), min_val, max_val

    def calculate_outliers(self, column_name, identified_data_type, total_count):
        quartiles = self.source.withColumn(column_name, col(column_name).cast(identified_data_type)).approxQuantile(column_name, [0.25, 0.75], 0.05)
        q1, q3 = quartiles
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers_count = self.source.filter((col(column_name) < lower_bound) | (col(column_name) > upper_bound)).count()
        return round((outliers_count / total_count) * 100, 2)

    def calculate_mode(self, column_name):
        grouped_df = self.source.groupBy(column_name).count()
        sorted_df = grouped_df.orderBy(desc('count'))
        return sorted_df.first()[column_name]

    def profile_details_schema(self):
        schema = StructType([
            StructField("PROFILER_DETAILS_KEY", StringType(), True),
            StructField("PROFILER_SUMMARY_KEY", StringType(), True),
            StructField("ATTRIBUTE_NAME", StringType(), True),
            StructField("NULL_PERCENTAGE", FloatType(), True),
            StructField("UNIQUE_PERCENTAGE", FloatType(), True),
            StructField("DISTINCT_VALUES_COUNT", IntegerType(), True),
            StructField("EXISTING_DATA_TYPE", StringType(), True),
            StructField("IDENTIFIED_DATA_TYPE", StringType(), True),
            StructField("MIN_VALUE", StringType(), True),
            StructField("MAX_VALUE", StringType(), True),
            StructField("RANGE", StringType(), True),
            StructField("AVERAGE_VALUE", StringType(), True),
            StructField("MODE", StringType(), True),
            StructField("STD_DEVIATION", StringType(), True),
            StructField("OUTLIER_PER", StringType(), True),
            StructField("MIN_LENGTH", IntegerType(), True),
            StructField("MAX_LENGTH", IntegerType(), True),
            StructField("AVERAGE_LENGTH", FloatType(), True),
            StructField("TIME_CREATED", TimestampType(), True)
        ])
        return schema

    def profile_summary_schema(self):
        schema = StructType([
            StructField("PROFILER_SUMMARY_KEY", StringType(), True),
            StructField("JOB_ID", StringType(), True),
            StructField("RULE_ID", StringType(), True),
            StructField("SOURCE", StringType(), True),
            StructField("ROW_COUNT", IntegerType(), True),
            StructField("COLUMN_COUNT", IntegerType(), True),
            StructField("UNIQUE_PERCENTAGE", FloatType(), True),
            StructField("TIME_CREATED", TimestampType(), True)
        ])
        return schema

    def profile_column_details_schema(self):
        schema = StructType([
            StructField("PROFILER_COLUMN_DETAILS_KEY", StringType(), True),
            StructField("PROFILER_DETAILS_KEY", StringType(), True),
            StructField("DETAILS_TYPE", StringType(), True),
            StructField("DATA_TYPE", StringType(), True),
            StructField("PERCENT", FloatType(), True),
            StructField("TIME_CREATED", TimestampType(), True)
        ])
        return schema