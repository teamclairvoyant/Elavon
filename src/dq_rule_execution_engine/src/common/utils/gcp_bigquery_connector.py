import os
import pandas as pd

from functools import reduce
from pandas import DataFrame
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from google.cloud import bigquery
from pyspark.sql import SparkSession, DataFrame as SparkDf, functions
from pyspark.sql.functions import date_format
from pyspark.sql.types import StructType, IntegerType


class GCPBigQueryConnector:

    def __init__(
            self,
            spark: SparkSession,
            credential_path,
            project_id: str
    ):
        if credential_path is not None and os.path.exists(credential_path):
            os.environ['GOGGLE_APPLICATION_CREDENTIALS'] = credential_path
            credentials = service_account.Credentials.from_service_account_file(credential_path)
            self.client = bigquery.Client(project=credentials.project_id)
        else:
            self.client = bigquery.Client(project=project_id)
        self.spark = spark
        # self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    def read_table_using_query(
            self,
            query: str
    ):
        job = self.client.query(query=query)
        data = job.result().to_dataframe()
        resulted_df = self.convert_to_spark_df(data)
        return resulted_df

    def read_table_using_name(
            self,
            table_name: str
    ):
        table_ref = self.client.get_table(table=table_name)
        data = self.client.list_rows(table_ref).to_dataframe()
        resulted_df = self.convert_to_spark_df(data)
        return resulted_df

    def table_exists(
            self,
            table_name: str
    ):
        try:
            self.client.get_table(table_name)  # Make an API request.
            print("Table {} already exists.".format(table_name))
            return True
        except NotFound:
            print("Table {} is not found.".format(table_name))
            return False

    def convert_to_spark_df(self, data: DataFrame):
        pd.DataFrame.iteritems = pd.DataFrame.items
        if not data.dropna().empty:
            spark_df = self.spark.createDataFrame(data)
        else:
            spark_df = self.spark.createDataFrame([], schema=StructType())
        return spark_df

    def save_data(self, data: SparkDf, table_name: str, mode: str):
        data_pd = self.convert_to_pd(data)
        job_config = bigquery.LoadJobConfig(write_disposition=mode)
        job = self.client.load_table_from_dataframe(
            data_pd, table_name, job_config=job_config
        )
        job.result()

    def convert_to_pd(self, df):
        bool_columns = [col_detail[0] for col_detail in df.dtypes if col_detail[1] == 'boolean']
        timestamp_columns = [col_detail[0] for col_detail in df.dtypes if col_detail[1] == 'timestamp']
        fixed_timestamp_df = reduce(
            lambda int_df, col_name: int_df.withColumn(col_name, date_format(col_name, "yyyy-MM-dd HH:mm:ss.SSSSSS")),
            timestamp_columns,
            df
        )
        fixed_bool_df = reduce(
            lambda int_df, col_name: int_df.withColumn(col_name, functions.col(col_name).cast(IntegerType())),
            bool_columns,
            fixed_timestamp_df
        )
        result_df = fixed_bool_df.toPandas()
        for column in bool_columns:
            result_df[column] = result_df[column].astype('bool')

        for column in timestamp_columns:
            result_df[column] = pd.to_datetime(result_df[column], format="%Y-%m-%d %H:%M:%S.%f")

        return result_df
