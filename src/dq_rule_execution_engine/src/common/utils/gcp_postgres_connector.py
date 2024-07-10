import pandas as pd
import sqlalchemy

from pandas import DataFrame
from pyspark.sql import SparkSession, DataFrame as SparkDf
from pyspark.sql.types import StructType


class GCPPostgresConnector:

    def __init__(
            self,
            spark: SparkSession,
            database_credentials: dict
    ):
        self.database_credentials = database_credentials
        self.client = self.get_db_client()
        self.spark = spark

    def create_db_url(self):
        # postgresql+psycopg2://username:password@localhost:5432/mydatabase
        return f"postgresql+psycopg2://{self.database_credentials.get('username')}:" \
               f"{self.database_credentials.get('password')}@{self.database_credentials.get('host')}:" \
               f"{self.database_credentials.get('port')}/{self.database_credentials.get('database')}"

    def get_db_client(self):
        pool = sqlalchemy.create_engine(
            self.create_db_url(),
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800,
        )
        return pool

    def read_table_using_query(
            self,
            query: str
    ):
        with self.client.connect() as conn:
            response = conn.execute(
                sqlalchemy.text(
                    query
                )
            ).fetchall()

            data = DataFrame(response)
            resulted_df = self.convert_to_spark_df(data)

        return resulted_df

    def read_table_using_name(
            self,
            table_name: str
    ):
        with self.client.connect() as conn:
            response = conn.execute(
                sqlalchemy.text(
                    f"select * from {table_name};"
                )
            ).fetchall()

            data = DataFrame(response)
            resulted_df = self.convert_to_spark_df(data)

        return resulted_df

    def table_exists(
            self,
            table_name: str
    ):
        with self.client.connect() as conn:
            response = conn.execute(
                sqlalchemy.text(
                    f"select exists(select * from information_schema.tables where table_name='{table_name}');"
                )).fetchone()[0]
        return response

    def convert_to_spark_df(self, data: DataFrame):
        pd.DataFrame.iteritems = pd.DataFrame.items
        if not data.dropna().empty:
            spark_df = self.spark.createDataFrame(data)
        else:
            spark_df = self.spark.createDataFrame([], schema=StructType())
        return spark_df

    def save_data(self, data: SparkDf, table_name: str, mode: str):
        data.to_sql(table_name, self.client, if_exists=mode)
