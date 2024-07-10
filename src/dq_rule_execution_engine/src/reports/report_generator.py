
import argparse
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, functions, DataFrame

from dq_rule_execution_engine.src.common.utils.custom_error_bucket import IncorrectConfigError
from dq_rule_execution_engine.src.common.utils.gcp_bigquery_connector import GCPBigQueryConnector
from dq_rule_execution_engine.src.common.utils.gcp_postgres_connector import GCPPostgresConnector
from dq_rule_execution_engine.src.common.utils.tables_info_object_handler import TableInfoObjectHandler
from dq_rule_execution_engine.src.utils import read_file, get_current_time


class ReportGenerator(ABC):

    parser = argparse.ArgumentParser(
        prog='ReportGenerator',
        description='To generate an aggregated report based on the stats created while the process was running.'
    )

    parser.add_argument(
        "report_name",
        help="Name of the report for which we needed to run the job"
        )

    parser.add_argument(
        "--prop_file_path",
        required=True,
        help="Path to the application property file containing source and destination config",
        )

    sections = ['reference', 'stats', 'report']
    host_key = 'host'
    source_key = 'source'
    host_config_keys = {
        'local': [],
        'gcp': []
    }
    source_config_keys = {
        'big_query': ['project_id'],
        'postgres': ['database_connection_details']
    }

    def __init__(
            self,
            spark: SparkSession,
            job_name: str
    ):
        self.spark = spark
        self.checkpoint = TableInfoObjectHandler.aggregated_report_job_checkpoint_obj
        self.run_time = get_current_time()
        self.job_name = job_name

    @classmethod
    def get_reporting_job(
            cls,
            spark_session: SparkSession,
            report_name: str,
            config: dict
    ):

        if report_name == "dq_aggregated_reporting":
            from dq_rule_execution_engine.src.reports.dq_aggregated_reporting_job import (
                DqAggregatedReportingJob,
            )

            return DqAggregatedReportingJob(
                spark=spark_session,
                config=config
            )

        elif report_name == 'dq_aggregated_reporting_failed':
            from dq_rule_execution_engine.src.reports.dq_aggregated_reporting_failed_job import (
                DqAggregatedReportingFailedJob,
            )

            return DqAggregatedReportingFailedJob(
                spark=spark_session,
                config=config
            )

        elif report_name == "dq_aggregated_blend_data":
            from dq_rule_execution_engine.src.reports.dq_aggregated_blend_data import (
                DqAggregatedBlendData,
            )

            return DqAggregatedBlendData(
                spark=spark_session,
                config=config
            )

        else:
            raise NotImplementedError(
                f"The report name provided is incorrect",
                report_name,
            )

    @classmethod
    def get_config(
            cls,
            prop_file_path: str
    ):
        config = read_file(prop_file_path)
        cls.verify_config(config)
        return config

    @classmethod
    def verify_config(
            cls,
            config: dict
    ):

        for section in cls.sections:
            host_key_name = section + '.' + cls.host_key
            source_key_name = section + '.' + cls.source_key
            if source_key_name in config and host_key_name in config:
                source = config.get(source_key_name)
                key = config.get(host_key_name)

                for source_config_key in cls.source_config_keys.get(source):
                    source_config_key_name = section + '.' + source_config_key
                    if source_config_key_name not in config:
                        raise IncorrectConfigError(
                            f"{source_config_key_name} configuration not found"
                        )

                for host_config_key in cls.host_config_keys.get(key):
                    host_config_key_name = section + '.' + host_config_key
                    if host_config_key_name not in config:
                        raise IncorrectConfigError(
                            f"{host_config_key_name} configuration not found"
                        )

            else:
                raise IncorrectConfigError(
                    f"{host_key_name} or {source_key_name} configuration not found"
                )

    @abstractmethod
    def run(self): pass

    def get_incremental_table(
            self,
            client,
            table_name: str,
            checkpoint_col_name: str
    ):
        checkpoint_exist, last_checkpoint = self.check_and_get_checkpoint(client,
                                                                          table_name)
        if not checkpoint_exist:
            table = self.read_whole_table(client=client,
                                          table_name=table_name)

        else:
            table = self.read_table_using_checkpoint(client=client,
                                                     table_name=table_name,
                                                     checkpoint_col_name=checkpoint_col_name,
                                                     checkpoint_val=last_checkpoint)
        if table.isEmpty():
            future_checkpoint = last_checkpoint
        else:
            future_checkpoint = self.get_future_checkpoint_value(table,
                                                                 checkpoint_col_name)
        future_checkpoint_row = (self.job_name, table_name, future_checkpoint, self.run_time)
        return table, future_checkpoint_row

    def read_table_using_checkpoint(
            self,
            client,
            table_name: str,
            checkpoint_col_name: str,
            checkpoint_val: str
    ):
        query = f"select * from {table_name} " \
                f"where {checkpoint_col_name} > '{checkpoint_val}'"
        result = client.read_table_using_query(query=query)
        return result

    def get_future_checkpoint_value(
            self,
            table: DataFrame,
            checkpoint_col: str
    ):
        max_checkpoint_df = table.agg(functions.max(checkpoint_col).alias('latest_run'))
        max_checkpoint_row = max_checkpoint_df.first()
        max_checkpoint_value = max_checkpoint_row[0]
        return max_checkpoint_value

    def add_checkpoint_in_db(
            self,
            client,
            new_checkpoint_row
    ):
        checkpoint_table_name = self.checkpoint.get_qualified_table_name()
        checkpoint_row_df = self.spark.createDataFrame(new_checkpoint_row, self.checkpoint.get_schema())
        client.save_data(checkpoint_row_df, checkpoint_table_name, 'WRITE_APPEND')

    def read_whole_table(
            self,
            client,
            table_name: str
    ):
        result = client.read_table_using_name(table_name=table_name)
        return result

    def check_and_get_checkpoint(
            self,
            client,
            table_name: str
    ):
        checkpoint_table_name = self.checkpoint.get_qualified_table_name()
        if client.table_exists(checkpoint_table_name):
            query = f"select max({self.checkpoint.LAST_RUN_AT_COL_NAME}) as last_run from {checkpoint_table_name} " \
                    f"where {self.checkpoint.JOB_NAME_COL_NAME} = '{self.job_name}' " \
                    f"and {self.checkpoint.TABLE_NAME_COL_NAME} = '{table_name}'"
            result = client.read_table_using_query(query=query)

            if result.isEmpty():
                return False, None
            else:
                return True, result.first()[0]
        else:
            return False, None

    def get_db_client(
            self,
            section: str,
            config: dict
    ):

        host_key = '.'.join([section, 'host'])
        source_key = '.'.join([section, 'source'])

        if config.get(host_key) == 'gcp' and config.get(source_key) == 'big_query':
            project_id_key = '.'.join([section, 'project_id'])
            credential_path_key = '.'.join([section, 'credential_path'])
            client = GCPBigQueryConnector(spark=self.spark,
                                          credential_path=config.get(credential_path_key),
                                          project_id=config.get(project_id_key))
        elif config.get(host_key) == 'gcp' and config.get(source_key) == 'postgres':
            database_credentials_path_key = '.'.join([section, 'database_connection_details'])
            client = GCPPostgresConnector(spark=self.spark,
                                          database_credentials=read_file(config.get(database_credentials_path_key)))
        else:
            client = None

        return client
