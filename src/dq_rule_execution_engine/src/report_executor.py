import os

from pyspark.sql import SparkSession

from dq_rule_execution_engine.src.reports.report_generator import ReportGenerator

os.environ['TZ'] = 'UTC'

if __name__ == "__main__":

    namespace = ReportGenerator.parser.parse_args()     # Parsing the runtime arguments

    prop_file_path = namespace.prop_file_path           # Getting the prop_file_path argument value
    report_name = namespace.report_name                 # Getting the report_name argument value

    spark_session = SparkSession.builder \
        .appName(report_name)\
        .config("spark.sql.session.timeZone", "UTC")\
        .enableHiveSupport() \
        .getOrCreate()

    config = ReportGenerator.get_config(                # Reading the configuration from the prop_file
        prop_file_path=prop_file_path
    )

    report_job = ReportGenerator\
        .get_reporting_job(                             # Getting the object for the required report generator
            spark_session=spark_session,
            report_name=report_name,
            config=config
        )

    report_job.run()                              # Running the report generator job
